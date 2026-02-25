// verity-core/src/application/pipeline.rs

use futures::StreamExt;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use std::sync::Arc;

// --- Domain Imports ---
use crate::domain::compliance::anomaly::{AnomalyError, ModelExecutionState, RowCountCheck};
use crate::domain::error::DomainError;
use crate::domain::governance::rewriter::PolicyRewriter;
use crate::domain::governance::{ColumnPolicy, GovernanceLinter, SecurityLevel};
use crate::domain::graph::GraphSolver;
use crate::domain::ports::{ManifestLoader, SchemaSource};
use crate::domain::project::LifecycleStatus;
use crate::domain::project::manifest::{ManifestNode, ResourceType};

// --- Application & Infra Imports ---
use crate::application::materialization::Materializer;
use crate::application::ports::TemplateEngine;
use crate::application::validation;
use crate::error::VerityError;
use crate::infrastructure::config::ProjectConfig;
use crate::infrastructure::fs::atomic_write;
use crate::ports::connector::Connector;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RunResult {
    pub success: bool,
    pub models_executed: usize,
    pub errors: Vec<String>,
}

/// Contexte injecté dans chaque tâche asynchrone pour éviter l'enfer des paramètres
struct PipelineContext<'a, T, S> {
    renderer: &'a T,
    schema_source: &'a S,
    connector: &'a dyn Connector,
    target_dir: &'a Path,
    project_dir: &'a Path,
    col_policies: &'a [ColumnPolicy],
    strict_mode: bool,
    default_anomaly_threshold: f64,
    prev_row_count: Option<u64>,
}

pub async fn run_pipeline<M, T, S>(
    manifest_loader: &M,
    template_engine: Arc<T>,
    schema_source: &S,
    project_dir: &Path,
    config: &ProjectConfig,
    connector: &dyn Connector,
    select: Option<String>,
) -> Result<RunResult, VerityError>
where
    M: ManifestLoader,
    T: TemplateEngine + 'static,
    S: SchemaSource,
{
    println!("🚀 Starting Pipeline Orchestrator...");
    let start_time = std::time::Instant::now();

    // 1. SETUP (Infra/IO)
    let target_dir = project_dir.join(&config.target_path);
    if !target_dir.exists() {
        fs::create_dir_all(&target_dir)?;
    }

    let state_path = target_dir.join("state.json");
    let mut state_store = load_state_store(&state_path).unwrap_or_default();

    // 2. DISCOVERY (Infra -> Domain)
    println!("📦 Compiling Project Manifest...");
    let manifest = manifest_loader
        .load(project_dir, config)
        .map_err(VerityError::Domain)?;

    save_json(&target_dir.join("manifest.json"), &manifest)?;

    // 3. SOURCE REGISTRATION
    println!("🔌 Registering Sources...");
    for source in manifest.sources.values() {
        let raw_path = Path::new(&source.path);
        let absolute_path = if raw_path.is_absolute() {
            raw_path.to_path_buf()
        } else {
            project_dir.join(raw_path)
        };

        if absolute_path.exists() {
            connector
                .register_source(&source.name, &absolute_path)
                .await?;
        } else {
            println!(
                "   ⚠️  Warning: Source file not found at {:?}",
                absolute_path
            );
        }
    }

    // 3.5. PLAN-LEVEL GOVERNANCE (Zero-Trust Pushdown)
    if connector.supports_plan_governance() {
        use crate::domain::governance::governance_rule::GovernancePolicySet;

        let all_policies: Vec<_> = manifest
            .nodes
            .values()
            .flat_map(|node| node.columns.iter())
            .filter_map(|col| col.policy.map(|p| (col.name.clone(), p.to_string())))
            .collect();

        let policy_set = GovernancePolicySet::from_pairs(all_policies);
        if !policy_set.column_policies.is_empty() {
            connector.register_governance(policy_set).await;
        }
    }

    // 4. DAG SCHEDULING (Topological Sort)
    println!("🧠 Calculating Execution DAG...");
    let execution_layers_refs = GraphSolver::plan_execution(&manifest)?;

    // Convert zero-copy refs to owned Strings for async scheduling
    let execution_layers: Vec<Vec<String>> = execution_layers_refs
        .into_iter()
        .map(|layer| layer.into_iter().map(|s| s.to_string()).collect())
        .collect();

    // Filter logic (--select)
    let layers_to_run: Vec<Vec<String>> = if let Some(ref sel) = select {
        if execution_layers.iter().flatten().any(|n| n == sel) {
            vec![vec![sel.clone()]]
        } else {
            vec![] // Model not found in DAG
        }
    } else {
        execution_layers
    };

    let total_models: usize = layers_to_run.iter().map(|l| l.len()).sum();
    println!(
        "📝 Execution Plan: {} models selected in {} layers",
        total_models,
        layers_to_run.len()
    );

    // 5. PARALLEL EXECUTION LOOP
    let col_policies = Arc::new(config.governance.pii_detection.column_policies.clone());
    let strict_mode = config.governance.strict || std::env::var("VERITY_STRICT").is_ok();
    let default_anomaly_threshold = config.governance.default_anomaly_threshold;

    println!(
        "    {} Strict Governance Mode: {}",
        if strict_mode { "🔒" } else { "🔓" },
        if strict_mode { "ON" } else { "OFF (Dev)" }
    );
    println!("🟢 Processing Pipeline...");

    let mut success_count = 0;
    let mut errors = Vec::new();

    for (i, layer) in layers_to_run.iter().enumerate() {
        if layer.is_empty() {
            continue;
        }
        println!("  🔹 Executing Layer {} ({} models)...", i + 1, layer.len());

        // Prepare data for the current layer (Disconnect from state_store borrow)
        let layer_nodes: Vec<_> = layer
            .iter()
            .filter_map(|node_name| {
                let node = manifest.nodes.get(node_name)?.clone();
                if node.resource_type != ResourceType::Model {
                    return None;
                }
                let prev_rows = state_store.get(node_name).map(|s| s.row_count);
                Some((node, prev_rows))
            })
            .collect();

        // Create a stream of futures for the current layer
        let futures = layer_nodes.into_iter().map(|(node, prev_row_count)| {
            let renderer = template_engine.clone();
            let policies = col_policies.clone();
            let target_dir = target_dir.clone();
            let project_dir = project_dir.to_path_buf();

            async move {
                let ctx = PipelineContext {
                    renderer: &*renderer,
                    schema_source,
                    connector,
                    target_dir: &target_dir,
                    project_dir: &project_dir,
                    col_policies: &policies,
                    strict_mode,
                    default_anomaly_threshold,
                    prev_row_count,
                };
                let res = execute_node(&node, ctx).await;
                (node.name.clone(), res)
            }
        });

        // Execute layer nodes concurrently with a max concurrency of 8
        let mut stream = futures::stream::iter(futures).buffer_unordered(8);

        while let Some((node_name, res)) = stream.next().await {
            match res {
                Ok(current_rows) => {
                    println!("    ✅ Built model: {}", node_name);
                    state_store.insert(
                        node_name,
                        ModelExecutionState {
                            last_run_at: chrono::Utc::now().to_rfc3339(),
                            row_count: current_rows,
                        },
                    );
                    success_count += 1;
                }
                Err(e) => {
                    eprintln!("    ❌ Error building {}: {}", node_name, e);
                    errors.push(format!("{}: {}", node_name, e));
                    return Err(e); // Fail-fast on layer error
                }
            }
        }
    }

    // 6. FINALIZE & CHECKPOINT
    save_json(&state_path, &state_store)?;
    let _ = connector.execute("CHECKPOINT").await;

    let result = RunResult {
        success: errors.is_empty(),
        models_executed: success_count,
        errors,
    };

    save_json(&target_dir.join("run_results.json"), &result)?;

    println!(
        "✨ Done in {:.2}s. Executed {} models.",
        start_time.elapsed().as_secs_f64(),
        success_count
    );
    Ok(result)
}

// --- HELPER FUNCTIONS ---

async fn execute_node<T, S>(
    node: &ManifestNode,
    ctx: PipelineContext<'_, T, S>,
) -> Result<u64, VerityError>
where
    T: TemplateEngine,
    S: SchemaSource,
{
    let layer_name = match node.name.as_str() {
        name if name.starts_with("stg_") => "staging",
        name if name.starts_with("int_") => "intermediate",
        _ => "marts",
    };

    // --- A. TEMPLATING & QUOTING ---
    let context = serde_json::json!({ "model_name": node.name });
    let compiled_sql = ctx.renderer.render(&node.raw_sql, &context)?;
    let compiled_sql =
        crate::domain::compiler::quoter::UniversalQuoter::quote_identifiers(&compiled_sql)
            .map_err(|e| VerityError::InternalError(format!("SQL Quoting failed: {}", e)))?;

    save_artifact(
        ctx.target_dir,
        "compiled",
        layer_name,
        &node.name,
        &compiled_sql,
    )?;

    // --- B. GOVERNANCE AST REWRITE ---
    let secured_sql = if ctx.connector.supports_plan_governance() {
        compiled_sql.clone()
    } else {
        PolicyRewriter::apply_masking(&compiled_sql, node)?
    };

    save_artifact(ctx.target_dir, "run", layer_name, &node.name, &secured_sql)?;

    // --- C. PRE-FLIGHT LINTING (Zero-Trust) ---
    if ctx.strict_mode && node.security_level != SecurityLevel::Public {
        let sample_query = format!("SELECT * FROM ({}) LIMIT 500", compiled_sql);
        if let Ok(sample_batches) = ctx.connector.fetch_sample(&sample_query).await {
            let linter =
                GovernanceLinter::new().map_err(|e| VerityError::InternalError(e.to_string()))?;
            for batch in sample_batches {
                linter
                    .verify_model_compliance(node, &batch)
                    .map_err(|e| VerityError::InternalError(e.to_string()))?;
            }
        }
    }

    // --- D. MATERIALIZATION ---
    let strategy_used =
        Materializer::materialize(ctx.connector, &node.name, &secured_sql, &node.config).await?;
    println!("     Strategy used: {}", strategy_used);

    // --- E. VALIDATION (Schema Drift) ---
    let undocumented_columns = validation::run_tests(node, ctx.connector).await?;
    if !undocumented_columns.is_empty() {
        let cols: Vec<String> = undocumented_columns.into_iter().collect();
        match &node.schema_path {
            Some(path) => {
                ctx.schema_source
                    .update_model_columns(path, &node.name, &cols)
                    .map_err(VerityError::Domain)?;
            }
            None => {
                let full_sql_path = ctx.project_dir.join(&node.path);
                ctx.schema_source
                    .create_versioned_model(
                        &full_sql_path,
                        &node.name,
                        1,
                        &cols,
                        ctx.col_policies,
                        Some(LifecycleStatus::Provisioning),
                    )
                    .map_err(VerityError::Domain)?;
                println!(
                    "    ✨ [Auto-Gen] Initialized schema for {} (v1)",
                    node.name
                );
            }
        }
    }

    // --- F. COMPLIANCE (Anomaly Detection) ---
    let current_rows = count_rows(ctx.connector, &node.name).await?;
    check_compliance(
        node,
        current_rows,
        ctx.prev_row_count,
        ctx.strict_mode,
        ctx.default_anomaly_threshold,
    )?;

    Ok(current_rows)
}

fn save_artifact(
    base: &Path,
    phase: &str,
    layer: &str,
    model_name: &str,
    content: &str,
) -> Result<(), VerityError> {
    let dir = base.join(phase).join(layer);
    if !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    atomic_write(dir.join(format!("{}.sql", model_name)), content)?;
    Ok(())
}

fn check_compliance(
    node: &ManifestNode,
    current_rows: u64,
    prev_rows: Option<u64>,
    strict_mode: bool,
    default_anomaly_threshold: f64,
) -> Result<(), VerityError> {
    let compliance = match &node.compliance {
        Some(c) => c,
        None => return Ok(()),
    };

    if let Some(checks) = &compliance.post_flight {
        for check in checks {
            if check.check == "row_count_anomaly" {
                let threshold_val = check
                    .params
                    .get("threshold")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(default_anomaly_threshold);

                if let Err(AnomalyError::DeviationExceeded {
                    deviation,
                    threshold,
                    prev,
                    curr,
                }) = RowCountCheck::validate(current_rows, prev_rows, threshold_val)
                {
                    let msg = format!(
                        "Anomaly detected on {}: Rows changed by {:.2}% (Threshold {:.2}%). Prev: {}, Curr: {}",
                        node.name, deviation, threshold, prev, curr
                    );

                    if check.severity == "error" {
                        if strict_mode {
                            eprintln!("❌  [Strict] {}", msg);
                            return Err(VerityError::Domain(DomainError::ComplianceError(msg)));
                        }
                        eprintln!("⚠️  [Bypass] {} (Strict Mode: OFF)", msg);
                    } else {
                        eprintln!("⚠️  {}", msg);
                    }
                }
            }
        }
    }
    Ok(())
}

async fn count_rows(connector: &dyn Connector, table_name: &str) -> Result<u64, VerityError> {
    connector
        .query_scalar(&format!("SELECT count(*) FROM \"{}\"", table_name))
        .await
}

fn save_json<T: serde::Serialize>(path: &Path, data: &T) -> Result<(), VerityError> {
    let content = serde_json::to_string_pretty(data)
        .map_err(|e| VerityError::InternalError(format!("Serialization: {}", e)))?;
    atomic_write(path, content)?;
    Ok(())
}

fn load_state_store(path: &Path) -> Result<HashMap<String, ModelExecutionState>, VerityError> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let content = fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content).unwrap_or_default())
}
