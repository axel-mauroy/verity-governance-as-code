// verity-core/src/application/pipeline.rs

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::error::VerityError;
use crate::ports::connector::Connector;

// Application Services
use crate::application::materialization::Materializer;
use crate::application::validation;

// Domain
use crate::domain::compliance::anomaly::{AnomalyError, ModelExecutionState, RowCountCheck};
use crate::domain::error::DomainError;
use crate::domain::governance::rewriter::PolicyRewriter;
use crate::domain::graph::GraphSolver;
use crate::domain::project::manifest::{ManifestNode, ResourceType};

// Infrastructure
// Infrastructure Types
// use crate::infrastructure::compiler::discovery::GraphDiscovery; // Removed
// use crate::infrastructure::compiler::jinja::JinjaRenderer; // Removed
// use crate::infrastructure::config::schema::{...}; // Removed

// Ports
use crate::application::ports::TemplateEngine;
use crate::domain::ports::{ManifestLoader, SchemaSource};
use crate::domain::project::LifecycleStatus;
use crate::infrastructure::config::ProjectConfig;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct RunResult {
    pub success: bool,
    pub models_executed: usize,
    pub errors: Vec<String>,
}

use futures::StreamExt; // Extension trait for streams
use std::sync::Arc;

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
    T: TemplateEngine + 'static, // 'static required for Arc sharing across threads? No, Arc<T> implies T: Send+Sync which is in trait bound.
    S: SchemaSource,
{
    println!("🚀 Starting Pipeline Orchestrator...");
    let start_time = std::time::Instant::now();

    // 1. SETUP (Infra/IO)
    let target_dir = project_dir.join(&config.target_path);
    if !target_dir.exists() {
        fs::create_dir_all(&target_dir)?;
    }

    // Load State (for Anomaly Detection)
    let state_path = target_dir.join("state.json");
    let initial_state = load_state_store(&state_path).unwrap_or_default();
    // Wrap state_store in Arc<Mutex> for parallel access if needed,
    // BUT since we are writing results at the end, we can collect results and update state sequentially after each layer.
    // However, to keep it simple and avoid Mutex contention during execution, we'll collect results and update a local map.
    let mut state_store = initial_state;

    // 2. DISCOVERY (Infra -> Domain)
    println!("📦 Compiling Project Manifest...");
    let manifest = manifest_loader
        .load(project_dir, config)
        .map_err(VerityError::Domain)?;

    // Debug: Save manifest
    save_json(&target_dir.join("manifest.json"), &manifest)?;

    // 3. SOURCE REGISTRATION (Abstracted via Connector)
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
                .register_source(&source.name, &absolute_path.to_string_lossy())
                .await?;
        } else {
            println!(
                "   ⚠️  Warning: Source file not found at {:?}",
                absolute_path
            );
        }
    }

    // 4. DAG SCHEDULING (Domain Pure Logic -> Layers)
    println!("🧠 Calculating Execution DAG...");
    let execution_layers = GraphSolver::plan_execution(&manifest)?;

    // Filter (--select)
    // If select is present, we filter nodes BUT we must respect dependencies.
    // For now, simpler implementation: Flatten, filter, then re-layering is hard.
    // Alternative: Just execute the selected node and its upstreams?
    // Current behavior: Filter the list. If we filter the layers, we might lose dependencies if not careful.
    // Let's keep it simple: If select is ON, we flatten and run sequentially (or just run that single node).
    // The previous implementation filtered the list.
    // Efficient Selection Logic:
    // If `select` is active, we validly assume the user wants to run ONLY that model (and maybe parents).
    // The previous code just filtered the name.

    let layers_to_run: Vec<Vec<String>> = if let Some(sel) = select {
        // Flatten layers, find the node, return as single layer
        let all_nodes: Vec<String> = execution_layers.into_iter().flatten().collect();
        if all_nodes.contains(&sel) {
            vec![vec![sel]]
        } else {
            vec![]
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

    // 5. EXECUTION LOOP (Parallelized Layers)
    // let renderer = Arc::new(JinjaRenderer::new()); // Removed: passed as argument
    let col_policies = Arc::new(config.governance.pii_detection.column_policies.clone());
    let mut success_count = 0;
    let mut errors = Vec::new();

    println!("🟢 Processing Pipeline...");

    let env_strict = std::env::var("VERITY_STRICT").is_ok();
    let strict_mode = config.governance.strict || env_strict;
    if strict_mode {
        println!("    🔒 Strict Governance Mode: ON");
    } else {
        println!("    🔓 Strict Governance Mode: OFF (Dev)");
    }

    for (i, layer) in layers_to_run.iter().enumerate() {
        if layer.is_empty() {
            continue;
        }
        println!("  🔹 Executing Layer {} ({} models)...", i + 1, layer.len());

        let futures = layer.iter().map(|node_name| {
            let node = manifest.nodes.get(node_name).cloned(); // Clone cheap (ManifestNode is Clone)
            let renderer = template_engine.clone(); // Clone Arc
            let policies = col_policies.clone();
            let target_dir = target_dir.clone();
            let project_dir = project_dir.to_path_buf();
            let prev_row_count = state_store.get(node_name).map(|s| s.row_count);
            // S is not Clone necessarily (depends). But schema_source is &S and valid for 'async block lifetime?
            // Async implementation requires Owned or 'static if spawned?
            // The futures are collected and executed in this function scope.
            // But Map iterator closure must produce a Future.
            // If schema_source is &S, we can capture it if the future is not 'static.
            // buffer_unordered requires streams of futures.

            async move {
                if let Some(node) = node
                    && node.resource_type == ResourceType::Model
                {
                    let ctx = PipelineContext {
                        renderer: &*renderer,
                        schema_source, // Captured from environment
                        connector,
                        target_dir: &target_dir,
                        project_dir: &project_dir,
                        col_policies: &policies,
                        strict_mode,
                        prev_row_count,
                    };

                    let res = execute_node(&node, ctx).await;
                    return Some((node.name.clone(), res));
                }
                None
            }
        });

        // Parallel Execution with bounded concurrency (e.g. 8 or 4)
        // Note: buffered_unordered executes them in parallel but returns items as they finish.
        // We ensure all items in *this layer* are finished before moving to next layer.
        let stream = futures::stream::iter(futures).buffer_unordered(8);
        let results: Vec<_> = stream.collect().await;

        for (node_name, res) in results.into_iter().flatten() {
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
                    // Fail Fast? Or continue layer?
                    // If one fails in a layer, dependent layers might fail.
                    // For strict fail-fast:
                    return Err(e);
                }
            }
        }
    }

    // Save updated state
    save_json(&state_path, &state_store)?;

    // 6. FINALIZE
    let _ = connector.execute("CHECKPOINT").await;

    let duration = start_time.elapsed();
    println!(
        "✨ Done in {:.2}s. Executed {} models.",
        duration.as_secs_f64(),
        success_count
    );

    let result = RunResult {
        success: errors.is_empty(),
        models_executed: success_count,
        errors,
    };

    save_json(&target_dir.join("run_results.json"), &result)?;

    Ok(result)
}

// --- HELPER FUNCTIONS ---

// Context struct to reduce argument count for execute_node
// Context struct to reduce argument count for execute_node
struct PipelineContext<'a, T, S> {
    renderer: &'a T,
    schema_source: &'a S,
    connector: &'a dyn Connector,
    target_dir: &'a Path,
    project_dir: &'a Path,
    col_policies: &'a [crate::domain::governance::ColumnPolicy],
    strict_mode: bool,
    prev_row_count: Option<u64>,
}

/// Execute a single node: Render -> Governance -> Materialize -> Validate -> Compliance
/// Returns the number of rows produced.
/// Execute a single node: Render -> Governance -> Materialize -> Validate -> Compliance
/// Returns the number of rows produced.
async fn execute_node<T, S>(
    node: &ManifestNode,
    ctx: PipelineContext<'_, T, S>,
) -> Result<u64, VerityError>
where
    T: TemplateEngine,
    S: SchemaSource,
{
    // A. Compilation Jinja
    // Using simple string as context for now, wrapped in Value
    let context = serde_json::json!({ "model_name": node.name });
    let compiled_sql = ctx.renderer.render(&node.raw_sql, &context)?;

    let layer = if node.name.starts_with("stg_") {
        "staging"
    } else if node.name.starts_with("int_") {
        "intermediate"
    } else {
        "marts"
    };

    // LOG: Compiled
    let compiled_path = ctx.target_dir.join("compiled").join(layer);
    if !compiled_path.exists() {
        fs::create_dir_all(&compiled_path)?;
    }
    crate::infrastructure::fs::atomic_write(
        compiled_path.join(format!("{}.sql", node.name)),
        &compiled_sql,
    )?;

    // B. Application de la Gouvernance (Masking)
    // strict_mode is passed as argument, we don't need to recalculate it.

    let secured_sql = PolicyRewriter::apply_masking(&compiled_sql, node)?;

    // LOG: Run
    let run_path = ctx.target_dir.join("run").join(layer);
    if !run_path.exists() {
        fs::create_dir_all(&run_path)?;
    }
    crate::infrastructure::fs::atomic_write(
        run_path.join(format!("{}.sql", node.name)),
        &secured_sql,
    )?;

    // C. Matérialisation
    let strategy_used =
        Materializer::materialize(ctx.connector, &node.name, &secured_sql, &node.config).await?;

    println!("     Strategy used: {}", strategy_used);

    // D. Validation (Schema Drift & Data Tests)
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

    // E. Compliance & Anomaly Checks (Post-Flight)
    let current_rows = count_rows(ctx.connector, &node.name).await?;

    check_compliance(node, current_rows, ctx.prev_row_count, ctx.strict_mode)?;

    Ok(current_rows)
}

/// Run post-flight compliance checks defined in YAML (Anomaly, etc.)
fn check_compliance(
    node: &ManifestNode,
    current_rows: u64,
    prev_rows: Option<u64>,
    strict_mode: bool,
) -> Result<(), VerityError> {
    // 1. Check if compliance config exists
    let compliance = match &node.compliance {
        Some(c) => c,
        None => return Ok(()), // No compliance config, skip
    };

    // 2. Iterate over Post-Flight checks
    if let Some(checks) = &compliance.post_flight {
        for check in checks {
            // Dispatch based on check name
            if check.check == "row_count_anomaly" {
                // Extract parameters
                let threshold_val = check
                    .params
                    .get("threshold")
                    .and_then(|v| v.as_f64())
                    .unwrap_or(0.1); // Default 10%

                match RowCountCheck::validate(current_rows, prev_rows, threshold_val) {
                    Ok(_) => {}
                    Err(AnomalyError::DeviationExceeded {
                        deviation,
                        threshold,
                        prev,
                        curr,
                    }) => {
                        let msg = format!(
                            "Anomaly detected on {}: Rows changed by {:.2}% (Threshold {:.2}%). Prev: {}, Curr: {}",
                            node.name, deviation, threshold, prev, curr
                        );

                        if check.severity == "error" {
                            if strict_mode {
                                eprintln!("❌  [Strict] {}", msg);
                                return Err(VerityError::Domain(DomainError::ComplianceError(msg)));
                            } else {
                                eprintln!("⚠️  [Bypass] {} (Strict Mode: OFF)", msg);
                            }
                        } else {
                            eprintln!("⚠️  {}", msg);
                        }
                    }
                    Err(AnomalyError::NoHistory) => {
                        // println!("    ℹ️  First run (no history), skipping anomaly check.");
                    }
                }
            } else {
                println!("    ℹ️  Unknown check '{}', skipping.", check.check);
            }
        }
    }

    Ok(())
}

async fn count_rows(_connector: &dyn Connector, table_name: &str) -> Result<u64, VerityError> {
    // ⚠️ DuckDB specific syntax
    let _query = format!("SELECT count(*) FROM \"{}\"", table_name);
    // On suppose que connector.query_scalar renvoie un u64, ou on parse le result.
    // Ici simplifcation via execute qui ne retourne pas de rows dans l'interface actuelle.
    // IL FAUDRA AJOUTER `query_scalar` au Trait Connector.

    // Mock pour compilation si le trait n'est pas à jour :
    // let count = connector.query_scalar(&query).await?;
    Ok(100) // Placeholder: Remplacer par vrai appel DB
}

fn save_json<T: serde::Serialize>(path: &Path, data: &T) -> Result<(), VerityError> {
    let content = serde_json::to_string_pretty(data)
        .map_err(|e| VerityError::InternalError(format!("Serialization: {}", e)))?;
    crate::infrastructure::fs::atomic_write(path, content)?;
    Ok(())
}

fn load_state_store(path: &Path) -> Result<HashMap<String, ModelExecutionState>, VerityError> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let content = fs::read_to_string(path)?;
    let store = serde_json::from_str(&content).unwrap_or_default();
    Ok(store)
}
