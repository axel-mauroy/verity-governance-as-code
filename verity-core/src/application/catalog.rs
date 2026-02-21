use chrono::Utc;
use serde::Serialize;
use std::fs;
use std::path::Path;

use crate::domain::project::manifest::{Manifest, ResourceType};
use crate::error::VerityError;
use anyhow::Context;

// --- DTOs (Data Transfer Objects) ---
// Those structures define exactly what the UI will display.

#[derive(Serialize)]
pub struct CatalogArtifact {
    pub generated_at: String,
    pub project_name: String,
    pub assets: Vec<CatalogAsset>,
    pub stats: CatalogStats,
}

#[derive(Serialize)]
pub struct CatalogStats {
    pub total_models: usize,
    pub governed_models: usize, // models with data_contract = true
    pub pii_columns_protected: usize,
}

#[derive(Serialize)]
pub struct CatalogAsset {
    pub id: String, // unique id
    pub name: String,
    pub resource_type: String,
    pub description: String,

    // Governance
    pub owners: OwnersDisplay,
    pub security_level: String,
    pub version: u32,
    pub status: String, // Active, Deprecated...
    pub is_contract: bool,

    // Technical
    pub materialized: String,
    pub layer: String,
    pub row_count: Option<u64>,         // Comes from the state
    pub last_execution: Option<String>, // Success/Fail

    pub columns: Vec<CatalogColumn>,
}

#[derive(Serialize)]
pub struct OwnersDisplay {
    pub tech: String,
    pub business: String,
}

#[derive(Serialize)]
pub struct CatalogColumn {
    pub name: String,
    pub type_info: String,
    pub description: String,
    pub policy: Option<String>, // Hash, Redact... (The most important)
    pub is_pii: bool,
}

// --- GENERATOR SERVICE ---

pub struct CatalogGenerator;

impl CatalogGenerator {
    pub fn generate(
        _project_dir: &Path,
        target_dir: &Path,
        manifest: &Manifest,
    ) -> Result<String, VerityError> {
        println!("📚 Generating Data Catalog...");

        // 1. Load the run results (if available) to enrich the catalog
        // let _run_results: Option<RunResult> = load_json_file(&target_dir.join("run_results.json"));

        // Load State for Row Counts
        let state_path = target_dir.join("state.json");
        let state_store = load_state_store(&state_path).unwrap_or_default();

        // 2. Transform the Manifest into Assets Catalog
        let mut assets = Vec::new();
        let mut pii_count = 0;
        let mut governed_count = 0;

        for (node_name, node) in &manifest.nodes {
            if node.resource_type == ResourceType::Model {
                // Get State
                let (row_count, last_exec) = if let Some(state) = state_store.get(node_name) {
                    (Some(state.row_count), Some(state.last_run_at.clone()))
                } else {
                    (None, None)
                };

                // Simple layer detection
                let layer = if node_name.starts_with("stg") {
                    "Staging"
                } else if node_name.starts_with("int") {
                    "Intermediate"
                } else {
                    "Marts"
                };

                // Column mapping
                let columns: Vec<CatalogColumn> = node
                    .columns
                    .iter()
                    .map(|c| {
                        if c.policy.is_some() {
                            pii_count += 1;
                        }
                        CatalogColumn {
                            name: c.name.clone(),
                            type_info: "UNKNOWN".to_string(), // Placeholder: Type inference requires DuckDB connection
                            description: "Description auto-generated".to_string(), // Schema description mapping to be added
                            policy: c.policy.map(|p| p.to_string()),
                            is_pii: c.policy.is_some(),
                        }
                    })
                    .collect();

                if node.config.protected {
                    governed_count += 1;
                } // Or check data_contract

                assets.push(CatalogAsset {
                    id: node_name.clone(),
                    name: node_name.clone(),
                    resource_type: "Model".to_string(),
                    description: "Documentation indisponible".to_string(), // Placeholder: Model description from schema

                    owners: OwnersDisplay {
                        tech: node
                            .config
                            .tech_owner
                            .clone()
                            .unwrap_or("Unassigned".into()),
                        business: node
                            .config
                            .business_owner
                            .clone()
                            .unwrap_or("Unassigned".into()),
                    },
                    security_level: if node.config.protected {
                        "Confidential".into()
                    } else {
                        "Public".into()
                    },

                    // Those fields need NodeConfig to be enriched with the new props from Schema.rs
                    // For now, we use placeholders or map if available
                    version: 1,
                    status: "Active".to_string(),
                    is_contract: false, // mapper node.config.data_contract

                    materialized: node
                        .config
                        .materialized
                        .as_ref()
                        .map(|m| format!("{:?}", m))
                        .unwrap_or("View".into()),
                    layer: layer.to_string(),
                    row_count,
                    last_execution: last_exec,
                    columns,
                });
            }
        }

        // 3. Create the global artifact
        let artifact = CatalogArtifact {
            generated_at: Utc::now().to_rfc3339(),
            project_name: manifest.project_name.clone(),
            assets,
            stats: CatalogStats {
                total_models: manifest.nodes.len(),
                governed_models: governed_count,
                pii_columns_protected: pii_count,
            },
        };

        // 4. Write the JSON (Catalog API)
        let json_path = target_dir.join("catalog.json");
        let json_content = serde_json::to_string_pretty(&artifact)
            .context("Failed to serialize catalog to JSON")
            .map_err(|e| VerityError::InternalError(e.to_string()))?;
        crate::infrastructure::fs::atomic_write(&json_path, &json_content)
            .with_context(|| format!("Failed to write catalog.json to {:?}", json_path))
            .map_err(|e| VerityError::InternalError(e.to_string()))?;

        // 5. Generate the HTML (Single File App)
        let html_path = target_dir.join("index.html");
        let html_content = render_html_template(&json_content);
        crate::infrastructure::fs::atomic_write(&html_path, html_content)
            .with_context(|| format!("Failed to write index.html to {:?}", html_path))
            .map_err(|e| VerityError::InternalError(e.to_string()))?;

        println!("✨ Catalog generated at: {}", html_path.display());
        Ok(html_path.to_string_lossy().to_string())
    }
}

// --- HELPER LOADERS ---
fn load_json_file<T: serde::de::DeserializeOwned>(path: &Path) -> Option<T> {
    if !path.exists() {
        return None;
    }
    let content = fs::read_to_string(path).ok()?;
    serde_json::from_str(&content).ok()
}

// Helper for State Store
use crate::domain::compliance::anomaly::ModelExecutionState;
use std::collections::HashMap;

fn load_state_store(path: &Path) -> Option<HashMap<String, ModelExecutionState>> {
    load_json_file(path)
}

// --- EMBEDDED HTML TEMPLATE (No React dependency) ---
// Usage of Tailwind via CDN + Vanilla JS to parse the embedded JSON
fn render_html_template(json_data: &str) -> String {
    format!(
        r#"
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Verity Data Catalog</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        .pii-badge {{ @apply bg-red-100 text-red-800 text-xs font-semibold mr-2 px-2.5 py-0.5 rounded border border-red-400; }}
        .safe-badge {{ @apply bg-green-100 text-green-800 text-xs font-semibold mr-2 px-2.5 py-0.5 rounded border border-green-400; }}
    </style>
</head>
<body class="bg-gray-50 text-gray-900 font-sans">

    <nav class="bg-slate-900 text-white p-4 shadow-lg sticky top-0 z-50">
        <div class="container mx-auto flex justify-between items-center">
            <div class="flex items-center space-x-2">
                <span class="text-2xl">🛡️</span>
                <h1 class="text-xl font-bold tracking-tight">Verity Catalog</h1>
            </div>
            <div class="text-sm text-gray-400" id="project-meta">Loading...</div>
        </div>
    </nav>

    <div class="container mx-auto p-6">
        
        <div class="grid grid-cols-1 md:grid-cols-4 gap-6 mb-8">
            <div class="md:col-span-3">
                <input type="text" id="search" placeholder="Search models, columns, owners..." 
                       class="w-full p-4 rounded-lg shadow-sm border border-gray-200 focus:ring-2 focus:ring-blue-500 outline-none text-lg">
            </div>
            <div class="bg-white p-4 rounded-lg shadow-sm border border-gray-200">
                <div class="text-xs text-gray-500 uppercase font-bold">Protected PII Columns</div>
                <div class="text-3xl font-bold text-red-600" id="stat-pii">0</div>
            </div>
        </div>

        <div id="assets-grid" class="grid grid-cols-1 gap-6">
            </div>

    </div>

    <script>
        const catalogData = {json_data}; // Injection directe par Rust

        // DOM Elements
        const grid = document.getElementById('assets-grid');
        const searchInput = document.getElementById('search');

        // Init
        document.getElementById('project-meta').innerText = `${{catalogData.project_name}} • Generated ${{new Date(catalogData.generated_at).toLocaleString()}}`;
        document.getElementById('stat-pii').innerText = catalogData.stats.pii_columns_protected;

        function render(filterText = '') {{
            grid.innerHTML = '';
            const lowerFilter = filterText.toLowerCase();

            catalogData.assets.forEach(asset => {{
                // Search Logic
                if (filterText && !asset.name.includes(lowerFilter) && !asset.owners.tech.includes(lowerFilter)) return;

                const card = document.createElement('div');
                card.className = 'bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden hover:shadow-md transition-shadow';
                
                // Header Status Color
                let statusColor = asset.status === 'Active' ? 'bg-green-500' : 'bg-yellow-500';
                if(asset.security_level === 'Confidential') statusColor = 'bg-red-500';

                // Columns HTML
                const colsHtml = asset.columns.map(c => `
                    <div class="flex justify-between items-center py-2 border-b border-gray-50 last:border-0 text-sm">
                        <div class="flex items-center">
                            <span class="font-mono text-slate-700 mr-2">${{c.name}}</span>
                            ${{c.is_pii ? `<span class="pii-badge">🔒 ${{c.policy}}</span>` : ''}}
                        </div>
                        <span class="text-gray-400 text-xs">${{c.type_info}}</span>
                    </div>
                `).join('');

                card.innerHTML = `
                    <div class="p-5">
                        <div class="flex justify-between items-start mb-4">
                            <div>
                                <div class="flex items-center space-x-2">
                                    <h2 class="text-xl font-bold text-slate-800">${{asset.name}}</h2>
                                    <span class="px-2 py-0.5 rounded text-xs font-bold bg-slate-100 text-slate-600">v${{asset.version}}</span>
                                </div>
                                <p class="text-sm text-gray-500 mt-1">${{asset.description}}</p>
                            </div>
                            <div class="flex flex-col items-end space-y-1">
                                <span class="px-2 py-1 rounded-full text-xs font-bold text-white ${{statusColor}}">${{asset.status}}</span>
                                <span class="text-xs text-gray-400 uppercase tracking-wide border px-1 rounded">${{asset.layer}}</span>
                            </div>
                        </div>

                        <div class="grid grid-cols-2 gap-4 mb-4 bg-slate-50 p-3 rounded-lg text-sm">
                            <div>
                                <span class="block text-xs text-gray-400 uppercase">Tech Owner</span>
                                <span class="font-medium text-slate-700">${{asset.owners.tech}}</span>
                            </div>
                            <div>
                                <span class="block text-xs text-gray-400 uppercase">Security</span>
                                <span class="font-medium ${{asset.security_level === 'Confidential' ? 'text-red-600' : 'text-slate-700'}}">
                                    ${{asset.security_level}}
                                </span>
                            </div>
                        </div>

                        <div class="mt-4">
                            <h3 class="text-xs font-bold text-gray-400 uppercase mb-2">Schema & Policies</h3>
                            <div class="max-h-48 overflow-y-auto pr-2 custom-scrollbar">
                                ${{colsHtml}}
                            </div>
                        </div>
                    </div>
                    
                    ${{asset.is_contract ? '<div class="bg-indigo-50 p-2 text-center text-xs text-indigo-700 font-bold border-t border-indigo-100">📜 Certified Data Contract</div>' : ''}}
                `;

                grid.appendChild(card);
            }});
        }}

        // Initial Render
        render();

        // Search Listener
        searchInput.addEventListener('input', (e) => render(e.target.value));

    </script>
</body>
</html>
    "#,
        json_data = json_data
    )
}
