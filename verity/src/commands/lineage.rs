// verity/src/commands/lineage.rs
//
// USE CASE: Static Data Lineage Analysis — pre-flight compliance check.

use std::path::PathBuf;

use verity_core::application::LineageAnalyzer;
use verity_core::infrastructure::compiler::discovery::GraphDiscovery;
use verity_core::infrastructure::config::project::load_project_config;

pub fn execute(project_dir: PathBuf, check: bool, format: String) -> anyhow::Result<()> {
    println!("🔍 Analyzing Data Lineage...");

    // 1. Load Config & Discover Manifest
    let config = load_project_config(&project_dir)?;
    let target_dir = project_dir.join(&config.target_path);
    let manifest = GraphDiscovery::discover(&project_dir, &config)?;

    println!(
        "   Project: {} ({} models)",
        config.name,
        manifest.nodes.len()
    );

    // 2. Run Lineage Analysis
    let report = LineageAnalyzer::analyze(&manifest);

    // 3. Output Report
    match format.as_str() {
        "json" => {
            let json = report.to_json()?;
            // Save to target/lineage.json
            if !target_dir.exists() {
                std::fs::create_dir_all(&target_dir)?;
            }
            let out_path = target_dir.join("lineage.json");
            std::fs::write(&out_path, &json)?;
            println!("📄 JSON report saved to {}", out_path.display());
            println!("{}", json);
        }
        _ => {
            // Default: Mermaid
            let mermaid = report.to_mermaid();
            println!("\n```mermaid");
            println!("{}", mermaid);
            println!("```\n");
        }
    }

    // 4. Summary
    println!("📊 Lineage Summary:");
    println!("   Nodes: {}", report.nodes.len());
    println!("   Edges: {}", report.edges.len());

    if report.has_violations() {
        eprintln!("\n⚠️  {} violation(s) detected:", report.violations.len());
        for v in &report.violations {
            eprintln!("   ❌ {}", v.message);
        }

        if check {
            eprintln!("\n💥 --check mode: Failing due to compliance violations.");
            std::process::exit(1);
        }
    } else {
        println!("   ✅ No compliance violations detected.");
    }

    Ok(())
}
