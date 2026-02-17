// verity/src/commands/docs.rs
//
// USE CASE: Generate the Data Catalog (HTML/JSON).

use std::path::PathBuf;

use verity_core::application::CatalogGenerator;
use verity_core::infrastructure::compiler::discovery::GraphDiscovery;
use verity_core::infrastructure::config::project::load_project_config;

pub fn execute(project_dir: PathBuf) -> anyhow::Result<()> {
    println!("📚 Generating Data Catalog...");

    let config = load_project_config(&project_dir)?;
    let target_dir = project_dir.join(&config.target_path);

    // Discovery
    let manifest = GraphDiscovery::discover(&project_dir, &config)?;

    // Catalog generation
    CatalogGenerator::generate(&project_dir, &target_dir, &manifest)?;

    println!(
        "✨ Documentation generated successfully in {}",
        target_dir.display()
    );
    Ok(())
}
