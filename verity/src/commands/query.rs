// verity/src/commands/query.rs
//
// USE CASE: Execute a raw SQL query (ad-hoc).

use verity_core::application::execute_query;
use verity_core::infrastructure::adapters::duckdb::DuckDBConnector;

pub async fn execute(query: String, db_path: String) -> anyhow::Result<()> {
    let connector = DuckDBConnector::new(&db_path)?;
    if let Err(e) = execute_query(&connector, &query).await {
        eprintln!("❌ Query failed: {}", e);
        std::process::exit(1);
    }
    Ok(())
}
