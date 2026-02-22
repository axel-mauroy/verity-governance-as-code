// verity/src/commands/query.rs
//
// USE CASE: Execute a raw SQL query (ad-hoc).

pub async fn execute(query: String) -> anyhow::Result<()> {
    // TODO: Ad-hoc query execution currently disabled as DuckDB was removed.
    // DataFusion requires a target directory or context to run queries.
    println!(
        "Ad-hoc query execution currently disabled. Query: {}",
        query
    );
    Ok(())
}
