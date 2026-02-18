// verity/src/commands/inspect.rs
//
// USE CASE: Inspect a DuckDB table (schema + sample rows).
// Replaces the old hardcoded bin/inspect.rs binary.

use duckdb::{Connection, Row};
use std::path::Path;

pub fn execute(db_path: String, table: String, limit: usize) -> anyhow::Result<()> {
    if !Path::new(&db_path).exists() {
        anyhow::bail!(
            "❌ Database not found at: {}\n👉 Have you run 'verity run'?",
            db_path
        );
    }

    let conn = Connection::open(&db_path)?;

    println!("\n🔍 Inspecting Table: '{}'", table);

    // Fetch column names
    let mut stmt_cols = conn.prepare(&format!("PRAGMA table_info({})", table))?;

    let column_names: Vec<String> = stmt_cols
        .query_map([], |row: &Row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;

    println!("   Columns: [{}]", column_names.join(", "));
    println!("   --- Rows (Limit {}) ---", limit);

    // Fetch sample rows
    let mut stmt = conn.prepare(&format!("SELECT * FROM {} LIMIT {}", table, limit))?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let values: Vec<String> = (0..column_names.len())
            .map(|i| match row.get_ref(i) {
                Ok(val) => format!("{:?}", val),
                Err(_) => "ERROR".to_string(),
            })
            .collect();

        println!("   ➜ {}", values.join(" | "));
    }

    Ok(())
}
