use duckdb::{Connection, Row};
use miette::{IntoDiagnostic, Result};
use std::path::Path;

fn main() -> Result<()> {
    let db_path = "examples/basic_rag_pipeline/target/verity.duckdb";
    if !Path::new(db_path).exists() {
        return Err(miette::miette!(
            "❌ Database not found at: {}\n👉 Have you run 'verity run'?",
            db_path
        ));
    }

    let conn = Connection::open(db_path).into_diagnostic()?;

    let tables = vec!["business_intelligence"];

    for table in tables {
        println!("\n🔍 Inspecting Table: '{}'", table);

        let mut stmt_cols = conn
            .prepare(&format!("PRAGMA table_info({})", table))
            .into_diagnostic()?;

        let column_names: Vec<String> = stmt_cols
            .query_map([], |row: &Row| row.get(1))
            .into_diagnostic()?
            .map(|r| r.unwrap_or_else(|_| "UNKNOWN".to_string()))
            .collect();

        println!("   Columns: [{}]", column_names.join(", "));
        println!("   --- Rows (Limit 5) ---");

        let mut stmt = conn
            .prepare(&format!("SELECT * FROM {} LIMIT 5", table))
            .into_diagnostic()?;

        let mut rows = stmt.query([]).into_diagnostic()?;

        while let Some(row) = rows.next().into_diagnostic()? {
            let values: Vec<String> = (0..column_names.len())
                .map(|i| {
                    let val = row.get_ref(i).unwrap();
                    format!("{:?}", val)
                })
                .collect();

            println!("   ➜ {}", values.join(" | "));
        }
    }

    Ok(())
}
