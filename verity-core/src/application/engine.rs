// verity-core/src/application/engine.rs

use std::time::Instant;
use tracing::{debug, error, info, instrument};
use crate::error::VerityError;
use crate::ports::connector::Connector;


#[instrument(skip(connector, query), fields(query.preview = %preview_sql(query)))]
pub async fn execute_query(connector: &dyn Connector, query: &str) -> Result<(), VerityError> {
    let trimmed_query = query.trim();
    
    if trimmed_query.is_empty() {
        return Err(VerityError::InternalError("Attempted to execute an empty SQL query".into()));
    }

    let start = Instant::now();
    debug!("⚡ Executing SQL...");

    let result = connector.execute(trimmed_query).await;

    let duration = start.elapsed();

    match result {
        Ok(_) => {
            info!(target: "performance", "✅ Query finished in {:.2?}", duration);
            Ok(())
        }
        Err(e) => {
            error!(
                target: "security", 
                "❌ Query failed after {:.2?}: {} | SQL: {}", 
                duration, e, preview_sql(trimmed_query)
            );
            Err(e)
        }
    }
}

fn preview_sql(sql: &str) -> String {
    if sql.len() > 1000 {
        format!("{}...", &sql[..1000].replace('\n', " "))
    } else {
        sql.replace('\n', " ")
    }
}