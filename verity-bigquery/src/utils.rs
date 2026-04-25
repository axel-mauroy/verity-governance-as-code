// verity-bigquery/src/utils.rs

use gcp_bigquery_client::model::table_row::TableRow;

pub fn cell_value(row: &TableRow, index: usize) -> Option<String> {
    let val = row.columns.as_ref()?.get(index)?.value.as_ref()?;
    match val {
        serde_json::Value::Null => None,
        serde_json::Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}
