// verity-core/src/ports/protocol.rs

use serde::{Deserialize, Serialize};

/// Standard JSON-RPC 2.0 Request
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorRequest {
    pub jsonrpc: String,
    pub method: String,
    pub params: serde_json::Value,
    pub id: u64,
}

/// Standard JSON-RPC 2.0 Response
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorResponse {
    pub jsonrpc: String,
    pub result: Option<serde_json::Value>,
    pub error: Option<ConnectorError>,
    pub id: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorError {
    pub code: i32,
    pub message: String,
}

/// Parameters for "handshake" method
#[derive(Debug, Serialize, Deserialize)]
pub struct HandshakeParams {
    pub core_version: String,
}

/// Result for "handshake" method
#[derive(Debug, Serialize, Deserialize)]
pub struct HandshakeResult {
    pub connector_version: String,
}

/// Parameters for "fetch_sample" method
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchSampleParams {
    pub query: String,
}

/// Parameters for "execute" method
#[derive(Debug, Serialize, Deserialize)]
pub struct ExecuteParams {
    pub query: String,
}

/// Parameters for "fetch_columns" method
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchColumnsParams {
    pub table_name: String,
}

/// Parameters for "register_source" method
#[derive(Debug, Serialize, Deserialize)]
pub struct RegisterSourceParams {
    pub name: String,
    pub path: String,
}

/// Parameters for "materialize" method
#[derive(Debug, Serialize, Deserialize)]
pub struct MaterializeParams {
    pub table_name: String,
    pub sql: String,
    pub materialization_type: String,
}

/// Parameters for "query_scalar" method
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryScalarParams {
    pub query: String,
}

/// Parameters for "fetch_column_averages" method
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchColumnAveragesParams {
    pub table_name: String,
    pub columns: Vec<String>,
}

/// Result for "fetch_columns"
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchColumnsResult {
    pub columns: Vec<ColumnSchemaInternal>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnSchemaInternal {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

impl From<ColumnSchemaInternal> for crate::ports::connector::ColumnSchema {
    fn from(val: ColumnSchemaInternal) -> Self {
        Self {
            name: val.name,
            data_type: val.data_type,
            is_nullable: val.is_nullable,
        }
    }
}

impl From<crate::ports::connector::ColumnSchema> for ColumnSchemaInternal {
    fn from(val: crate::ports::connector::ColumnSchema) -> Self {
        Self {
            name: val.name,
            data_type: val.data_type,
            is_nullable: val.is_nullable,
        }
    }
}

/// Result for "fetch_sample" (Simplified JSON representation for now)
#[derive(Debug, Serialize, Deserialize)]
pub struct FetchSampleResult {
    pub schema: Vec<ColumnSchemaInternal>,
    pub rows: Vec<serde_json::Value>,
}
