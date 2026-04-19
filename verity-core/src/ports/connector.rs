use crate::error::VerityError;
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

use crate::ports::protocol::{
    ConnectorRequest, ConnectorResponse, ExecuteParams, FetchColumnsParams, MaterializeParams,
    QueryScalarParams,
};

// Struct simple pour décrire une colonne (indépendant de la DB)
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: String,
    pub is_nullable: bool,
}

#[async_trait]
pub trait Connector: Send + Sync {
    /// Retrieve a data sample as Arrow RecordBatches for dynamic validation.
    async fn fetch_sample(&self, _query: &str) -> Result<Vec<RecordBatch>, VerityError> {
        Err(VerityError::InternalError(
            "fetch_sample not implemented for this connector".into(),
        ))
    }
    /// Execute a SQL statement (DDL or DML, no result expected).
    async fn execute(&self, query: &str) -> Result<(), VerityError>;

    /// Fetch the column schema of a table/view.
    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError>;

    /// Register a data source (e.g. CSV file) as a named table/view.
    async fn register_source(&self, name: &str, path: &std::path::Path) -> Result<(), VerityError>;

    /// Materialize a SQL query as a table or view.
    async fn materialize(
        &self,
        table_name: &str,
        sql: &str,
        materialization_type: &str,
    ) -> Result<String, VerityError>;

    /// Execute a query and return a single scalar u64 value.
    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError>;

    /// Fetch the average value of multiple columns in a single query pass.
    async fn fetch_column_averages(
        &self,
        table_name: &str,
        columns: &[&str],
    ) -> Result<std::collections::HashMap<String, f64>, VerityError> {
        let mut result = std::collections::HashMap::new();
        for &col in columns {
            let query = format!("SELECT AVG(\"{}\") FROM \"{}\"", col, table_name);
            if let Ok(v) = self.query_scalar(&query).await {
                result.insert(col.to_string(), v as f64);
            }
        }
        Ok(result)
    }

    /// Return the engine name (for logging purposes).
    fn engine_name(&self) -> &str;

    /// Whether this engine handles governance at the plan level.
    fn supports_plan_governance(&self) -> bool {
        false
    }

    /// Register governance masking policies at the engine level.
    async fn register_governance(&self, _policies: crate::domain::governance::GovernancePolicySet) {
        // No-op by default
    }
}

/// A Connector that delegates work to an external binary via JSON-RPC over stdin/stdout.
pub struct ProxyConnector {
    engine_name: String,
    stdin: Mutex<ChildStdin>,
    reader: Mutex<BufReader<ChildStdout>>,
}

impl ProxyConnector {
    pub async fn new(binary_path: &str, engine_name: &str) -> Result<Self, VerityError> {
        let mut child = Command::new(binary_path)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| VerityError::InternalError(format!("Failed to start connector: {}", e)))?;

        let stdin = child
            .stdin
            .take()
            .ok_or(VerityError::InternalError("No stdin".into()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(VerityError::InternalError("No stdout".into()))?;

        Ok(Self {
            engine_name: engine_name.to_string(),
            stdin: Mutex::new(stdin),
            reader: Mutex::new(BufReader::new(stdout)),
        })
    }

    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, VerityError> {
        let request = ConnectorRequest {
            jsonrpc: "2.0".into(),
            method: method.into(),
            params,
            id: 1,
        };

        let mut req_str = serde_json::to_string(&request)
            .map_err(|e| VerityError::InternalError(format!("Serialization failed: {}", e)))?;
        req_str.push('\n');

        // Execute call within locks
        let mut stdin = self.stdin.lock().await;
        stdin
            .write_all(req_str.as_bytes())
            .await
            .map_err(|e| VerityError::InternalError(format!("I/O write failed: {}", e)))?;
        stdin
            .flush()
            .await
            .map_err(|e| VerityError::InternalError(format!("I/O flush failed: {}", e)))?;

        let mut reader = self.reader.lock().await;
        let mut line = String::new();
        loop {
            line.clear();
            let bytes_read = reader
                .read_line(&mut line)
                .await
                .map_err(|e| VerityError::InternalError(format!("I/O read failed: {}", e)))?;

            if bytes_read == 0 {
                return Err(VerityError::InternalError(
                    "Connector closed stdout unexpectedly".into(),
                ));
            }

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            // ENFORCE RESILIENCE: If a rogue library wrote plain text to stdout, intercept and warn
            // (JSON-RPC requests/responses logically always start with '{')
            if !trimmed.starts_with('{') {
                tracing::warn!("Connector stdout leaked non-JSON: {}", trimmed);
                continue;
            }

            let response: ConnectorResponse = match serde_json::from_str(trimmed) {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::error!("Failed to parse JSON-RPC: {} (Payload: {})", e, trimmed);
                    continue; // Attempt to gracefully recover by reading next line
                }
            };

            if let Some(err) = response.error {
                return Err(VerityError::InternalError(err.message));
            }

            return Ok(response.result.unwrap_or(serde_json::Value::Null));
        }
    }
}

#[async_trait]
impl Connector for ProxyConnector {
    async fn execute(&self, query: &str) -> Result<(), VerityError> {
        let params = serde_json::to_value(ExecuteParams {
            query: query.into(),
        })
        .map_err(|e| VerityError::InternalError(format!("Param serialization failed: {}", e)))?;
        self.call("execute", params).await?;
        Ok(())
    }

    async fn fetch_columns(&self, table_name: &str) -> Result<Vec<ColumnSchema>, VerityError> {
        let params = serde_json::to_value(FetchColumnsParams {
            table_name: table_name.into(),
        })
        .map_err(|e| VerityError::InternalError(format!("Param serialization failed: {}", e)))?;
        let res = self.call("fetch_columns", params).await?;
        let cols: Vec<ColumnSchema> = serde_json::from_value(res).map_err(|e| {
            VerityError::InternalError(format!("Schema deserialization failed: {}", e))
        })?;
        Ok(cols)
    }

    async fn register_source(&self, name: &str, path: &std::path::Path) -> Result<(), VerityError> {
        let params = serde_json::to_value(crate::ports::protocol::RegisterSourceParams {
            name: name.into(),
            path: path.to_string_lossy().into(),
        })
        .map_err(|e| VerityError::InternalError(format!("Param serialization failed: {}", e)))?;
        self.call("register_source", params).await?;
        Ok(())
    }

    async fn materialize(
        &self,
        table_name: &str,
        sql: &str,
        materialization_type: &str,
    ) -> Result<String, VerityError> {
        let params = serde_json::to_value(MaterializeParams {
            table_name: table_name.into(),
            sql: sql.into(),
            materialization_type: materialization_type.into(),
        })
        .map_err(|e| VerityError::InternalError(format!("Param serialization failed: {}", e)))?;
        let res = self.call("materialize", params).await?;
        Ok(res.as_str().unwrap_or("ok").into())
    }

    async fn query_scalar(&self, query: &str) -> Result<u64, VerityError> {
        let params = serde_json::to_value(QueryScalarParams {
            query: query.into(),
        })
        .map_err(|e| VerityError::InternalError(format!("Param serialization failed: {}", e)))?;
        let res = self.call("query_scalar", params).await?;
        Ok(res.as_u64().unwrap_or(0))
    }

    fn engine_name(&self) -> &str {
        &self.engine_name
    }
}

/// Helper to implement a standalone connector binary.
pub struct ConnectorRunner;

impl ConnectorRunner {
    pub async fn run(connector: Arc<dyn Connector>) -> anyhow::Result<()> {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut stdout = tokio::io::stdout();

        loop {
            let mut line = String::new();
            if reader.read_line(&mut line).await? == 0 {
                break;
            }

            let req: ConnectorRequest = match serde_json::from_str(&line) {
                Ok(r) => r,
                Err(_) => continue,
            };

            let res = match req.method.as_str() {
                "execute" => {
                    let params: ExecuteParams = serde_json::from_value(req.params)?;
                    match connector.execute(&params.query).await {
                        Ok(_) => ConnectorResponse {
                            jsonrpc: "2.0".into(),
                            result: Some(serde_json::Value::Null),
                            error: None,
                            id: req.id,
                        },
                        Err(e) => ConnectorResponse {
                            jsonrpc: "2.0".into(),
                            result: None,
                            error: Some(crate::ports::protocol::ConnectorError {
                                code: -1,
                                message: e.to_string(),
                            }),
                            id: req.id,
                        },
                    }
                }
                "fetch_columns" => {
                    let params: FetchColumnsParams = serde_json::from_value(req.params)?;
                    match connector.fetch_columns(&params.table_name).await {
                        Ok(cols) => ConnectorResponse {
                            jsonrpc: "2.0".into(),
                            result: Some(serde_json::to_value(cols)?),
                            error: None,
                            id: req.id,
                        },
                        Err(e) => ConnectorResponse {
                            jsonrpc: "2.0".into(),
                            result: None,
                            error: Some(crate::ports::protocol::ConnectorError {
                                code: -1,
                                message: e.to_string(),
                            }),
                            id: req.id,
                        },
                    }
                }
                "query_scalar" => {
                    let params: QueryScalarParams = serde_json::from_value(req.params)?;
                    match connector.query_scalar(&params.query).await {
                        Ok(v) => ConnectorResponse {
                            jsonrpc: "2.0".into(),
                            result: Some(v.into()),
                            error: None,
                            id: req.id,
                        },
                        Err(e) => ConnectorResponse {
                            jsonrpc: "2.0".into(),
                            result: None,
                            error: Some(crate::ports::protocol::ConnectorError {
                                code: -1,
                                message: e.to_string(),
                            }),
                            id: req.id,
                        },
                    }
                }
                _ => ConnectorResponse {
                    jsonrpc: "2.0".into(),
                    result: None,
                    error: Some(crate::ports::protocol::ConnectorError {
                        code: -32601,
                        message: "Method not found".into(),
                    }),
                    id: req.id,
                },
            };

            let mut res_str = serde_json::to_string(&res)?;
            res_str.push('\n');
            stdout.write_all(res_str.as_bytes()).await?;
            stdout.flush().await?;
        }
        Ok(())
    }
}
