# Future Architecture: The Data Plane Bottleneck & Arrow IPC

## 1. Context & Problem Formulation
With the decoupling of Verity Core and Connectors into standalone modular binaries, the current communication protocol relies entirely on **JSON-RPC over standard input/output (STDIO)**.

This choice is excellent for the **Control Plane**:
- DDL executions (`CREATE TABLE`, `CREATE VIEW`)
- Metadata extraction (`INFORMATION_SCHEMA` queries)
- Scalar validations (`COUNT(*)`)

These operations involve minimal data transfer. However, if Verity evolves to perform memory-intensive operations requiring **mass data extraction** (e.g., streaming hundreds of thousands of rows from BigQuery to Verity Core for line-by-line local validation or local PII regex scanning), purely textual JSON communication will become a massive bottleneck.
- **CPU Penalty**: Serializing and parsing massive JSON text arrays is computationally expensive.
- **Memory Penalty**: Text-based representation of numbers and large datasets requires significantly more RAM and buffer space than raw binary formats.

## 2. Planned Evolution: Hybrid Protocol

When heavy data extraction becomes a requirement (e.g., `fetch_sample` scaling to large previews or local validation runs), the architecture must adopt a hybrid protocol, separating control instructions from mass data payloads.

### Control Plane: Stays JSON-RPC
Instructions (e.g., `{"method": "fetch_sample", "query": "..."}`) and their associated metadata will remain as JSON-RPC over `stdin` / `stdout`. It remains the best format for debugging and extensibility.

### Data Plane: Apache Arrow IPC Streaming
Instead of responding with a gigantic JSON payload containing the row data, the connector should switch the output channel to a binary stream.
1. **Connector Side**: Upon receiving a heavy data request, the connector executes the query and uses `arrow::ipc::writer::StreamWriter` to dump raw columnar memory structures directly into `stdout`.
2. **Core Side (ProxyConnector)**: Upon expecting a data payload, the core shifts from reading lines of JSON to utilizing an `arrow::ipc::reader::StreamReader` to ingest the raw bytes from `stdout`.

### Benefits
1. **Zero-Copy Deserialization**: The bytes read by the Core are directly mapped to Rust `RecordBatch` structures without allocating intermediate text buffers or invoking parsers.
2. **Native Ecosystem Integration**: Verity already relies heavily on Apache DataFusion and Arrow internally. Arrow is the lingua franca of this modern data stack, meaning passing Arrow streams between processes introduces zero overhead.
