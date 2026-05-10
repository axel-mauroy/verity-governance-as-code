// verity-core/src/infrastructure/sql_transpiler.rs
//
// Translates SQL from the canonical BigQuery dialect to engine-specific dialects.
//
// Design rationale:
//   - The authoritative SQL dialect is BigQuery (production target).
//   - When running on DataFusion (local/CI), SQL is automatically transpiled at
//     the connector boundary — transparent to the user and to model authors.
//   - Uses sqlparser-rs (already a workspace dependency) for AST-level rewriting,
//     which is safer and more robust than regex string replacement.
//   - Falls back to the original SQL if parsing fails (e.g. internal DDL generated
//     by Verity itself, which is already in the target dialect).

use std::ops::ControlFlow;

use sqlparser::ast::visit_expressions_mut;
use sqlparser::ast::{DataType, Expr};
use sqlparser::dialect::{BigQueryDialect, GenericDialect};
use sqlparser::parser::Parser;

/// Transpiles SQL between engine dialects.
///
/// The canonical input dialect is **BigQuery**. Model authors write SQL in BigQuery
/// dialect; Verity transpiles automatically when the target engine is DataFusion.
pub struct SqlDialectTranspiler;

impl SqlDialectTranspiler {
    /// Transpile a BigQuery-dialect SQL statement to DataFusion-compatible SQL.
    ///
    /// Rewrites BigQuery-specific data types in `CAST` / `TRY_CAST` expressions
    /// to their ANSI SQL equivalents understood by DataFusion:
    ///
    /// | BigQuery  | DataFusion (ANSI)  |
    /// |-----------|--------------------|
    /// | FLOAT64   | DOUBLE PRECISION   |
    /// | INT64     | BIGINT             |
    /// | STRING    | VARCHAR            |
    /// | BOOL      | BOOLEAN            |
    /// | BYTES     | BYTEA              |
    ///
    /// Silently returns the original SQL if parsing fails (e.g. Verity-internal DDL).
    pub fn bigquery_to_datafusion(sql: &str) -> String {
        Self::try_transpile(sql).unwrap_or_else(|_| sql.to_string())
    }

    fn try_transpile(sql: &str) -> Result<String, sqlparser::parser::ParserError> {
        // Try BigQuery dialect first (correctly resolves FLOAT64, INT64, etc.)
        // Fall back to GenericDialect for SQL that BQ dialect cannot parse.
        let mut statements = Parser::parse_sql(&BigQueryDialect {}, sql)
            .or_else(|_| Parser::parse_sql(&GenericDialect {}, sql))?;

        // visit_expressions_mut walks every Expr node in every statement.
        let _ = visit_expressions_mut(&mut statements, |expr| {
            rewrite_cast_types(expr);
            ControlFlow::<()>::Continue(())
        });

        Ok(statements
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<_>>()
            .join("; "))
    }
}

/// Rewrite BigQuery-specific data types inside CAST / TRY_CAST expressions.
fn rewrite_cast_types(expr: &mut Expr) {
    if let Expr::Cast {
        data_type, kind, ..
    } = expr
    {
        // Handles both CAST(...) and TRY_CAST(...) — they share the same variant.
        // TRY_CAST is identified by `kind == CastKind::TryCast`.
        let _ = kind; // present for both variants; no special handling needed here.
        *data_type = bq_type_to_ansi(data_type.clone());
    }
}

/// Maps a BigQuery-specific `DataType` to its ANSI SQL / DataFusion equivalent.
fn bq_type_to_ansi(dt: DataType) -> DataType {
    match dt {
        // ── Numeric ───────────────────────────────────────────────────────────
        // FLOAT64 → DOUBLE PRECISION (unambiguous ANSI, no extra args needed)
        DataType::Float64 => DataType::DoublePrecision,
        // INT64 → BIGINT
        DataType::Int64 => DataType::BigInt(None),

        // ── String ────────────────────────────────────────────────────────────
        // BigQuery STRING → VARCHAR (no length constraint in the source model)
        DataType::String(_) => DataType::Varchar(None),

        // ── Bool / Bytes ──────────────────────────────────────────────────────
        DataType::Bool => DataType::Boolean,
        DataType::Bytes(_) => DataType::Bytea,

        // ── Custom fallback ───────────────────────────────────────────────────
        // When parsed with GenericDialect, BQ types may appear as Custom nodes.
        DataType::Custom(ref name, _) => match name.to_string().to_uppercase().as_str() {
            "FLOAT64" => DataType::DoublePrecision,
            "INT64" => DataType::BigInt(None),
            "STRING" => DataType::Varchar(None),
            "BOOL" => DataType::Boolean,
            "BYTES" => DataType::Bytea,
            _ => dt,
        },

        // ── Pass-through ──────────────────────────────────────────────────────
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_float64_to_double_precision() {
        let input = "SELECT CAST(quantity_kg AS FLOAT64) AS quantity_kg FROM stg_stock";
        let output = SqlDialectTranspiler::bigquery_to_datafusion(input);
        assert!(
            output.to_uppercase().contains("DOUBLE PRECISION"),
            "Expected DOUBLE PRECISION in: {output}"
        );
        assert!(
            !output.to_uppercase().contains("FLOAT64"),
            "Unexpected FLOAT64 in: {output}"
        );
    }

    #[test]
    fn test_int64_to_bigint() {
        let input = "SELECT CAST(expiry_days AS INT64) AS expiry_days FROM stg_stock";
        let output = SqlDialectTranspiler::bigquery_to_datafusion(input);
        assert!(
            output.to_uppercase().contains("BIGINT"),
            "Expected BIGINT in: {output}"
        );
        assert!(
            !output.to_uppercase().contains("INT64"),
            "Unexpected INT64 in: {output}"
        );
    }

    #[test]
    fn test_multiple_casts_in_one_query() {
        let input = r#"
            WITH source AS (SELECT * FROM erp_donations),
            renamed AS (
                SELECT
                    donation_id,
                    CAST(transport_cost_eur AS FLOAT64) AS transport_cost_eur,
                    CAST(distance_km AS FLOAT64) AS distance_km
                FROM source
            )
            SELECT * FROM renamed
        "#;
        let output = SqlDialectTranspiler::bigquery_to_datafusion(input);
        assert!(
            !output.to_uppercase().contains("FLOAT64"),
            "Unexpected FLOAT64 in: {output}"
        );
        assert!(
            output.to_uppercase().contains("DOUBLE PRECISION"),
            "Expected DOUBLE PRECISION in: {output}"
        );
    }

    #[test]
    fn test_no_op_for_standard_types() {
        let input = "SELECT CAST(x AS DOUBLE PRECISION) FROM t";
        let output = SqlDialectTranspiler::bigquery_to_datafusion(input);
        assert!(output.to_uppercase().contains("DOUBLE PRECISION"));
    }

    #[test]
    fn test_fallback_on_unparseable_sql() {
        let original = "THIS IS NOT SQL @@@@";
        let output = SqlDialectTranspiler::bigquery_to_datafusion(original);
        assert_eq!(output, original, "Should return original on parse failure");
    }
}
