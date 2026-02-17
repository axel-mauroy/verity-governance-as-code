// verity-core/src/infrastructure/adapters/governance_optimizer.rs
//
// DataFusion AnalyzerRule that intercepts Projection nodes
// and rewrites columns matching governance policies (PII masking).
//
// Uses AnalyzerRule (not OptimizerRule) because analysis runs BEFORE optimization.
// The optimizer's built-in passes may eliminate Projections (e.g. unnecessary projection
// removal), which would prevent an OptimizerRule from seeing them.
//
// Design notes:
// - SHA-256 hashing is CPU-intensive but DataFusion's vectorized execution
//   parallelizes it natively across all cores (Rust + DataFusion >> dbt/Python).
// - A configurable salt is prepended to hash inputs to prevent dictionary attacks
//   on PII values: digest(concat(col, salt), 'sha256').

use std::collections::HashMap;

use datafusion::common::Result as DFResult;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::*;

use crate::domain::governance::governance_rule::{GovernancePolicySet, MaskingPolicy};

/// A DataFusion `AnalyzerRule` that rewrites column projections
/// to apply governance masking at the logical plan level.
///
/// Instead of wrapping SQL strings in CTEs (like `PolicyRewriter`),
/// this rule operates on the AST: it finds `Projection` nodes and
/// replaces column expressions with their masked equivalents.
#[derive(Debug)]
pub struct GovernanceRule {
    /// Maps lowercase column name → masking policy
    policies: HashMap<String, MaskingPolicy>,
    /// Optional salt for hash-based masking (prevents dictionary attacks)
    salt: Option<String>,
}

impl GovernanceRule {
    pub fn new(policy_set: GovernancePolicySet) -> Self {
        Self {
            salt: policy_set.salt.clone(),
            policies: policy_set.column_policies,
        }
    }

    /// Rewrite a single expression if it matches a governance policy.
    fn rewrite_expr(&self, expr: Expr) -> Expr {
        match &expr {
            Expr::Column(col) => {
                let col_name = col.name().to_lowercase();
                if let Some(policy) = self.policies.get(&col_name) {
                    self.apply_policy(&expr, col.name(), policy)
                } else {
                    expr
                }
            }
            Expr::Alias(alias) => {
                let inner = *alias.expr.clone();
                match &inner {
                    Expr::Column(col) => {
                        let col_name = col.name().to_lowercase();
                        if let Some(policy) = self.policies.get(&col_name) {
                            self.apply_policy(&inner, &alias.name, policy)
                        } else {
                            expr
                        }
                    }
                    _ => expr,
                }
            }
            _ => expr,
        }
    }

    /// Build the hashing expression: digest(concat(cast(col AS Utf8), salt), 'sha256')
    /// If no salt is configured, uses: digest(cast(col AS Utf8), 'sha256')
    fn build_hash_expr(&self, col_expr: &Expr) -> Expr {
        let casted = Expr::Cast(datafusion::logical_expr::Cast {
            expr: Box::new(col_expr.clone()),
            data_type: datafusion::arrow::datatypes::DataType::Utf8,
        });

        let hash_input = if let Some(salt) = &self.salt {
            concat(vec![casted, lit(salt.as_str())])
        } else {
            casted
        };

        digest(hash_input, lit("sha256"))
    }

    /// Apply a masking policy to a column expression, aliasing with the original name.
    fn apply_policy(&self, col_expr: &Expr, alias_name: &str, policy: &MaskingPolicy) -> Expr {
        match policy {
            MaskingPolicy::Hash | MaskingPolicy::PiiMasking => {
                self.build_hash_expr(col_expr).alias(alias_name)
            }
            MaskingPolicy::Redact => lit("REDACTED").alias(alias_name),
            MaskingPolicy::MaskEmail => regexp_replace(
                col_expr.clone(),
                lit("(^.).*(@.*$)"),
                lit("\\1****\\2"),
                None,
            )
            .alias(alias_name),
        }
    }

    /// Recursively transform the plan tree, rewriting Projection nodes.
    fn transform_plan(&self, plan: LogicalPlan) -> DFResult<LogicalPlan> {
        let transformed = plan.transform(|node| match node {
            LogicalPlan::Projection(proj) => {
                let new_exprs: Vec<Expr> = proj
                    .expr
                    .into_iter()
                    .map(|e| self.rewrite_expr(e))
                    .collect();

                let new_proj = Projection::try_new(new_exprs, proj.input)?;
                Ok(Transformed::yes(LogicalPlan::Projection(new_proj)))
            }
            other => Ok(Transformed::no(other)),
        })?;

        Ok(transformed.data)
    }
}

impl AnalyzerRule for GovernanceRule {
    fn name(&self) -> &str {
        "verity_governance_masking"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> DFResult<LogicalPlan> {
        if self.policies.is_empty() {
            return Ok(plan);
        }
        self.transform_plan(plan)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use datafusion::arrow::util::display::array_value_to_string;
    use datafusion::prelude::SessionContext;
    use std::sync::Arc;

    fn make_policies() -> GovernancePolicySet {
        GovernancePolicySet::from_pairs(vec![
            ("email".to_string(), "hash".to_string()),
            ("ssn".to_string(), "redact".to_string()),
        ])
    }

    /// Helper: collect all values from column `col_idx` as strings.
    fn col_values(
        batches: &[datafusion::arrow::record_batch::RecordBatch],
        col_idx: usize,
    ) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let col = b.column(col_idx);
                (0..b.num_rows()).map(move |i| array_value_to_string(col, i).unwrap_or_default())
            })
            .collect()
    }

    #[tokio::test]
    async fn test_governance_rule_rewrites_projection() {
        let ctx = SessionContext::new();
        ctx.add_analyzer_rule(Arc::new(GovernanceRule::new(make_policies())));

        ctx.sql("CREATE TABLE test_users (id INT, email VARCHAR, ssn VARCHAR) AS VALUES (1, 'alice@test.com', '123-45-6789')")
            .await.unwrap().collect().await.unwrap();

        let batches = ctx
            .sql("SELECT id, email, ssn FROM test_users")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let ids = col_values(&batches, 0);
        let emails = col_values(&batches, 1);
        let ssns = col_values(&batches, 2);

        assert_eq!(ids, vec!["1"]);
        assert_ne!(
            emails[0], "alice@test.com",
            "email should be masked by SHA-256"
        );
        assert_eq!(ssns, vec!["REDACTED"]);
    }

    #[tokio::test]
    async fn test_governance_rule_passes_through_clean_columns() {
        let ctx = SessionContext::new();

        let empty_policies = GovernancePolicySet::new();
        ctx.add_analyzer_rule(Arc::new(GovernanceRule::new(empty_policies)));

        ctx.sql("CREATE TABLE clean (id INT, name VARCHAR) AS VALUES (1, 'Alice')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batches = ctx
            .sql("SELECT id, name FROM clean")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let names = col_values(&batches, 1);
        assert_eq!(names, vec!["Alice"], "clean columns should be unchanged");
    }

    #[tokio::test]
    async fn test_redact_policy_replaces_value() {
        let ctx = SessionContext::new();

        let policies =
            GovernancePolicySet::from_pairs(vec![("secret".to_string(), "redact".to_string())]);
        ctx.add_analyzer_rule(Arc::new(GovernanceRule::new(policies)));

        ctx.sql("CREATE TABLE secrets (id INT, secret VARCHAR) AS VALUES (1, 'top_secret_data')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batches = ctx
            .sql("SELECT id, secret FROM secrets")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let secrets = col_values(&batches, 1);
        assert_eq!(secrets, vec!["REDACTED"]);
    }

    #[tokio::test]
    async fn test_hash_with_salt() {
        let ctx = SessionContext::new();

        let mut policies =
            GovernancePolicySet::from_pairs(vec![("email".to_string(), "hash".to_string())]);
        policies.salt = Some("verity_salt_2026".to_string());

        ctx.add_analyzer_rule(Arc::new(GovernanceRule::new(policies)));

        ctx.sql("CREATE TABLE salted (id INT, email VARCHAR) AS VALUES (1, 'alice@test.com')")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let batches = ctx
            .sql("SELECT id, email FROM salted")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let emails = col_values(&batches, 1);
        assert_ne!(emails[0], "alice@test.com", "email should be hashed");
        assert!(!emails[0].is_empty(), "hashed value should not be empty");
    }
}
