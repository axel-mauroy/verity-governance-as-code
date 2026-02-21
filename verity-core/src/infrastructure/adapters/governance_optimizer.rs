// verity-core/src/infrastructure/adapters/governance_optimizer.rs

use std::collections::HashMap;

use datafusion::common::Result as DFResult;
use datafusion::common::ScalarValue;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::logical_expr::{Expr, LogicalPlan, Projection};
use datafusion::optimizer::AnalyzerRule;
use datafusion::prelude::*;

use crate::domain::governance::{GovernancePolicySet, MaskingStrategy};

#[derive(Debug)]
pub struct GovernanceRule {
    policies: HashMap<String, MaskingStrategy>,
    salt: Option<String>,
}

impl GovernanceRule {
    pub fn new(policy_set: GovernancePolicySet) -> Self {
        Self {
            salt: policy_set.salt.clone(),
            policies: policy_set.column_policies,
        }
    }

    fn rewrite_expr(&self, expr: Expr) -> Expr {
        match &expr {
            Expr::Column(col) => {
                let col_name_clean = col.name().to_lowercase().trim_matches('"').to_string();
                if let Some(strategy) = self.policies.get(&col_name_clean) {
                    self.apply_policy(&expr, col.name(), strategy)
                } else {
                    expr
                }
            }
            Expr::Alias(alias) => {
                if let Expr::Column(col) = alias.expr.as_ref() {
                    let col_name_clean = col.name().to_lowercase().trim_matches('"').to_string();
                    if let Some(strategy) = self.policies.get(&col_name_clean) {
                        return self.apply_policy(&alias.expr, &alias.name, strategy);
                    }
                }
                expr
            }
            _ => expr,
        }
    }

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

    fn apply_policy(&self, col_expr: &Expr, alias_name: &str, strategy: &MaskingStrategy) -> Expr {
        match strategy {
            MaskingStrategy::Hash => self.build_hash_expr(col_expr).alias(alias_name),
            MaskingStrategy::Redact => lit("REDACTED").alias(alias_name),
            MaskingStrategy::Nullify => lit(ScalarValue::Utf8(None)).alias(alias_name),
            MaskingStrategy::Partial => {
                concat(vec![left(col_expr.clone(), lit(2)), lit("***")]).alias(alias_name)
            }
            MaskingStrategy::MaskEmail => regexp_replace(
                col_expr.clone(),
                lit("(^.).*(@.*$)"),
                lit("\\1****\\2"),
                None,
            )
            .alias(alias_name),
            MaskingStrategy::EntityPreserving => {
                concat(vec![lit("[PRESERVED_"), length(col_expr.clone()), lit("]")])
                    .alias(alias_name)
            }
        }
    }

    fn transform_plan(&self, plan: LogicalPlan) -> DFResult<LogicalPlan> {
        plan.transform(|node| match node {
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
        })
        .map(|t| t.data)
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
