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
        let (base_expr, top_level_alias) = match expr {
            Expr::Alias(alias) => (*alias.expr.clone(), Some(alias.name.clone())),
            Expr::Column(col) => (Expr::Column(col.clone()), Some(col.name.clone())),
            other => (other, None),
        };

        let fallback_expr = base_expr.clone();

        let transformed_expr = base_expr
            .transform(|e| {
                if let Expr::Column(col) = &e {
                    let col_name_clean = col.name().to_lowercase().trim_matches('"').to_string();
                    if let Some(strategy) = self.policies.get(&col_name_clean) {
                        let masked = self.apply_policy_raw(&e, strategy);
                        return Ok(Transformed::yes(masked));
                    }
                }
                Ok(Transformed::no(e))
            })
            .unwrap_or_else(|_| Transformed::no(fallback_expr))
            .data;

        if let Some(name) = top_level_alias {
            transformed_expr.alias(name)
        } else {
            transformed_expr
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

        encode(digest(hash_input, lit("sha256")), lit("hex"))
    }

    fn apply_policy_raw(&self, col_expr: &Expr, strategy: &MaskingStrategy) -> Expr {
        match strategy {
            MaskingStrategy::Hash => self.build_hash_expr(col_expr),
            MaskingStrategy::Redact => lit("REDACTED"),
            MaskingStrategy::Nullify => lit(ScalarValue::Utf8(None)),
            MaskingStrategy::Partial => concat(vec![left(col_expr.clone(), lit(2)), lit("***")]),
            MaskingStrategy::MaskEmail => regexp_replace(
                col_expr.clone(),
                lit("(^.).*(@.*$)"),
                lit("\\1****\\2"),
                None,
            ),
            MaskingStrategy::EntityPreserving => {
                // Remplacement de length() par character_length() si length() n'est pas reconnu
                concat(vec![
                    lit("[PRESERVED_"),
                    character_length(col_expr.clone()),
                    lit("]"),
                ])
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
