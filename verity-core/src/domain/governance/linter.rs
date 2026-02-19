// verity-core/src/domain/governance/linter.rs

use crate::domain::project::manifest::ManifestNode;
use anyhow::{anyhow, Result};
use datafusion::arrow::array::{Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use regex::Regex;

pub struct GovernanceLinter {
    pii_patterns: Vec<(String, Regex)>,
}

impl Default for GovernanceLinter {
    fn default() -> Self {
        Self::new()
    }
}

impl GovernanceLinter {
    pub fn new() -> Self {
        Self {
            pii_patterns: vec![
                (
                    "EMAIL".to_string(),
                    Regex::new(r"(?i)[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}").expect("Invalid email regex"),
                ),
                (
                    "CREDIT_CARD".to_string(),
                    Regex::new(r"\b(?:\d[ -]*?){13,16}\b").expect("Invalid CC regex"),
                ),
            ],
        }
    }

    /// Analyse un échantillon de données pour vérifier si les tags de sécurité correspondent à la réalité.
    pub fn verify_model_compliance(&self, model: &ManifestNode, sample_data: &RecordBatch) -> Result<()> {
        for (col_idx, field) in sample_data.schema().fields().iter().enumerate() {
            let column_name = field.name();

            // Si la colonne n'est pas marquée comme sensible...
            if !model.is_flagged_as_pii(column_name) {
                // ... on vérifie si elle contient des données sensibles
                let col_array = sample_data.column(col_idx);
                
                if let Some(string_array) = col_array.as_any().downcast_ref::<StringArray>() {
                    if self.detect_pii_leak(string_array) {
                        return Err(anyhow!(
                            "COMPLIANCE ERROR: PII detected in column '{}' of model '{}' \
                            without mandatory security tags (e.g. policy: pii_masking).",
                            column_name,
                            model.name
                        ));
                    }
                }
            }
        }
        Ok(())
    }

    fn detect_pii_leak(&self, array: &StringArray) -> bool {
        // Optimisation: on vérifie sur un échantillon pour ne pas pénaliser les immenses batchs
        // Mais puisqu'on est dans un linter ou un test, on peut scanner tout le RecordBatch
        for i in 0..array.len() {
            if array.is_null(i) {
                continue;
            }
            let val = array.value(i);
            for (_, regex) in &self.pii_patterns {
                if regex.is_match(val) {
                    return true;
                }
            }
        }
        false
    }
}
