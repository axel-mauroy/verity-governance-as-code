// verity-core/src/domain/compliance/anomaly.rs

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AnomalyError {
    #[error(
        "Row count deviation too high: {deviation:.2}% (Threshold: {threshold:.2}%). Prev: {prev}, Curr: {curr}"
    )]
    DeviationExceeded {
        deviation: f64,
        threshold: f64,
        prev: u64,
        curr: u64,
    },
    #[error("No historical data available for anomaly check (First Run).")]
    NoHistory,
}

/// Structure to persist the state between runs
#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct ModelExecutionState {
    pub last_run_at: String,
    pub row_count: u64,
    // Store arbitrary ML metrics (e.g. "avg_churn_prob" -> { "mean": 0.12, "stddev": 0.05, "count": 100 })
    #[serde(default)]
    pub ml_metrics: std::collections::HashMap<String, MetricState>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
pub struct MetricState {
    pub mean: f64,
    pub variance: f64,
    pub count: u64,
}

pub struct RowCountCheck;

impl RowCountCheck {
    /// Check if an anomaly exists.
    ///
    /// # Arguments
    /// * `current_count` - The number of rows inserted.
    /// * `previous_count` - The number of rows from the previous run (Option car peut ne pas exister).
    /// * `threshold` - The tolerance (ex: 0.05 for 5%).
    pub fn validate(
        current_count: u64,
        previous_count: Option<u64>,
        threshold: f64,
    ) -> Result<(), AnomalyError> {
        // 1. Handle "Cold Start" (First run)
        let prev = match previous_count {
            Some(p) if p > 0 => p,
            Some(_) => return Ok(()), // If prev = 0, accept all (or define a strict rule)
            None => return Err(AnomalyError::NoHistory), // Warning plutôt qu'erreur bloquante en prod
        };

        // 2. Calculate the Delta
        let diff = (current_count as i64 - prev as i64).abs();

        // 3. Calculate the Score (Ratio)
        let ratio = diff as f64 / prev as f64;

        // 4. Check the threshold
        if ratio > threshold {
            return Err(AnomalyError::DeviationExceeded {
                deviation: ratio * 100.0,
                threshold: threshold * 100.0,
                prev,
                curr: current_count,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_deviation() {
        assert!(RowCountCheck::validate(1040, Some(1000), 0.05).is_ok());
    }

    #[test]
    fn test_invalid_deviation_growth() {
        let _res = RowCountCheck::validate(1100, Some(1000), 0.05);
        let res = RowCountCheck::validate(1100, Some(1000), 0.05);
        assert!(matches!(res, Err(AnomalyError::DeviationExceeded { .. })));
    }

    #[test]
    fn test_invalid_deviation_shrink() {
        let res = RowCountCheck::validate(900, Some(1000), 0.05);
        assert!(matches!(res, Err(AnomalyError::DeviationExceeded { .. })));
    }

    #[test]
    fn test_first_run() {
        let res = RowCountCheck::validate(100, None, 0.05);
        assert!(matches!(res, Err(AnomalyError::NoHistory)));
    }
}
