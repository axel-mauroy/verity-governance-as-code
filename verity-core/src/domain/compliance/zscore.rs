// verity-core/src/domain/compliance/zscore.rs

use crate::domain::compliance::anomaly::MetricState;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ZScoreError {
    #[error(
        "Z-Score anomaly detected on '{metric}': Score {z_score:.2} exceeds threshold {threshold:.2}. State: Mean {mean:.4}, StdDev {stddev:.4}, Current {current:.4}"
    )]
    AnomalyDetected {
        metric: String,
        z_score: f64,
        threshold: f64,
        current: f64,
        mean: f64,
        stddev: f64,
    },
    #[error("Not enough history for Z-Score check on '{0}' (need at least 2 runs)")]
    NotEnoughHistory(String),
}

pub struct ZScoreCheck;

impl ZScoreCheck {
    /// Checks a new value against the historical distribution.
    /// Does NOT modify state — call `update_state` only if you accept the value.
    pub fn validate(
        metric_name: &str,
        current_value: f64,
        state: &MetricState,
        threshold: f64,
    ) -> Result<(), ZScoreError> {
        // Need at least count >= 2 to have a meaningful variance
        if state.count < 2 {
            return Err(ZScoreError::NotEnoughHistory(metric_name.to_string()));
        }

        let stddev = state.variance.sqrt();
        if stddev > 1e-9 {
            let z_score = ((current_value - state.mean) / stddev).abs();
            if z_score > threshold {
                return Err(ZScoreError::AnomalyDetected {
                    metric: metric_name.to_string(),
                    z_score,
                    threshold,
                    current: current_value,
                    mean: state.mean,
                    stddev,
                });
            }
        }
        Ok(())
    }

    /// Updates the rolling state with a new value using Welford's online algorithm.
    /// Only call this after `validate` returns Ok (or on the first run to initialize).
    pub fn update_state(current_value: f64, state: Option<MetricState>) -> MetricState {
        let mut s = state.unwrap_or_default();
        s.count += 1;
        if s.count == 1 {
            s.mean = current_value;
            s.variance = 0.0;
        } else {
            let old_mean = s.mean;
            s.mean += (current_value - old_mean) / (s.count as f64);
            // Welford: M2_new = M2_old + (x - old_mean) * (x - new_mean)
            let prev_m2 = s.variance * (s.count - 1) as f64;
            let new_m2 = prev_m2 + (current_value - old_mean) * (current_value - s.mean);
            s.variance = new_m2 / s.count as f64;
        }
        s
    }

    /// Convenience: validates THEN updates state if clean.
    /// Returns the result and the new state (unchanged if anomaly detected).
    pub fn validate_and_update(
        metric_name: &str,
        current_value: f64,
        previous_state: Option<MetricState>,
        threshold: f64,
    ) -> (Result<(), ZScoreError>, MetricState) {
        let res = match &previous_state {
            None => Err(ZScoreError::NotEnoughHistory(metric_name.to_string())),
            Some(s) => Self::validate(metric_name, current_value, s, threshold),
        };

        // CRITICAL: only update state with clean values (or first run).
        // If an anomaly is detected, preserve the previous state unchanged
        // to prevent polluting the historical mean/variance.
        let new_state = match &res {
            Ok(()) | Err(ZScoreError::NotEnoughHistory(_)) => {
                Self::update_state(current_value, previous_state)
            }
            Err(ZScoreError::AnomalyDetected { .. }) => {
                // Return previous state unchanged — anomaly is NOT ingested
                previous_state.unwrap_or_default()
            }
        };

        (res, new_state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zscore_no_history() {
        let (res, new_state) = ZScoreCheck::validate_and_update("prob", 0.5, None, 3.0);
        assert!(matches!(res, Err(ZScoreError::NotEnoughHistory(_))));
        assert_eq!(new_state.count, 1);
        assert_eq!(new_state.mean, 0.5);
    }

    #[test]
    fn test_zscore_valid_drift() {
        let state = MetricState {
            mean: 0.5,
            variance: 0.01, // stddev = 0.1
            count: 10,
        };
        // 0.6 is exactly 1 stddev away (Z = 1.0)
        let (res, new_state) = ZScoreCheck::validate_and_update("prob", 0.6, Some(state), 3.0);
        assert!(res.is_ok());
        assert_eq!(new_state.count, 11);
    }

    #[test]
    fn test_zscore_anomaly() {
        let state = MetricState {
            mean: 0.5,
            variance: 0.01, // stddev = 0.1
            count: 10,
        };
        // 0.9 is 4 stddev away (Z = 4.0), threshold is 3.0
        let (res, _) = ZScoreCheck::validate_and_update("prob", 0.9, Some(state), 3.0);
        assert!(matches!(
            res,
            Err(ZScoreError::AnomalyDetected { z_score, .. }) if (z_score - 4.0).abs() < 1e-6
        ));
    }

    /// Circuit breaker test: simulates 10 runs with stable prediction avg (~0.25)
    /// then injects a 5-sigma spike (avg >> 0.25) on run 11 — the pipeline MUST refuse.
    #[test]
    fn test_zscore_circuit_breaker_at_5_sigma_cuts_pipeline() {
        let threshold = 3.0; // classic 3-sigma
        let mut state: Option<MetricState> = None;

        // Simulate 10 stable runs around mean=0.25, small jitter
        let stable_values = [0.24, 0.26, 0.25, 0.23, 0.27, 0.25, 0.24, 0.26, 0.25, 0.25];
        for val in stable_values {
            let (_, new_state) =
                ZScoreCheck::validate_and_update("avg_churn_prob", val, state, threshold);
            state = Some(new_state);
        }

        // On run 11: inject a brutal 5-sigma deviation (avg = 0.75 when mean is ~0.25, stddev ~ 0.01)
        let (res_normal, _) =
            ZScoreCheck::validate_and_update("avg_churn_prob", 0.25, state.clone(), threshold);
        assert!(
            res_normal.is_ok(),
            "A normal run must not trigger the circuit breaker"
        );

        // 5 sigma departure: should trip the circuit breaker
        let stable_state_before = state.clone();
        let (res_anomaly, state_after_anomaly) =
            ZScoreCheck::validate_and_update("avg_churn_prob", 0.95, state, threshold);
        assert!(
            matches!(res_anomaly, Err(ZScoreError::AnomalyDetected { .. })),
            "A 5 sigma drift must cut the circuit breaker"
        );
        // CRITICAL: the historical state must be unchanged — anomaly must NOT be ingested
        if let Some(before) = stable_state_before {
            assert_eq!(
                state_after_anomaly.count, before.count,
                "The state.count must not change after an anomaly (no pollution)"
            );
        }
    }
}
