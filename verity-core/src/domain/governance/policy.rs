// verity-core/src/governance/policy.rs

use serde::{Deserialize, Serialize};
use regex::Regex;
use validator::Validate;
use std::collections::HashMap;


#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
    Error,
}

impl Default for Severity {
    fn default() -> Self { Self::Low }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
#[serde(rename_all = "snake_case")]
pub enum Action {
    Mask,
    Block,
    Warn,
}

impl Default for Action {
    fn default() -> Self { Self::Warn }
}

// --- CONFIGURATION STRUCTS ---

#[derive(Debug, Serialize, Deserialize, Validate, Clone, Default)]
pub struct PolicyConfig {
    #[validate(nested)]
    #[serde(default)]
    pub pii_detection: PiiConfig,
    
    #[validate(nested)]
    #[serde(default)]
    pub data_quality: QualityConfig,
    
    #[serde(default)] 
    pub environments: HashMap<String, EnvOverride>,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
pub struct PiiConfig {
    #[serde(default)]
    pub enabled: bool,
    
    #[validate(nested)]
    #[validate(custom(function = "validate_unique_pii_names"))]
    #[serde(default)]
    pub patterns: Vec<PiiPattern>,
}

// Manual implementation of Default for PiiConfig
impl Default for PiiConfig {
    fn default() -> Self {
        Self { enabled: false, patterns: vec![] }
    }
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
pub struct PiiPattern {
    pub name: String,
    
    #[validate(length(min = 1, message = "Regex cannot be empty"))]
    pub regex: String, 
    
    pub severity: Severity,
    pub action: Action,
}

#[derive(Debug, Serialize, Deserialize, Validate, Clone)]
pub struct QualityConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub rules: Vec<EmbeddingRule>,
}

impl Default for QualityConfig {
    fn default() -> Self {
        Self { enabled: false, rules: vec![] }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EmbeddingRule {
    pub name: String,
    pub description: String,
    pub severity: Severity,
    pub expected_dimensions: Option<usize>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct EnvOverride {
    pub strict_mode: Option<bool>,
}

// --- RUNTIME STRUCTS ---

pub struct CompiledPiiPattern {
    pub name: String,
    pub regex: Regex,
    pub severity: Severity,
    pub action: Action,
}

impl PiiPattern {
    pub fn try_compile(&self) -> Result<CompiledPiiPattern, regex::Error> {
        Ok(CompiledPiiPattern {
            name: self.name.clone(),
            regex: Regex::new(&self.regex)?,
            severity: self.severity,
            action: self.action,
        })
    }
}

fn validate_unique_pii_names(_patterns: &[PiiPattern]) -> Result<(), validator::ValidationError> {
    Ok(())
}