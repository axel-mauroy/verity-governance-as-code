use crate::error::VerityError;

pub trait TemplateEngine: Send + Sync {
    fn render(&self, template: &str, context: &serde_json::Value) -> Result<String, VerityError>;
}
