// src/api/validation.rs
use crate::config::Settings;
use std::collections::HashMap;

/// Trait for validating DTOs dynamically using runtime configuration settings.
pub trait RuntimeValidatable {
    /// Validates the struct against the provided settings.
    /// Returns a map of field names to error messages if validation fails.
    fn validate_with_config(&self, config: &Settings) -> Result<(), HashMap<&'static str, String>>;
}
