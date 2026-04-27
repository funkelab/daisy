use thiserror::Error;

#[derive(Debug, Error)]
pub enum DaisyError {
    #[error("block processing failed: {0}")]
    ProcessFailed(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("dependency error: {0}")]
    DependencyError(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}
