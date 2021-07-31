use thiserror::Error;

#[derive(Error, Debug)]
pub enum ColStoreError {
    #[error("No such field with name'{0}' found.")]
    InvalidField(&'static str),
}
