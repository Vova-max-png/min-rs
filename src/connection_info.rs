use std::io::{Error, ErrorKind};

#[derive(PartialEq, Clone, Debug)]
pub enum ConnectionStatus {
    Disconnected,
    Connected,
}

/// Connection info struct is used to control connection state
#[derive(PartialEq, Clone, Debug)]
pub struct ConnectionInfo {
    status: ConnectionStatus,
    retries: i8,
    max_retries: i8,
}

impl ConnectionInfo {
    pub fn new() -> Self {
        Self {
            status: ConnectionStatus::Disconnected,
            retries: 0,
            max_retries: 3
        }
    }

    pub fn increase_retries(&mut self) -> Result<(), Error> {
        if self.retries == self.max_retries {
            return Err(Error::new(ErrorKind::OutOfMemory, format!("All {} available retries were exceeded", self.max_retries)))
        }
        self.retries += 1;
        Ok(())
    }

    pub fn reset_retries(&mut self) -> Result<(), Error> {
        self.retries = 0;
        Ok(())
    }
}