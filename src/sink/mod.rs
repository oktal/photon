use std::collections::HashMap;

use thiserror::Error;

use crate::point::Points;

mod console;
mod influxdb;

#[derive(Error, Debug)]
pub enum Error {
    #[error("unknown sink {0}")]
    Unknown(String),

    #[error("invalid TOML: {0}")]
    Toml(#[source] toml::de::Error),

    #[error("invalid configuration for {1}: {0}")]
    Config(#[source] Box<dyn std::error::Error>, String),
}

pub type SinkResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait Sink {
    fn sink(&self, points: &Points) -> SinkResult<()>;
}

pub trait SinkConfig: Send + Sync {
    fn build(self) -> SinkResult<Box<dyn Sink>>;
}

pub struct Registration {
    name: &'static str,

    builder: fn(&str, toml::Value) -> Result<Box<dyn Sink>, Error>,
}

impl Registration {
    pub const fn new<'a, SC>(name: &'static str) -> Self
    where
        SC: SinkConfig + serde::Deserialize<'a>,
    {
        let builder = |name: &str, value: toml::Value| {
            let config: SC = value.try_into().map_err(Error::Toml)?;
            config
                .build()
                .map_err(|e| Error::Config(e, name.to_string()))
        };

        Self { name, builder }
    }

    pub fn build(name: &str, value: toml::Value) -> Result<Box<dyn Sink>, Error> {
        let registrations: HashMap<&'static str, &Registration> = inventory::iter::<Registration>()
            .map(|r| (r.name, r))
            .collect();

        registrations
            .get(name)
            .ok_or(Error::Unknown(name.to_string()))
            .and_then(|r| {
                let builder = r.builder;

                builder(name, value)
            })
    }
}

inventory::collect!(Registration);
