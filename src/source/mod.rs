use std::collections::HashMap;

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::point::Points;

pub mod rte;

#[derive(Error, Debug)]
pub enum Error {
    #[error("unknown source {0}")]
    Unknown(String),

    #[error("invalid configuration: {0}")]
    Toml(#[source] toml::de::Error),

    #[error("invalid configuration for {1}: {0}")]
    Config(#[source] Box<dyn std::error::Error>, String),
}

pub type DataSourceResult<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait DataSource {
    fn collect(&self) -> DataSourceResult<Points>;
}

pub trait DataSourceConfig: Send + Sync {
    fn build(self, global: GlobalConfig) -> DataSourceResult<Box<dyn DataSource>>;
}

#[derive(Copy, Clone, Serialize, Deserialize)]
pub struct GlobalConfig {
    pub from_date: NaiveDate,

    pub to_date: NaiveDate,
}

pub struct Registration {
    name: &'static str,

    builder: fn(&str, toml::Value, GlobalConfig) -> Result<Box<dyn DataSource>, Error>,
}

impl Registration {
    pub const fn new<'a, DSC>(name: &'static str) -> Self
    where
        DSC: DataSourceConfig + serde::Deserialize<'a>,
    {
        let builder = |name: &str, value: toml::Value, global: GlobalConfig| {
            let config: DSC = value.try_into().map_err(Error::Toml)?;
            config
                .build(global)
                .map_err(|e| Error::Config(e, name.to_string()))
        };

        Self { name, builder }
    }

    pub fn build(
        name: &str,
        value: toml::Value,
        global_config: GlobalConfig,
    ) -> Result<Box<dyn DataSource>, Error> {
        let registrations: HashMap<&'static str, &Registration> = inventory::iter::<Registration>()
            .map(|r| (r.name, r))
            .collect();

        registrations
            .get(name)
            .ok_or(Error::Unknown(name.to_string()))
            .and_then(|r| {
                let builder = r.builder;

                builder(name, value, global_config)
            })
    }
}

inventory::collect!(Registration);
