use crate::{
    sink,
    source::{self, GlobalConfig},
    topology::{Component, Topology},
};
use std::{collections::HashMap, path::Path};

use chrono::NaiveDate;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("error reading file: {0}")]
    ReadFile(#[source] std::io::Error),

    #[error("invalid date format: {0}")]
    DateFormat(String),

    #[error("invalid toml format: {0}")]
    Toml(#[source] toml::de::Error),

    #[error("invalid data source {1}: {0}")]
    Source(#[source] source::Error, String),

    #[error("invalid sink {1}: {0}")]
    Sink(#[source] sink::Error, String),
}

#[derive(Serialize, Deserialize, Debug)]
struct ConfigRaw {
    from_date: String,

    to_date: String,

    sources: HashMap<String, toml::Value>,

    sinks: HashMap<String, toml::Value>,
}

impl ConfigRaw {
    fn to_config(self) -> Result<Config, Error> {
        Ok(Config {
            from_date: Self::parse_date(&self.from_date, "from_date")?,
            to_date: Self::parse_date(&self.to_date, "to_date")?,
            sources: self.sources,
            sinks: self.sinks,
        })
    }

    fn parse_date(s: &str, name: &str) -> Result<NaiveDate, Error> {
        let lower = s.to_lowercase();

        match lower.as_str() {
            "today" => {
                let now = chrono::Utc::now();
                Ok(now.naive_local().date())
            }
            "yesterday" => {
                let now = chrono::Utc::now();
                let today = now.naive_local().date();
                Ok(today.pred())
            }
            _ => s
                .parse::<NaiveDate>()
                .map_err(|_| Error::DateFormat(name.to_string())),
        }
    }
}

#[derive(Debug)]
struct Config {
    from_date: NaiveDate,

    to_date: NaiveDate,

    sources: HashMap<String, toml::Value>,

    sinks: HashMap<String, toml::Value>,
}

impl Config {
    fn global(&self) -> GlobalConfig {
        GlobalConfig {
            from_date: self.from_date,

            to_date: self.to_date,
        }
    }
}

pub fn read(file: impl AsRef<Path>) -> Result<Topology, Error> {
    let content = std::fs::read_to_string(file).map_err(Error::ReadFile)?;
    let config_raw: ConfigRaw = toml::from_str(&content).map_err(Error::Toml)?;
    let config = config_raw.to_config()?;
    let global_config = config.global();

    let data_sources = config
        .sources
        .into_iter()
        .map(|(k, v)| {
            let component = source::Registration::build(&k, v, global_config)
                .map_err(|e| Error::Source(e, k.clone()))?;

            Ok(Component {
                name: k.clone(),
                component,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let sinks = config
        .sinks
        .into_iter()
        .map(|(k, v)| {
            let component =
                sink::Registration::build(&k, v).map_err(|e| Error::Sink(e, k.clone()))?;

            Ok(Component {
                name: k.clone(),

                component,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Topology {
        data_sources,
        sinks,
    })
}
