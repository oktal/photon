use chrono::{prelude::*, Duration};
use chrono_tz::{Europe::Paris, Tz};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    point::{self, Points, Value},
    source::{DataSource, DataSourceConfig, DataSourceResult, GlobalConfig, Registration},
    value,
};

const ECOWATT_URL: &'static str =
    "https://digital.iservices.rte-france.com/open_api/ecowatt/v4/signals";
const ECOWATT_SANDBOX_URL: &'static str =
    "https://digital.iservices.rte-france.com/open_api/ecowatt/v4/sandbox/signals";

#[derive(Error, Debug)]
enum Error {
    #[error("no EcoWatt signal returned from RTE")]
    NoSignal,
}

#[derive(Deserialize, Debug)]
struct EcoWattValue {
    #[serde(rename = "pas")]
    hour: u32,

    #[serde(rename = "hvalue")]
    value: u32,
}

#[derive(Deserialize, Debug)]
struct EcoWattSignal {
    #[serde(rename = "jour")]
    day: DateTime<Utc>,

    #[serde(rename = "dvalue")]
    day_value: u32,

    message: String,

    values: Vec<EcoWattValue>,
}

#[derive(Deserialize, Debug)]
struct EcowattResponse {
    signals: Vec<EcoWattSignal>,
}

struct EcoWatt {
    global: GlobalConfig,

    token: String,

    url: String,
}

#[derive(Serialize, Deserialize)]
struct Config {
    token: String,

    sandbox: Option<bool>,
}

impl DataSourceConfig for Config {
    fn build(self, global: GlobalConfig) -> DataSourceResult<Box<dyn DataSource>> {
        let url = match self.sandbox {
            Some(true) => ECOWATT_SANDBOX_URL,
            _ => ECOWATT_URL,
        }
        .to_string();

        Ok(Box::new(EcoWatt {
            global,
            token: self.token,
            url,
        }))
    }
}

impl DataSource for EcoWatt {
    fn collect(&self) -> DataSourceResult<point::Points> {
        let response = reqwest::blocking::Client::builder()
            .build()?
            .get(&self.url)
            .bearer_auth(self.token.clone())
            .send()?
            .error_for_status()?
            .json::<EcowattResponse>()?;

        let today_signal = response.signals.get(0).ok_or(Error::NoSignal)?;
        let mut points = point::Points::new();

        points.add(
            point::Point::builder("ecowatt_signal")
                .field("value", value!(today_signal.day_value))
                .timestamp(today_signal.day)
                .build(),
        );

        for hourly in &today_signal.values {
            let ts = today_signal.day + Duration::hours(hourly.hour as i64);

            points.add(
                point::Point::builder("ecowatt_signal")
                    .field("value", value!(hourly.value))
                    .timestamp(ts)
                    .build(),
            );
        }

        Ok(points)
    }
}

inventory::submit! {
    Registration::new::<Config>("rte-ecowatt")
}
