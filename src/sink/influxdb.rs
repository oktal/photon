use chrono::{DateTime, Utc};
use reqwest::{StatusCode, Url};
use rinfluxdb::line_protocol::{FieldValue, Line, LineBuilder};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::debug;

use crate::{
    point::{Point, Points, Value},
    sink::Registration,
};

use super::{Sink, SinkConfig, SinkResult};

#[derive(Error, Debug)]
enum Error {
    #[error("failed to send request")]
    Request(#[source] reqwest::Error),

    #[error("write request resulted in a non-success status code {0} with error: {1}")]
    Write(StatusCode, String),
}

impl Into<FieldValue> for Value {
    fn into(self) -> FieldValue {
        match self {
            Value::Integer(i) => FieldValue::Integer(i),
            Value::Float(f) => FieldValue::Float(f),
            Value::Boolean(b) => FieldValue::Boolean(b),
            Value::String(s) => FieldValue::String(s),
        }
    }
}

fn line(point: &Point, timestamp: DateTime<Utc>) -> Line {
    let mut builder = LineBuilder::new(point.name.clone());

    for (k, v) in &point.tags {
        builder = builder.insert_tag(k.clone(), v.clone());
    }

    for (k, v) in &point.fields {
        builder = builder.insert_field(k.clone(), v.clone());
    }

    builder
        .set_timestamp(point.timestamp.unwrap_or(timestamp))
        .build()
}

struct InfluxDB {
    host: Url,

    token: String,

    org: String,

    bucket: String,
}

impl Sink for InfluxDB {
    fn sink(&self, points: &Points) -> SinkResult<()> {
        let utc_now = Utc::now();

        let lines: Vec<Line> = points.iter().map(|p| line(p, utc_now)).collect();

        debug!("sending {} points", lines.len());

        let client = reqwest::blocking::Client::new();
        let response = client
            .post(self.host.clone())
            .bearer_auth(self.token.clone())
            .query(&[("org", &self.org), ("bucket", &self.bucket)])
            .send()
            .map_err(Error::Request)?;

        if !response.status().is_success() {
            return Err(Error::Write(
                response.status(),
                response
                    .text()
                    .unwrap_or("Failed to retrieve response text".to_string()),
            )
            .into());
        }

        Ok(())
    }
}

#[derive(Deserialize, Serialize, Debug)]
struct Config {
    host: String,

    token: String,

    org: String,

    bucket: String,
}

impl SinkConfig for Config {
    fn build(self) -> SinkResult<Box<dyn Sink>> {
        Ok(Box::new(InfluxDB {
            host: self.host.parse()?,

            token: self.token,

            org: self.org,

            bucket: self.bucket,
        }))
    }
}

inventory::submit! {
    Registration::new::<Config>("influxdb")
}
