use serde::{Deserialize, Serialize};

use crate::{point::Points, sink::Registration};

use super::{Sink, SinkConfig};

struct Console;

impl Sink for Console {
    fn sink(&self, points: &Points) -> super::SinkResult<()> {
        println!("{}", serde_json::to_string_pretty(&points)?);
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Config {
    codec: String,
}

impl SinkConfig for Config {
    fn build(self) -> super::SinkResult<Box<dyn Sink>> {
        Ok(Box::new(Console))
    }
}

inventory::submit! {
    Registration::new::<Config>("console")
}
