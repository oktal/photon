use crate::{
    point::Points,
    sink::Sink,
    source::{DataSource, DataSourceResult},
};

pub struct Component<T: ?Sized> {
    pub name: String,

    pub component: Box<T>,
}

pub struct Topology {
    pub data_sources: Vec<Component<dyn DataSource>>,

    pub sinks: Vec<Component<dyn Sink>>,
}

fn collect(name: impl AsRef<str>, data_source: Box<dyn DataSource>) -> DataSourceResult<Points> {
    let mut points = data_source.collect()?;
    points.tag_all("source", name);
    Ok(points)
}

pub fn run(topology: Topology) -> Result<(), Box<dyn std::error::Error>> {
    let mut points = Points::new();

    for data_source in topology.data_sources {
        points.merge_with(collect(&data_source.name, data_source.component)?);
    }

    for sink in topology.sinks {
        sink.component.sink(&points)?;
    }

    Ok(())
}
