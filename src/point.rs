use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[macro_export]
macro_rules! value {
    ($k:expr) => {
        Value::from($k)
    };
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Value {
    Integer(i64),

    Float(f64),

    Boolean(bool),

    String(String),
}

impl From<i8> for Value {
    fn from(val: i8) -> Self {
        Self::Integer(val as i64)
    }
}

impl From<i16> for Value {
    fn from(val: i16) -> Self {
        Self::Integer(val as i64)
    }
}

impl From<i32> for Value {
    fn from(val: i32) -> Self {
        Self::Integer(val as i64)
    }
}

impl From<i64> for Value {
    fn from(val: i64) -> Self {
        Self::Integer(val)
    }
}

impl From<u8> for Value {
    fn from(val: u8) -> Self {
        Self::Integer(val as i64)
    }
}

impl From<u16> for Value {
    fn from(val: u16) -> Self {
        Self::Integer(val as i64)
    }
}

impl From<u32> for Value {
    fn from(val: u32) -> Self {
        Self::Integer(val as i64)
    }
}

impl From<u64> for Value {
    fn from(val: u64) -> Self {
        Self::Integer(val as i64)
    }
}

impl From<f64> for Value {
    fn from(val: f64) -> Self {
        Self::Float(val)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Point {
    pub name: String,

    pub tags: HashMap<String, String>,

    pub fields: HashMap<String, Value>,

    pub timestamp: Option<DateTime<Utc>>,
}

pub struct PointBuilder(Point);

impl PointBuilder {
    pub fn tag(mut self, key: String, value: String) -> Self {
        self.0.tags.insert(key, value);
        self
    }

    pub fn field(mut self, key: impl AsRef<str>, value: Value) -> Self {
        self.0.fields.insert(key.as_ref().to_string(), value);
        self
    }

    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.0.timestamp = Some(ts);
        self
    }

    pub fn build(self) -> Point {
        self.0
    }
}

impl Point {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),

            tags: HashMap::new(),

            fields: HashMap::new(),

            timestamp: None,
        }
    }

    pub fn builder(name: impl Into<String>) -> PointBuilder {
        PointBuilder(Self::new(name))
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Points(Vec<Point>);

impl From<Vec<Point>> for Points {
    fn from(points: Vec<Point>) -> Self {
        Self(points)
    }
}

impl FromIterator<Point> for Points {
    fn from_iter<T: IntoIterator<Item = Point>>(iter: T) -> Self {
        Points(Vec::from_iter(iter))
    }
}

impl IntoIterator for Points {
    type Item = Point;

    type IntoIter = <Vec<Point> as IntoIterator>::IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl Points {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn with_capacity(size: usize) -> Self {
        Self(Vec::with_capacity(size))
    }

    pub fn add(&mut self, p: Point) {
        self.0.push(p)
    }

    pub fn tag_all(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) {
        for point in &mut self.0 {
            point
                .tags
                .insert(key.as_ref().to_string(), value.as_ref().to_string());
        }
    }

    pub fn merge_with(&mut self, mut other: Points) {
        self.0.append(&mut other.0);
    }

    pub fn iter(&self) -> impl Iterator<Item = &Point> {
        self.0.iter()
    }
}
