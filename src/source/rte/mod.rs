use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
    str::FromStr,
};

use chrono::prelude::*;
use chrono_tz::{Europe::Paris, Tz};
use csv::ByteRecord;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tracing::{field, info};

use crate::{
    point::{Point, Points, Value},
    value,
};

use super::{DataSource, DataSourceConfig, DataSourceResult, GlobalConfig, Registration};

const END_RECORD: &'static [u8] = b"RTE ne pourra";
const ECO2MIX_DATA_URL: &str = "https://eco2mix.rte-france.com/curves/eco2mixDl";

struct DaysIterator(NaiveDate, NaiveDate);

impl Iterator for DaysIterator {
    type Item = NaiveDate;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0 <= self.1 {
            let curr = self.0;
            let next = curr.succ();

            self.0 = next;
            Some(curr)
        }
        else {
            None
        }
    }
}

fn iter_days(from: NaiveDate, to: NaiveDate) -> DaysIterator {
    DaysIterator(from, to)
}

#[derive(Error, Debug)]
enum DownloadError {
    #[error("error creating file {1}: {0}")]
    CreateFile(#[source] std::io::Error, PathBuf),

    #[error("http request error: {0}")]
    Http(#[source] reqwest::Error),

    #[error("io error: {0}")]
    Io(#[source] reqwest::Error),
}

#[derive(Error, Debug)]
enum ExtractionError {
    #[error("error opening file: {0}")]
    OpenFile(#[source] std::io::Error),

    #[error("error creating file {1}: {0}")]
    CreateFile(#[source] std::io::Error, PathBuf),

    #[error("zip error: {0}")]
    Zip(#[source] zip::result::ZipError),

    #[error("could not find file in archive")]
    FileNotFound,

    #[error("error extracing data: {0}")]
    Copy(#[source] std::io::Error),
}

#[derive(Error, Debug)]
enum DataError {
    #[error("invalid scope {0} (expected France)")]
    InvalidScope(String),

    #[error("missing field {0}")]
    MissingField(String),

    #[error("invalid date format: {0}")]
    Date(#[source] chrono::ParseError),

    #[error("error parsing field {1}: {0}")]
    Parse(#[source] Box<dyn std::error::Error>, String),
}

#[derive(Error, Debug)]
enum Error {
    #[error("error downloading data: {0}")]
    Download(#[source] DownloadError),

    #[error("error extracing data: {0}")]
    Extraction(#[source] ExtractionError),

    #[error("error processing data: {0}")]
    Data(#[source] DataError),
}

#[derive(Debug)]
struct DailyRow {
    date: DateTime<Tz>,
    generation_total: u64,
    prediction_yesterday: u64,
    prediction_now: u64,
    oil: u64,
    coal: u64,
    gas: u64,
    nuclear: u64,
    wind: u64,
    solar: u64,
    hydro: u64,
    pumped_storage: i64,
    bioenergy: u64,
    co2: u64,
}

impl DailyRow {
    fn from_record(record: ByteRecord) -> Result<DailyRow, DataError> {
        let scope = record
            .get(0)
            .ok_or(DataError::MissingField("Perimetre".to_string()))?;

        if scope != b"France" {
            return Err(DataError::InvalidScope(
                String::from_utf8_lossy(scope).to_string(),
            ));
        }

        let date = record
            .get(2)
            .ok_or(DataError::MissingField("Date".to_string()))
            .map(String::from_utf8_lossy)
            .and_then(|s| NaiveDate::parse_from_str(&s, "%Y-%m-%d").map_err(DataError::Date))?;

        let time = record
            .get(3)
            .ok_or(DataError::MissingField("Time".to_string()))
            .map(String::from_utf8_lossy)
            .and_then(|s| NaiveTime::parse_from_str(&s, "%H:%M").map_err(DataError::Date))?;

        let dt = Paris.ymd(date.year(), date.month(), date.day()).and_hms(
            time.hour(),
            time.minute(),
            time.second(),
        );

        Ok(DailyRow {
            date: dt,
            generation_total: DailyRow::get_field(&record, 4, "Generation")?,
            prediction_yesterday: DailyRow::get_field(&record, 5, "Prediction D-1")?,
            prediction_now: DailyRow::get_field(&record, 6, "Prediction")?,
            oil: DailyRow::get_field(&record, 7, "Oil")?,
            coal: DailyRow::get_field(&record, 8, "Coal")?,
            gas: DailyRow::get_field(&record, 9, "Gas")?,
            nuclear: DailyRow::get_field(&record, 10, "Nuclear")?,
            wind: DailyRow::get_field(&record, 11, "Wind")?,
            solar: DailyRow::get_field(&record, 12, "Solar")?,
            hydro: DailyRow::get_field(&record, 13, "Hydro")?,
            pumped_storage: DailyRow::get_field(&record, 14, "Pumped storage")?,
            bioenergy: DailyRow::get_field(&record, 15, "Bioenergy")?,
            co2: DailyRow::get_field(&record, 17, "CO2")?,
        })
    }

    fn get_field<S: FromStr>(record: &ByteRecord, index: usize, name: &str) -> Result<S, DataError>
    where
        S::Err: std::error::Error + 'static,
    {
        record
            .get(index)
            .ok_or(DataError::MissingField(name.into()))
            .map(String::from_utf8_lossy)
            .and_then(|s| S::from_str(&s).map_err(|e| DataError::Parse(e.into(), name.to_string())))
    }
}

fn format_url(date: NaiveDate) -> String {
    format!("{ECO2MIX_DATA_URL}?date={}", date.format("%d/%m/%Y"))
}

fn download(date: NaiveDate, folder: &Path) -> Result<PathBuf, DownloadError> {
    let url = format_url(date);

    let mut file_path = folder.to_path_buf();
    file_path.push(format!("eco2mix-{}.zip", date.format("%Y-%m-%d")));

    info!(
        url = url,
        path = field::display(file_path.display()),
        "downloading data file"
    );

    let mut file =
        File::create(&file_path).map_err(|e| DownloadError::CreateFile(e, file_path.clone()))?;
    let mut response = reqwest::blocking::get(url).map_err(DownloadError::Http)?;
    response.copy_to(&mut file).map_err(DownloadError::Io)?;

    Ok(file_path)
}

fn extract(path: impl AsRef<Path>) -> Result<PathBuf, ExtractionError> {
    let file = std::fs::File::open(path).map_err(ExtractionError::OpenFile)?;
    let mut archive = zip::ZipArchive::new(file).map_err(ExtractionError::Zip)?;
    let mut file = archive.by_index(0).map_err(ExtractionError::Zip)?;
    let out_path = file
        .enclosed_name()
        .ok_or(ExtractionError::FileNotFound)?
        .to_owned();

    info!(
        out_path = field::display(out_path.display()),
        "extracting file"
    );

    let mut out_file = std::fs::File::create(&out_path)
        .map_err(|e| ExtractionError::CreateFile(e, out_path.clone()))?;
    io::copy(&mut file, &mut out_file).map_err(ExtractionError::Copy)?;

    Ok(out_path)
}

fn read(path: impl AsRef<Path>) -> Result<Vec<DailyRow>, DataError> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b'\t')
        .flexible(true)
        .from_path(path)
        .unwrap();

    let mut rows = Vec::new();

    for record in reader.byte_records() {
        let record = record.unwrap();

        if is_end_record(&record) {
            break;
        }

        rows.push(DailyRow::from_record(record)?);
    }

    Ok(rows)
}

impl From<DailyRow> for Point {
    fn from(line: DailyRow) -> Self {
        Point::builder("eco2mix")
            .field("generation_total", value!(line.generation_total))
            .field("oil", value!(line.oil))
            .field("coal", value!(line.coal))
            .field("gas", value!(line.gas))
            .field("nuclear", value!(line.nuclear))
            .field("wind", value!(line.wind))
            .field("solar", value!(line.solar))
            .field("hydro", value!(line.hydro))
            .field("pumped_storage", value!(line.pumped_storage))
            .field("bioenergy", value!(line.bioenergy))
            .field("co2", value!(line.co2))
            .timestamp(line.date.with_timezone(&Utc))
            .build()
    }
}

impl From<Vec<DailyRow>> for Points {
    fn from(lines: Vec<DailyRow>) -> Self {
        Points::from_iter(lines.into_iter().map(Point::from))
    }
}

fn is_end_record(record: &ByteRecord) -> bool {
    record
        .get(0)
        .map(|t| {
            t.get(..END_RECORD.len())
                .map(|e| e == END_RECORD)
                .unwrap_or(false)
        })
        .unwrap_or(false)
}

fn collect(
    global_config: &GlobalConfig,
    download_folder: impl AsRef<Path>,
) -> Result<Points, Error> {
    let mut points = Points::new();

    for date in iter_days(global_config.from_date, global_config.to_date) {
        info!("collecting date for {date}");

        let day_points: Points = download(date, download_folder.as_ref())
            .map_err(Error::Download)
            .and_then(|file_path| extract(file_path).map_err(Error::Extraction))
            .and_then(|file_path| read(file_path).map_err(Error::Data))
            .map(|lines| lines.into())?;

        points.merge_with(day_points);
    }

    Ok(points)
}

struct Rte {
    global: GlobalConfig,

    download_folder: Option<String>,
}

#[derive(Serialize, Deserialize)]
struct Config {
    download_folder: Option<String>,
}

impl DataSourceConfig for Config {
    fn build(self, global: GlobalConfig) -> DataSourceResult<Box<dyn DataSource>> {
        Ok(Box::new(Rte {
            global,
            download_folder: self.download_folder,
        }))
    }
}

impl DataSource for Rte {
    fn collect(&self) -> DataSourceResult<Points> {
        let download_folder = self
            .download_folder
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or(std::env::temp_dir());

        collect(&self.global, download_folder).map_err(Into::into)
    }
}

inventory::submit! {
    Registration::new::<Config>("rte")
}

#[cfg(test)]
mod test {
    use super::iter_days;
    use chrono::NaiveDate;

    fn assert_day_range(from: NaiveDate, to: NaiveDate, expected: Vec<(NaiveDate, NaiveDate)>) {
        assert_eq!(iter_days(from, to).collect::<Vec<_>>(), expected);
    }


    #[test]
    fn test_day_range() {
        assert_day_range(NaiveDate::from_ymd(2022, 5, 1), NaiveDate::from_ymd(2022, 5, 2), vec![
            (NaiveDate::from_ymd(2022, 5, 1), NaiveDate::from_ymd(2022, 5, 2)),
        ]);

        assert_day_range(NaiveDate::from_ymd(2022, 5, 1), NaiveDate::from_ymd(2022, 5, 4), vec![
            (NaiveDate::from_ymd(2022, 5, 1), NaiveDate::from_ymd(2022, 5, 2)),
            (NaiveDate::from_ymd(2022, 5, 2), NaiveDate::from_ymd(2022, 5, 3)),
            (NaiveDate::from_ymd(2022, 5, 3), NaiveDate::from_ymd(2022, 5, 4)),
        ]);
    }
}
