use main_error::MainResult;
mod config;
mod point;
mod sink;
mod source;
mod topology;

fn main() -> MainResult {
    tracing_subscriber::fmt::init();

    let config_file = std::env::args()
        .skip(1)
        .next()
        .expect("usage crawler config_file.toml");
    topology::run(config::read(config_file)?)?;

    Ok(())
}
