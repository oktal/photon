use crate::options::Opts;
mod cmd;
mod console;
mod kube;
mod options;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    cmd::run(Opts::parse_from_args()).await
}
