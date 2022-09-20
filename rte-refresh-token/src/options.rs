use crate::kube;
use clap::{Parser, Subcommand};

#[derive(Parser, Debug)]
#[clap(author, version, about)]
pub struct Opts {
    /// The client_id generated by the Rte portal
    #[clap(long, value_parser)]
    pub client_id: String,

    /// The client secret generated by the Rte portal
    #[clap(long, value_parser)]
    pub client_secret: String,

    #[clap(subcommand)]
    pub output: OutputCommand,
}

impl Opts {
    pub fn parse_from_args() -> Self {
        Opts::parse()
    }
}

#[derive(Subcommand, Debug)]
pub enum OutputCommand {
    /// Dump the token to the console
    Console,

    /// Store the token as a kubernetes secret
    KubeSecret(kube::Opts),
}
