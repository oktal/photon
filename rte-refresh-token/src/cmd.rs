use reqwest::header::AUTHORIZATION;
use serde::Deserialize;

use crate::{
    console, kube,
    options::{Opts, OutputCommand},
};

const AUTH_ENDPOINT: &'static str = "https://digital.iservices.rte-france.com/token/oauth/";

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

#[derive(Deserialize)]
struct AuthResponse {
    access_token: String,
    token_type: String,
    expires_in: u64,
}

pub async fn run(options: Opts) -> Result<()> {
    let client_id = options.client_id;
    let client_secret = options.client_secret;

    let auth_info = format!("{client_id}:{client_secret}");
    let auth = format!("Basic {}", base64::encode(auth_info));
    let client = reqwest::Client::new();
    let resp = client
        .post(AUTH_ENDPOINT)
        .header(AUTHORIZATION, auth)
        .send()
        .await?;

    let auth_response: AuthResponse = resp.json().await?;
    exec(options.output, auth_response.access_token).await
}

async fn exec(command: OutputCommand, token: String) -> Result<()> {
    match command {
        OutputCommand::Console => console::exec(token),
        OutputCommand::KubeSecret(opts) => kube::exec(token, opts).await,
    }
}
