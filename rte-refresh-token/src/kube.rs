use crate::cmd;
use clap::Args;
use k8s_openapi::{api::core::v1::Secret, serde_json, ByteString, Metadata};
use kube::{
    api::{ObjectMeta, Patch, PatchParams},
    config::KubeConfigOptions,
    Api, Client, Config,
};
use thiserror::Error;
use tracing::info;

const FIELD_MANAGER: &'static str = "photon/rte-refresh-token";
const DEFAULT_NAMESPACE: &'static str = "default";

#[derive(Error, Debug)]
enum Error {
    #[error("invalid secret {0}")]
    InvalidSecret(String),

    #[error("key '{1}' not found when attempting to patch secret {0}")]
    SecretKeyNotFound(String, String),
}

#[derive(Args, Debug)]
pub struct Opts {
    /// The name of the secret to generate
    #[clap(value_parser)]
    pub secret_name: String,

    /// The key of the secret in which to store the token
    #[clap(long, value_parser)]
    pub secret_key: String,

    /// The kubernetes namespace in which to store the secret
    #[clap(long, value_parser)]
    pub namespace: Option<String>,

    /// The name of the kubecontext context to use
    #[clap(long, value_parser)]
    context: Option<String>,

    /// The name of the kubecontext cluster to use
    #[clap(long, value_parser)]
    cluster: Option<String>,
}

pub async fn exec(token: String, opts: Opts) -> cmd::Result<()> {
    if let Ok(client) = Client::try_default().await {
        exec_with(client, token, opts).await
    } else {
        let kube_config = KubeConfigOptions {
            context: opts.context.clone(),
            cluster: opts.cluster.clone(),
            user: None,
        };

        let config = Config::from_kubeconfig(&kube_config).await?;
        exec_with(Client::try_from(config)?, token, opts).await
    }
}

async fn exec_with(client: Client, token: String, opts: Opts) -> cmd::Result<()> {
    let secrets = Api::<Secret>::namespaced(
        client,
        opts.namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE),
    );

    let _secret = match secrets.get_opt(&opts.secret_name).await? {
        Some(secret) => patch_secret(secrets, secret, opts.secret_key, token).await,
        None => create_secret(secrets, opts.secret_name, opts.secret_key, token).await,
    };

    Ok(())
}

async fn patch_secret(
    secrets: Api<Secret>,
    mut secret: Secret,
    secret_key: String,
    token: String,
) -> cmd::Result<Secret> {
    let secret_name = secret
        .metadata()
        .name
        .as_ref()
        .expect("invalid secret")
        .clone();

    let data = secret
        .data
        .as_mut()
        .ok_or(Error::InvalidSecret(secret_name.clone()))?;
    let key = data.get_mut(&secret_key).ok_or(Error::SecretKeyNotFound(
        secret_name.clone(),
        secret_key.clone(),
    ))?;
    *key = ByteString(base64::encode(token).into_bytes());
    info!(name = secret_name, key = secret_key, "patching secret");

    secrets
        .patch(
            &secret_name,
            &PatchParams::apply(FIELD_MANAGER),
            &Patch::Merge(Secret {
                metadata: ObjectMeta {
                    name: Some(secret_name.clone()),
                    ..ObjectMeta::default()
                },
                data: Some(data.clone()),
                ..Secret::default()
            }),
        )
        .await
        .map_err(Into::into)
}

async fn create_secret(
    secrets: Api<Secret>,
    secret_name: String,
    secret_key: String,
    token: String,
) -> cmd::Result<Secret> {
    info!(name = secret_name, key = secret_key, "creating secret");

    let params = kube::api::PostParams {
        dry_run: false,
        field_manager: Some(FIELD_MANAGER.to_string()),
    };

    let json = serde_json::json!({
        "kind": "Secret",
        "apiVersion": "v1",
        "metadata": {
            "name": secret_name
        },
        "data": {
            secret_key: base64::encode(token)
        },
        "type": "Opaque"
    });
    let data: Secret = serde_json::from_value(json).expect("invalid json");
    secrets.create(&params, &data).await.map_err(Into::into)
}
