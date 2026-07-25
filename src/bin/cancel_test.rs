use arb::config::Config;
use arb::mexc::http::MexcClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::load("config.toml")?;
    let client = MexcClient::new(cfg.account.mexc_uid.clone(), &cfg.account.proxy_url)?;
    let resp = client.cancel_order("804467087991608960").await?;
    println!("{}", serde_json::to_string_pretty(&resp)?);
    Ok(())
}
