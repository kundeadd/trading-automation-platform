use arb::config::Config;
use arb::mexc::http::MexcClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::load("config.toml")?;
    let client = MexcClient::new(cfg.account.mexc_uid.clone(), &cfg.account.proxy_url)?;

    println!("--- ASTER limit IOC test ---");
    let resp = client.open_order_limit("ASTER_USDT", 1, 10.0, 100, 0.6700).await?;
    println!("Response: {}", serde_json::to_string_pretty(&resp)?);

    Ok(())
}
