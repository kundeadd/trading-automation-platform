//! Speed test for MEXC HTTP through proxy.
//! Запускати через: cargo run --release --bin speedtest

use arb::config::Config;
use arb::mexc::http::MexcClient;
use std::time::Instant;

type DynErr = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynErr> {
    let cfg = Config::load("config.toml")?;
    println!("Config loaded: account={}", cfg.account.label);

    let client = MexcClient::new(cfg.account.mexc_uid.clone(), &cfg.account.proxy_url)?;
    println!("MEXC client created");

    // Pre-warm: один запит щоб TCP+TLS handshake відбувся
    let t0 = Instant::now();
    let _ = client.get_balance().await?;
    println!("Pre-warm get_balance: {}ms", t0.elapsed().as_millis());

    println!("\n=== open_order × 10 (vol=1, не виконається але latency буде реальний) ===");
    let mut times: Vec<u128> = Vec::new();
    for i in 1..=10 {
        let t = Instant::now();
        let _ = client.open_order("TAO_USDT", 1, 1.0, 200).await;
        let dt = t.elapsed().as_millis();
        times.push(dt);
        println!("#{}: {}ms", i, dt);
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    times.sort();
    let median = times[times.len() / 2];
    println!("\nMin: {}ms, Median: {}ms, Max: {}ms", times[0], median, times[times.len()-1]);

    println!("\n=== fetch_depth × 10 (легший запит) ===");
    let mut times2: Vec<u128> = Vec::new();
    for i in 1..=10 {
        let t = Instant::now();
        let _ = client.fetch_depth("TAO_USDT", 5).await?;
        let dt = t.elapsed().as_millis();
        times2.push(dt);
        println!("#{}: {}ms", i, dt);
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
    }
    times2.sort();
    let median2 = times2[times2.len() / 2];
    println!("\nMin: {}ms, Median: {}ms, Max: {}ms", times2[0], median2, times2[times2.len()-1]);

    Ok(())
}
