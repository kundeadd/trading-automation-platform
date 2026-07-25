use arb::config::Config;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use std::time::{Duration, Instant};
use tokio_tungstenite::tungstenite::protocol::Message;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;

const TOKENS: &[&str] = &["TAO", "ASTER", "PENGU", "ZEC", "SUI", "HYPE", "ENA"];
const SCAN_DURATION_SEC: u64 = 3600; // 1 година
const SPREAD_THRESHOLD: f64 = 0.10; // 0.10%

#[derive(Default, Clone)]
struct PriceState {
    binance_bid: f64,
    binance_ask: f64,
    mexc_bid: f64,
    mexc_ask: f64,
}

#[derive(Default)]
struct Stats {
    spread_count_010: u64,
    spread_count_015: u64,
    spread_count_020: u64,
    max_spread: f64,
    sum_spread: f64,
    sample_count: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cfg = Config::load("config.toml")?;
    let proxy_url = cfg.account.proxy_url.clone();

    let prices: Arc<RwLock<HashMap<String, PriceState>>> = Arc::new(RwLock::new(HashMap::new()));
    let stats: Arc<RwLock<HashMap<String, Stats>>> = Arc::new(RwLock::new(HashMap::new()));

    for &t in TOKENS {
        prices.write().insert(t.to_string(), PriceState::default());
        stats.write().insert(t.to_string(), Stats::default());
    }

    println!("=== SCAN: {} tokens for {}s ===", TOKENS.len(), SCAN_DURATION_SEC);
    println!("Tokens: {:?}", TOKENS);
    println!();

    // Запустити Binance WS — один stream для всіх
    {
        let prices = prices.clone();
        tokio::spawn(async move {
            let url = "wss://fstream.binance.com/stream?streams=".to_string()
                + &TOKENS.iter()
                    .map(|t| format!("{}usdt@bookTicker", t.to_lowercase()))
                    .collect::<Vec<_>>()
                    .join("/");
            loop {
                match tokio_tungstenite::connect_async(&url).await {
                    Ok((mut ws, _)) => {
                        println!("[binance] connected");
                        while let Some(Ok(msg)) = ws.next().await {
                            if let Message::Text(txt) = msg {
                                if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                                    if let Some(data) = v.get("data") {
                                        if let (Some(s), Some(b), Some(a)) = (
                                            data.get("s").and_then(|v| v.as_str()),
                                            data.get("b").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()),
                                            data.get("a").and_then(|v| v.as_str()).and_then(|v| v.parse::<f64>().ok()),
                                        ) {
                                            let token = s.trim_end_matches("USDT").to_string();
                                            if let Some(p) = prices.write().get_mut(&token) {
                                                p.binance_bid = b;
                                                p.binance_ask = a;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[binance] connect failed: {}", e);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
    }

    // MEXC WS (один на всі токени)
    {
        let prices = prices.clone();
        let proxy_url = proxy_url.clone();
        tokio::spawn(async move {
            let url = "wss://contract.mexc.com/edge";
            loop {
                let connect_result = if proxy_url.is_empty() {
                    tokio_tungstenite::connect_async(url).await.map(|(ws, _)| ws)
                } else {
                    // Proxy connect через rquest не доступний для tungstenite напряму.
                    // Для простоти — пробуємо без проксі (з VPS Tokyo MEXC WS працює).
                    tokio_tungstenite::connect_async(url).await.map(|(ws, _)| ws)
                };
                match connect_result {
                    Ok(mut ws) => {
                        println!("[mexc] connected");
                        // Subscribe на depth.full для всіх токенів
                        for t in TOKENS {
                            let sub = serde_json::json!({
                                "method": "sub.depth.full",
                                "param": {"symbol": format!("{}_USDT", t), "limit": 5}
                            });
                            let _ = ws.send(Message::Text(sub.to_string())).await;
                        }
                        // Ping loop
                        let mut last_ping = Instant::now();
                        while let Some(Ok(msg)) = ws.next().await {
                            if last_ping.elapsed() > Duration::from_secs(15) {
                                let _ = ws.send(Message::Text(r#"{"method":"ping"}"#.to_string())).await;
                                last_ping = Instant::now();
                            }
                            if let Message::Text(txt) = msg {
                                if let Ok(v) = serde_json::from_str::<Value>(&txt) {
                                    if v.get("channel").and_then(|c| c.as_str()) == Some("push.depth.full") {
                                        if let (Some(sym), Some(data)) = (
                                            v.get("symbol").and_then(|s| s.as_str()),
                                            v.get("data"),
                                        ) {
                                            let token = sym.trim_end_matches("_USDT").to_string();
                                            let bid = data.get("bids").and_then(|b| b.as_array())
                                                .and_then(|arr| arr.first())
                                                .and_then(|x| x.as_array())
                                                .and_then(|x| x.first())
                                                .and_then(|x| x.as_f64()).unwrap_or(0.0);
                                            let ask = data.get("asks").and_then(|a| a.as_array())
                                                .and_then(|arr| arr.first())
                                                .and_then(|x| x.as_array())
                                                .and_then(|x| x.first())
                                                .and_then(|x| x.as_f64()).unwrap_or(0.0);
                                            if let Some(p) = prices.write().get_mut(&token) {
                                                p.mexc_bid = bid;
                                                p.mexc_ask = ask;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("[mexc] connect failed: {}", e);
                        tokio::time::sleep(Duration::from_secs(2)).await;
                    }
                }
            }
        });
    }

    // Спред-моніторинг — кожну 200мс
    {
        let prices = prices.clone();
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(200));
            loop {
                interval.tick().await;
                let snapshot: Vec<(String, PriceState)> = prices.read().iter()
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect();
                for (token, p) in snapshot {
                    if p.binance_bid <= 0.0 || p.mexc_ask <= 0.0 || p.binance_ask <= 0.0 || p.mexc_bid <= 0.0 {
                        continue;
                    }
                    // LONG: купити на MEXC (ask), продати на Binance (bid)
                    let long_spread = (p.binance_bid - p.mexc_ask) / p.mexc_ask * 100.0;
                    // SHORT: продати на MEXC (bid), купити на Binance (ask)
                    let short_spread = (p.mexc_bid - p.binance_ask) / p.binance_ask * 100.0;
                    let max_spread = long_spread.max(short_spread);

                    if let Some(s) = stats.write().get_mut(&token) {
                        s.sample_count += 1;
                        s.sum_spread += max_spread.max(0.0);
                        if max_spread > s.max_spread {
                            s.max_spread = max_spread;
                        }
                        if max_spread >= 0.10 { s.spread_count_010 += 1; }
                        if max_spread >= 0.15 { s.spread_count_015 += 1; }
                        if max_spread >= 0.20 { s.spread_count_020 += 1; }
                    }
                }
            }
        });
    }

    // Періодичний звіт кожну хвилину
    let report_stats = stats.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(60));
        interval.tick().await;
        let mut minutes = 0;
        loop {
            interval.tick().await;
            minutes += 1;
            println!("\n=== {} min ===", minutes);
            let stats = report_stats.read();
            let mut rows: Vec<_> = stats.iter().collect();
            rows.sort_by(|a, b| b.1.spread_count_010.cmp(&a.1.spread_count_010));
            println!("{:<8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8}", "TOKEN", ">0.10%", ">0.15%", ">0.20%", "MAX", "AVG");
            for (token, s) in rows {
                let avg = if s.sample_count > 0 { s.sum_spread / s.sample_count as f64 } else { 0.0 };
                println!("{:<8} | {:>8} | {:>8} | {:>8} | {:>7.3}% | {:>7.4}%",
                    token, s.spread_count_010, s.spread_count_015, s.spread_count_020, s.max_spread, avg);
            }
        }
    });

    // Чекаємо завершення
    tokio::time::sleep(Duration::from_secs(SCAN_DURATION_SEC)).await;

    println!("\n=== FINAL REPORT ({} sec) ===", SCAN_DURATION_SEC);
    let stats = stats.read();
    let mut rows: Vec<_> = stats.iter().collect();
    rows.sort_by(|a, b| b.1.spread_count_010.cmp(&a.1.spread_count_010));
    println!("{:<8} | {:>8} | {:>8} | {:>8} | {:>8} | {:>8}", "TOKEN", ">0.10%", ">0.15%", ">0.20%", "MAX", "AVG");
    for (token, s) in rows {
        let avg = if s.sample_count > 0 { s.sum_spread / s.sample_count as f64 } else { 0.0 };
        println!("{:<8} | {:>8} | {:>8} | {:>8} | {:>7.3}% | {:>7.4}%",
            token, s.spread_count_010, s.spread_count_015, s.spread_count_020, s.max_spread, avg);
    }

    Ok(())
}
