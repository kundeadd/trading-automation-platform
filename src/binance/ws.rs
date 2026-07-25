use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::json;
use tokio::net::TcpStream;
use tokio::time::{sleep, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{connect_async_tls_with_config, Connector, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};

use crate::config::Config;
use crate::state::{now_ms, PriceData, State};

const BINANCE_WS_URL: &str = "wss://fstream.binance.com/ws";
const RECONNECT_MIN_MS: u64 = 1_000;
const RECONNECT_MAX_MS: u64 = 30_000;
const STALE_MSG_WARN_MS: i64 = 200;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type BoxErr = Box<dyn std::error::Error + Send + Sync>;

/// bookTicker push: {"e":"bookTicker","u":...,"s":"TAOUSDT","b":"246.38","B":"123","a":"246.39","A":"456","T":...,"E":...}
#[derive(Debug, Deserialize)]
struct BookTicker {
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "b")]
    bid_price: String,
    #[serde(rename = "a")]
    ask_price: String,
    /// Transaction time
    #[serde(rename = "T", default)]
    transaction_time: i64,
}

/// Створює rustls-конектор з системними кореневими сертифікатами.
fn make_tls_connector() -> Result<Connector, BoxErr> {
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("load native certs") {
        root_store.add(cert).ok();
    }
    let tls_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    Ok(Connector::Rustls(std::sync::Arc::new(tls_config)))
}

async fn run_session(cfg: &Config, state: &Arc<State>) -> Result<(), BoxErr> {
    let connector = make_tls_connector()?;
    let req = BINANCE_WS_URL.into_client_request()?;

    let (mut ws, _resp) = connect_async_tls_with_config(req, None, false, Some(connector)).await?;
    info!(target: "binance_ws", "connected to {}", BINANCE_WS_URL);

    // Підписка на bookTicker для всіх символів. Binance вимагає lowercase у stream-name.
    let streams: Vec<String> = cfg
        .trading
        .symbols
        .iter()
        .map(|s| format!("{}@bookTicker", s.to_lowercase()))
        .collect();

    let sub_msg = json!({
        "method": "SUBSCRIBE",
        "params": streams,
        "id": 1
    });
    ws.send(Message::Text(sub_msg.to_string())).await?;
    debug!(target: "binance_ws", "subscribe sent: {:?}", cfg.trading.symbols);

    read_loop(&mut ws, state).await
}

async fn read_loop(ws: &mut WsStream, state: &Arc<State>) -> Result<(), BoxErr> {
    let mut last_msg = Instant::now();
    let mut idle_check = tokio::time::interval(Duration::from_secs(5));
    idle_check.tick().await;

    loop {
        tokio::select! {
            _ = idle_check.tick() => {
                if last_msg.elapsed() > Duration::from_secs(60) {
                    return Err("no messages for 60s, forcing reconnect".into());
                }
            }
            msg = ws.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(format!("ws recv: {}", e).into()),
                    None => return Err("ws stream closed".into()),
                };
                last_msg = Instant::now();
                match msg {
                    Message::Text(txt) => {
                        if let Err(e) = handle_text(&txt, state) {
                            warn!(target: "binance_ws", "handle: {} | {}", e, &txt[..txt.len().min(200)]);
                        }
                    }
                    Message::Ping(p) => {
                        // Binance шле WS-level ping кожні 3 хв — відповідаємо pong
                        ws.send(Message::Pong(p)).await.ok();
                        debug!(target: "binance_ws", "ws ping → pong");
                    }
                    Message::Close(f) => return Err(format!("server closed: {:?}", f).into()),
                    _ => {}
                }
            }
        }
    }
}

fn handle_text(txt: &str, state: &Arc<State>) -> Result<(), BoxErr> {
    // Subscribe ack: {"result":null,"id":1} — ігноруємо
    if txt.contains("\"result\":null") || txt.contains("\"id\":") && !txt.contains("\"e\":") {
        debug!(target: "binance_ws", "ack: {}", txt);
        return Ok(());
    }

    let bt: BookTicker = serde_json::from_str(txt)?;

    let bid: f64 = bt.bid_price.parse().map_err(|e| format!("bid parse: {}", e))?;
    let ask: f64 = bt.ask_price.parse().map_err(|e| format!("ask parse: {}", e))?;

    if bid <= 0.0 || ask <= 0.0 {
        return Ok(());
    }

    let ts = if bt.transaction_time > 0 { bt.transaction_time as u64 } else { now_ms() };

    if bt.transaction_time > 0 {
        let lag = now_ms() as i64 - bt.transaction_time;
        if lag > STALE_MSG_WARN_MS {
            debug!(target: "binance_ws", "lag {}ms for {}", lag, bt.symbol);
        }
    }

    state.binance_prices.insert(
        bt.symbol,
        PriceData { bid, ask, timestamp_ms: ts },
    );

    // Тригер для стратегії — миттєво реагувати на оновлення ціни.
    let _ = state.price_updates.send(());

    Ok(())
}

/// Вічний цикл reconnect для Binance bookTicker.
pub async fn run_binance_ws(cfg: Arc<Config>, state: Arc<State>) {
    let mut backoff_ms = RECONNECT_MIN_MS;
    loop {
        match run_session(&cfg, &state).await {
            Ok(()) => {
                warn!(target: "binance_ws", "session ended cleanly");
                backoff_ms = RECONNECT_MIN_MS;
            }
            Err(e) => {
                error!(target: "binance_ws", "session failed: {}", e);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
            }
        }
    }
}
