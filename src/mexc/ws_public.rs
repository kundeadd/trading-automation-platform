use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::{to_mexc_symbol, Config};
use crate::state::{now_ms, OrderBook, PriceData, State};

const MEXC_WS_URL: &str = "wss://contract.mexc.com/edge";
const PING_INTERVAL_SECS: u64 = 15;
const RECONNECT_MIN_MS: u64 = 1_000;
const RECONNECT_MAX_MS: u64 = 30_000;
const STALE_MSG_WARN_MS: i64 = 500;
const PRICE_SCALE: f64 = 1e8;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type BoxErr = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Deserialize)]
struct DepthFullPush {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    ts: Option<i64>,
    #[serde(default)]
    data: Option<DepthFullData>,
}

#[derive(Debug, Deserialize)]
struct DepthFullData {
    #[serde(default)]
    asks: Vec<Vec<f64>>,
    #[serde(default)]
    bids: Vec<Vec<f64>>,
    #[serde(default)]
    version: u64,
}

#[inline]
fn scale_price(p: f64) -> i64 {
    (p * PRICE_SCALE).round() as i64
}

fn from_mexc_symbol(s: &str) -> String {
    s.replace('_', "")
}

async fn connect_via_proxy(cfg: &Config) -> Result<WsStream, BoxErr> {
    let url = Url::parse(MEXC_WS_URL)?;
    let host = url.host_str().ok_or("ws url has no host")?.to_string();
    let port = url.port_or_known_default().unwrap_or(443);

    let proxy = Url::parse(&cfg.account.proxy_url)?;
    let proxy_host = proxy.host_str().ok_or("proxy has no host")?.to_string();
    let proxy_port = proxy.port().ok_or("proxy has no port")?;
    let proxy_user = proxy.username().to_string();
    let proxy_pass = proxy.password().unwrap_or("").to_string();

    let mut tcp = TcpStream::connect((proxy_host.as_str(), proxy_port)).await?;
    tcp.set_nodelay(true)?;

    let auth = B64.encode(format!("{}:{}", proxy_user, proxy_pass));
    let req = format!(
        "CONNECT {host}:{port} HTTP/1.1\r\n\
         Host: {host}:{port}\r\n\
         Proxy-Authorization: Basic {auth}\r\n\
         \r\n"
    );
    tcp.write_all(req.as_bytes()).await?;

    let mut buf = Vec::with_capacity(512);
    let mut tmp = [0u8; 256];
    loop {
        let n = tcp.read(&mut tmp).await?;
        if n == 0 {
            return Err("proxy closed during CONNECT".into());
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 8192 {
            return Err("proxy CONNECT response too large".into());
        }
    }
    let resp = std::str::from_utf8(&buf).unwrap_or("");
    let first_line = resp.lines().next().unwrap_or("");
    if !first_line.starts_with("HTTP/1.1 200") && !first_line.starts_with("HTTP/1.0 200") {
        return Err(format!("proxy CONNECT failed: {}", first_line).into());
    }

    // Rustls TLS connector з системними кореневими сертифікатами
    let mut root_store = tokio_rustls::rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().expect("load native certs") {
        root_store.add(cert).ok();
    }
    let tls_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let connector = Connector::Rustls(std::sync::Arc::new(tls_config));

    let client_req = MEXC_WS_URL.into_client_request()?;
    let (ws, _resp) = tokio_tungstenite::client_async_tls_with_config(
        client_req,
        tcp,
        None,
        Some(connector),
    )
    .await?;

    Ok(ws)
}

#[inline]
fn update_top_cache(book: &OrderBook, sym_unified: &str, state: &Arc<State>) {
    let bid = book.best_bid().map(|(p, _)| p).unwrap_or(0.0);
    let ask = book.best_ask().map(|(p, _)| p).unwrap_or(0.0);
    if bid > 0.0 && ask > 0.0 {
        state.mexc_prices.insert(
            sym_unified.to_string(),
            PriceData { bid, ask, timestamp_ms: book.last_update_ms },
        );
        // Тригер для стратегії — миттєво реагувати на оновлення ціни.
        // .send() поверне Err якщо немає підписників, нам це байдуже.
        let _ = state.price_updates.send(());
    }
}

async fn run_session(cfg: &Config, state: &Arc<State>) -> Result<(), BoxErr> {
    let mut ws = connect_via_proxy(cfg).await?;
    info!(target: "mexc_ws", "connected to {}", MEXC_WS_URL);

    for sym in &cfg.trading.symbols {
        let mexc_sym = to_mexc_symbol(sym);
        let sub = json!({"method":"sub.depth.full","param":{"symbol": mexc_sym}});
        ws.send(Message::Text(sub.to_string())).await?;
        debug!(target: "mexc_ws", "sub.depth.full {}", mexc_sym);
    }

    let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
    let ping_handle = tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(PING_INTERVAL_SECS));
        tick.tick().await;
        loop {
            tick.tick().await;
            if ping_tx.send(()).await.is_err() {
                break;
            }
        }
    });

    let result = read_loop(&mut ws, state, &mut ping_rx).await;
    ping_handle.abort();
    let _ = ws.send(Message::Close(None)).await;
    result
}

async fn read_loop(
    ws: &mut WsStream,
    state: &Arc<State>,
    ping_rx: &mut mpsc::Receiver<()>,
) -> Result<(), BoxErr> {
    let mut last_msg = Instant::now();
    let mut idle_check = interval(Duration::from_secs(5));
    idle_check.tick().await;

    loop {
        tokio::select! {
            _ = ping_rx.recv() => {
                let p = json!({"method":"ping"}).to_string();
                ws.send(Message::Text(p)).await?;
                debug!(target: "mexc_ws", "ping sent");
            }
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
                            warn!(target: "mexc_ws", "handle: {} | {}", e, &txt[..txt.len().min(200)]);
                        }
                    }
                    Message::Ping(p) => { ws.send(Message::Pong(p)).await.ok(); }
                    Message::Close(f) => return Err(format!("server closed: {:?}", f).into()),
                    _ => {}
                }
            }
        }
    }
}

fn handle_text(txt: &str, state: &Arc<State>) -> Result<(), BoxErr> {
    let v: Value = serde_json::from_str(txt)?;
    let channel = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");

    match channel {
        "pong" => { debug!(target: "mexc_ws", "pong"); return Ok(()); }
        "rs.sub.depth.full" => { debug!(target: "mexc_ws", "sub ack: {}", txt); return Ok(()); }
        "rs.error" => { warn!(target: "mexc_ws", "server error: {}", txt); return Ok(()); }
        "push.depth.full" => {}
        "" => return Ok(()),
        other => { debug!(target: "mexc_ws", "ignored: {}", other); return Ok(()); }
    }

    let push: DepthFullPush = serde_json::from_value(v)?;
    let mexc_sym = push.symbol.ok_or("push.depth.full: no symbol")?;
    let data = push.data.ok_or("push.depth.full: no data")?;
    let ts = push.ts.unwrap_or(0);

    if ts > 0 {
        let lag = now_ms() as i64 - ts;
        if lag > STALE_MSG_WARN_MS {
            warn!(target: "mexc_ws", "depth lag {}ms for {}", lag, mexc_sym);
        }
    }

    let key = from_mexc_symbol(&mexc_sym);

    if data.bids.is_empty() || data.asks.is_empty() {
        debug!(target: "mexc_ws", "empty side for {}, skip", mexc_sym);
        return Ok(());
    }

    let entry = state.mexc_books.entry(key.clone()).or_default();
    let mut book = entry.write();

    book.bids.clear();
    book.asks.clear();

    for row in data.bids {
        if row.len() >= 2 {
            let price = row[0];
            let qty = row[1];
            if price > 0.0 && qty > 0.0 {
                book.bids.insert(scale_price(price), qty);
            }
        }
    }
    for row in data.asks {
        if row.len() >= 2 {
            let price = row[0];
            let qty = row[1];
            if price > 0.0 && qty > 0.0 {
                book.asks.insert(scale_price(price), qty);
            }
        }
    }

    book.version = data.version;
    book.last_update_ms = if ts > 0 { ts as u64 } else { now_ms() };
    update_top_cache(&book, &key, state);

    Ok(())
}

pub async fn run_mexc_public_ws(cfg: Arc<Config>, state: Arc<State>) {
    let mut backoff_ms = RECONNECT_MIN_MS;
    loop {
        match run_session(&cfg, &state).await {
            Ok(()) => {
                warn!(target: "mexc_ws", "session ended cleanly");
                backoff_ms = RECONNECT_MIN_MS;
            }
            Err(e) => {
                error!(target: "mexc_ws", "session failed: {}", e);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
            }
        }
    }
}
