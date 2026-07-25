//! MEXC private WS: login через u_id, отримує події про ордери/позиції/баланс.
//! Дає monitor реальний position_id (без REST) і точний PnL.

use std::sync::Arc;
use std::time::Duration;

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep, timeout, Instant};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{Connector, MaybeTlsStream, WebSocketStream};
use tracing::{debug, error, info, warn};
use url::Url;

use crate::config::Config;
use crate::state::State;

const MEXC_WS_URL: &str = "wss://contract.mexc.com/edge";
const PING_INTERVAL_SECS: u64 = 15;
const RECONNECT_MIN_MS: u64 = 1_000;
const RECONNECT_MAX_MS: u64 = 30_000;
const LOGIN_TIMEOUT_SECS: u64 = 5;

type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type BoxErr = Box<dyn std::error::Error + Send + Sync>;

#[derive(Debug, Deserialize)]
struct PositionPush {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    data: Option<PositionData>,
}

#[derive(Debug, Deserialize)]
struct PositionData {
    #[serde(default, rename = "positionId")]
    position_id: i64,
    #[serde(default, rename = "holdVol")]
    hold_vol: f64,
    #[serde(default, rename = "realised")]
    realised: f64,
    #[serde(default, rename = "positionType")]
    position_type: i32, // 1=long, 2=short
}

#[derive(Debug, Deserialize)]
struct DealPush {
    #[serde(default)]
    symbol: Option<String>,
    #[serde(default)]
    data: Option<DealData>,
}

#[derive(Debug, Deserialize)]
struct DealData {
    #[serde(default, rename = "orderId")]
    order_id: String,
    // MEXC шле "profit", не "realisedProfit". Це і є реальний realized PnL для close deals.
    #[serde(default)]
    profit: f64,
    #[serde(default, rename = "realisedProfit")]
    realised_profit: f64,
    #[serde(default)]
    category: i32, // 1=open, 2=close, 3=liquidation
    #[serde(default)]
    side: i32,     // 1=open_long, 2=close_short, 3=open_short, 4=close_long
    #[serde(default)]
    price: f64,
    #[serde(default)]
    vol: f64,
    #[serde(default)]
    symbol: String,
    #[serde(default)]
    timestamp: u64,
}

#[derive(Debug, Deserialize)]
struct AssetPush {
    #[serde(default)]
    data: Option<AssetData>,
}

#[derive(Debug, Deserialize)]
struct AssetData {
    #[serde(default)]
    currency: String,
    #[serde(default, rename = "availableBalance")]
    available_balance: f64,
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
        if n == 0 { return Err("proxy closed during CONNECT".into()); }
        buf.extend_from_slice(&tmp[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") { break; }
        if buf.len() > 8192 { return Err("proxy CONNECT response too large".into()); }
    }
    let resp = std::str::from_utf8(&buf).unwrap_or("");
    let first_line = resp.lines().next().unwrap_or("");
    if !first_line.starts_with("HTTP/1.1 200") && !first_line.starts_with("HTTP/1.0 200") {
        return Err(format!("proxy CONNECT failed: {}", first_line).into());
    }

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
        client_req, tcp, None, Some(connector),
    ).await?;

    Ok(ws)
}

async fn run_session(cfg: &Config, state: &Arc<State>) -> Result<(), BoxErr> {
    let mut ws = connect_via_proxy(cfg).await?;
    info!(target: "mexc_priv", "connected to {}", MEXC_WS_URL);

    // === LOGIN ===
    let login_msg = json!({"method":"login","param":{"token": cfg.account.mexc_uid}});
    ws.send(Message::Text(login_msg.to_string())).await?;

    // Чекаємо rs.login до 5 секунд
    let login_ok = timeout(Duration::from_secs(LOGIN_TIMEOUT_SECS), wait_login(&mut ws)).await;
    match login_ok {
        Ok(Ok(true)) => info!(target: "mexc_priv", "login success"),
        Ok(Ok(false)) => return Err("login rejected".into()),
        Ok(Err(e)) => return Err(format!("login error: {}", e).into()),
        Err(_) => return Err("login timeout".into()),
    }

    // === SUBSCRIPTIONS ===
    // Невелика затримка після login — деякі MEXC сервери потребують часу
    // на створення сесії авторизації перед прийомом subscribe.
    sleep(Duration::from_millis(150)).await;

    let token = cfg.account.mexc_uid.clone();

    // Шлемо ОБА варіанти підписки — MEXC прийме той що правильний.
    // Невикористаний просто не дасть ack, нічого не зламає.
    for ch in ["sub.personal.order.deal", "sub.personal.position", "sub.personal.asset", "sub.personal.order"] {
        // Варіант A: param = {} (стандартний з документації)
        let sub_a = json!({"method": ch, "param": {}});
        ws.send(Message::Text(sub_a.to_string())).await?;
        debug!(target: "mexc_priv", "→ {} (param={{}})", ch);

        // Варіант B: param = {"token": uid} (альтернативний)
        let sub_b = json!({"method": ch, "param": {"token": &token}});
        ws.send(Message::Text(sub_b.to_string())).await?;
        debug!(target: "mexc_priv", "→ {} (param=token)", ch);

        // Маленька пауза між каналами щоб MEXC встигав обробити
        sleep(Duration::from_millis(50)).await;
    }

    // Ping task
    let (ping_tx, mut ping_rx) = mpsc::channel::<()>(1);
    let ping_handle = tokio::spawn(async move {
        let mut tick = interval(Duration::from_secs(PING_INTERVAL_SECS));
        tick.tick().await;
        loop {
            tick.tick().await;
            if ping_tx.send(()).await.is_err() { break; }
        }
    });

    let result = read_loop(&mut ws, state, &mut ping_rx).await;
    ping_handle.abort();
    let _ = ws.send(Message::Close(None)).await;
    result
}

async fn wait_login(ws: &mut WsStream) -> Result<bool, BoxErr> {
    while let Some(msg) = ws.next().await {
        let txt = match msg? {
            Message::Text(t) => t,
            _ => continue,
        };
        let v: Value = serde_json::from_str(&txt)?;
        let channel = v.get("channel").and_then(|c| c.as_str()).unwrap_or("");
        if channel == "rs.login" {
            let data = v.get("data").and_then(|d| d.as_str()).unwrap_or("");
            return Ok(data == "success");
        }
        if channel == "rs.error" {
            warn!(target: "mexc_priv", "login error: {}", txt);
            return Ok(false);
        }
    }
    Err("ws closed during login".into())
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
                debug!(target: "mexc_priv", "ping");
            }
            _ = idle_check.tick() => {
                if last_msg.elapsed() > Duration::from_secs(60) {
                    return Err("no messages for 60s".into());
                }
            }
            msg = ws.next() => {
                let msg = match msg {
                    Some(Ok(m)) => m,
                    Some(Err(e)) => return Err(format!("ws recv: {}", e).into()),
                    None => return Err("ws closed".into()),
                };
                last_msg = Instant::now();
                match msg {
                    Message::Text(txt) => {
                        if let Err(e) = handle_text(&txt, state) {
                            warn!(target: "mexc_priv", "handle: {} | {}", e, &txt[..txt.len().min(200)]);
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
        "pong" | "rs.sub.personal.order.deal" | "rs.sub.personal.position" | "rs.sub.personal.asset" | "rs.sub.personal.order" => {
            debug!(target: "mexc_priv", "{}: {}", channel, &txt[..txt.len().min(150)]);
        }
        "rs.error" => {
            warn!(target: "mexc_priv", "server error: {}", txt);
        }
        "push.personal.position" => {
            handle_position(v, state)?;
        }
        "push.personal.order.deal" => {
            info!(target: "mexc_priv", "deal raw: {}", &txt[..txt.len().min(300)]);
            handle_deal(v, state)?;
        }
        "push.personal.asset" => {
            handle_asset(v, state)?;
        }
        _ => {}
    }
    Ok(())
}

fn handle_position(v: Value, state: &Arc<State>) -> Result<(), BoxErr> {
    let push: PositionPush = serde_json::from_value(v)?;
    let symbol_raw = push.symbol.unwrap_or_default();
    let data = match push.data {
        Some(d) => d,
        None => return Ok(()),
    };

    if data.hold_vol <= 0.0 {
        // Позиція закрилась — не чіпаємо state.open_trade тут (це робить monitor)
        debug!(target: "mexc_priv", "position {} closed (holdVol=0)", symbol_raw);
        // Шлемо подію щоб monitor міг швидко зреагувати (без sleep+REST)
        let symbol_norm = symbol_raw.replace('_', "");
        let _ = state.position_closes.send((symbol_norm, data.position_id));
        return Ok(());
    }

    // Нормалізуємо символ: TAO_USDT → TAOUSDT (як зберігається в open_trade)
    let symbol = symbol_raw.replace('_', "");

    // Якщо у нас є open_trade з цим символом і position_id=0 — заповнюємо
    let mut trade_lock = state.open_trade.write();
    if let Some(ref mut t) = *trade_lock {
        if t.symbol == symbol {
            // Підтверджуємо що позиція реально існує на біржі
            if t.position_id == 0 {
                t.position_id = data.position_id;
            }
            if !t.verified {
                t.verified = true;
                info!(target: "mexc_priv", "position verified: {} pid={}", symbol, data.position_id);
            }
        }
    }
    Ok(())
}

fn handle_deal(v: Value, state: &Arc<State>) -> Result<(), BoxErr> {
    let push: DealPush = serde_json::from_value(v)?;
    let symbol_raw = push.symbol.clone().unwrap_or_default();
    let data = match push.data {
        Some(d) => d,
        None => return Ok(()),
    };

    // CLOSE deal detection: side=2 (close_short) або side=4 (close_long).
    // MEXC шле close events з category=1, не 2 — тому ловимо саме по side.
    if (data.side == 2 || data.side == 4) && !symbol_raw.is_empty() {
        let symbol_norm = symbol_raw.replace('_', "");
        let pid = state.open_trade.read().as_ref().map(|t| t.position_id).unwrap_or(0);
        let _ = state.position_closes.send((symbol_norm.clone(), pid));
        debug!(target: "mexc_priv", "close deal: sym={} side={} oid={} → position_close emitted",
               symbol_norm, data.side, data.order_id);
    }

    // CLOSE deals: side=2 (close_short) або side=4 (close_long).
    // MEXC шле close events з category=1, не 2 — тому ловимо по side.
    if (data.side != 2 && data.side != 4) || data.order_id.is_empty() {
        return Ok(());
    }

    // Dedup: якщо вже обробляли цей orderId — пропускаємо
    if state.corrected_oids.contains_key(&data.order_id) {
        debug!(target: "mexc_priv", "deal {} already processed (dedup)", data.order_id);
        return Ok(());
    }
    state.corrected_oids.insert(data.order_id.clone(), ());

    // MEXC шле "profit", не "realisedProfit". Беремо те що ненульове.
    let real_pnl = if data.profit != 0.0 { data.profit } else { data.realised_profit };

    // Шукаємо trade в історії з цим close_order_id і коригуємо PnL
    let mut hist = state.trade_history.write();
    for t in hist.iter_mut().rev().take(20) {
        if t.close_order_id == data.order_id && t.status == "CLOSED" {
            let old_pnl = t.pnl;
            t.pnl = real_pnl;
            *state.total_pnl.lock() += t.pnl - old_pnl;
            info!(target: "mexc_priv", "PnL corrected: oid={} approx={:.4} → real={:.4}",
                  data.order_id, old_pnl, real_pnl);
            drop(hist);
            let state_save = state.clone();
            tokio::spawn(async move {
                let snapshot: Vec<_> = state_save.trade_history.read().iter().rev().take(500).rev().cloned().collect();
                if let Ok(json) = serde_json::to_string(&snapshot) {
                    let _ = tokio::fs::write("/root/arb_rust/trade_history.json", json).await;
                }
            });
            return Ok(());
        }
    }
    // PHANTOM: trade не знайдено в history (monitor вже почистив state),
    // але WS deal каже що позиція реально була і реалізувала profit.
    // Створюємо синтетичний trade record щоб історія не губилась.
    if data.side == 2 || data.side == 4 {
        // symbol може бути на корені push або всередині data — беремо непорожній
        let sym_source = if !symbol_raw.is_empty() { symbol_raw.clone() } else { data.symbol.clone() };
        let symbol_norm = sym_source.replace('_', "");
        let side_str = if data.side == 2 { "SHORT" } else { "LONG" };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let synthetic = crate::state::Trade {
            id: format!("ws_{}", data.order_id),
            symbol: symbol_norm.clone(),
            side: side_str.to_string(),
            entry_price: 0.0,
            exit_price: data.price,
            size: data.vol,
            pnl: real_pnl,
            status: "CLOSED".to_string(),
            close_order_id: data.order_id.clone(),
            position_id: 0,
            open_time_ms: now.saturating_sub(60_000),
            close_time_ms: if data.timestamp > 0 { data.timestamp } else { now },
            spread_at_entry: 0.0,
            stop_armed: false,
            open_http_ms: 0,
            close_reason: "ws_synthetic".to_string(),
            verified: true,
        };
        hist.push(synthetic);
        *state.total_pnl.lock() += real_pnl;
        info!(target: "mexc_priv", "📝 synthetic trade from WS: {} {} pnl={:.4} oid={}",
              symbol_norm, side_str, real_pnl, data.order_id);
        drop(hist);
        let state_save = state.clone();
        tokio::spawn(async move {
            let snapshot: Vec<_> = state_save.trade_history.read().iter().rev().take(500).rev().cloned().collect();
            if let Ok(json) = serde_json::to_string(&snapshot) {
                let _ = tokio::fs::write("/root/arb_rust/trade_history.json", json).await;
            }
        });
        return Ok(());
    }
    debug!(target: "mexc_priv", "deal received but no matching trade: oid={}", data.order_id);
    Ok(())
}

fn handle_asset(v: Value, state: &Arc<State>) -> Result<(), BoxErr> {
    let push: AssetPush = serde_json::from_value(v)?;
    let data = match push.data {
        Some(d) => d,
        None => return Ok(()),
    };
    if data.currency == "USDT" {
        *state.balance_usdt.lock() = data.available_balance;
        debug!(target: "mexc_priv", "balance: {} USDT", data.available_balance);
    }
    Ok(())
}

pub async fn run_mexc_private_ws(cfg: Arc<Config>, state: Arc<State>) {
    let mut backoff_ms = RECONNECT_MIN_MS;
    loop {
        match run_session(&cfg, &state).await {
            Ok(()) => {
                warn!(target: "mexc_priv", "session ended cleanly");
                backoff_ms = RECONNECT_MIN_MS;
            }
            Err(e) => {
                error!(target: "mexc_priv", "session failed: {}", e);
                sleep(Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(RECONNECT_MAX_MS);
            }
        }
    }
}
