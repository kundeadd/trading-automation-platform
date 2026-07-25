//! Web-сервер: axum HTTP + WebSocket broadcast до фронта.
//! Сумісний з існуючим index.html (типи init/spread/trade_open/trade_close/log).

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message as WsMessage, WebSocket, WebSocketUpgrade};
use axum::extract::State as AxumState;
use axum::http::{header, Method};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::sync::broadcast;
use tower_http::cors::{Any, CorsLayer};
use tower_http::services::ServeDir;
use tracing::{debug, info, warn};

use crate::config::Config;
use crate::state::{now_ms, State};
use crate::spread_tracker::{Tracker, snapshot as spread_snapshot};
use crate::strategy::min_spread_for;

const ACCOUNT_ID: i64 = 0;

/// Канал для відправки повідомлень всім підключеним WebSocket-клієнтам.
/// Ємність 256 — достатньо щоб не блокуватись при бурсті trade events.
type BroadcastTx = broadcast::Sender<String>;

#[derive(Clone)]
struct AppState {
    cfg: Arc<Config>,
    state: Arc<State>,
    ws_tx: BroadcastTx,
    tracker: Tracker,
}

pub async fn run_web(cfg: Arc<Config>, state: Arc<State>, tracker: Tracker) {
    let (ws_tx, _ws_rx) = broadcast::channel::<String>(256);

    // Спавн фонового таска: кожну секунду шле snapshot спредів усім клієнтам
    let cfg_bg = cfg.clone();
    let state_bg = state.clone();
    let tx_bg = ws_tx.clone();
    tokio::spawn(async move {
        spread_broadcaster(cfg_bg, state_bg, tx_bg).await;
    });

    // Спавн фонового таска: ловить trade_open/trade_close через price_updates тригер
    // (хак: моніторимо open_trade зміни через polling, бо у нас немає окремого trade-канала)
    let state_tr = state.clone();
    let tx_tr = ws_tx.clone();
    tokio::spawn(async move {
        trade_watcher(state_tr, tx_tr).await;
    });

    let app_state = AppState {
        cfg: cfg.clone(),
        state: state.clone(),
        ws_tx,
        tracker: tracker.clone(),
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(Any);

    let app = Router::new()
        .route("/ws", get(ws_handler))
        .route("/api/status", get(api_status))
        .route("/api/start", post(api_start))
        .route("/api/stop", post(api_stop))
        .route("/api/config", get(api_config))
        .route("/api/accounts/:id/reset_stats", post(api_reset_stats))
        .route("/api/accounts/:id/unblock", post(api_unblock))
        .route("/api/accounts/:id", post(api_update_account))
        .route("/api/spread_stats", get(api_spread_stats))
        .nest_service("/", ServeDir::new("static"))
        .layer(cors)
        .with_state(app_state);

    let addr: SocketAddr = cfg.server.listen.parse().expect("bad listen addr");
    info!(target: "web", "listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.expect("bind failed");
    axum::serve(listener, app).await.ok();
}

// === WebSocket handler ===

async fn ws_handler(
    ws: WebSocketUpgrade,
    AxumState(app): AxumState<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, app))
}

async fn handle_socket(socket: WebSocket, app: AppState) {
    let (mut sender, mut receiver) = socket.split();
    let mut rx = app.ws_tx.subscribe();

    // 1. Відразу шлемо init snapshot
    let init = build_init(&app);
    if sender.send(WsMessage::Text(init)).await.is_err() {
        return;
    }

    // 2. Loop: пересилаємо broadcast → клієнту, ігноруємо вхідні
    let send_task = tokio::spawn(async move {
        while let Ok(msg) = rx.recv().await {
            if sender.send(WsMessage::Text(msg)).await.is_err() {
                break;
            }
        }
    });

    // Обробка вхідних від клієнта (поки що нічого не робимо, тільки drain)
    let recv_task = tokio::spawn(async move {
        while let Some(Ok(_msg)) = receiver.next().await {}
    });

    tokio::select! {
        _ = send_task => {}
        _ = recv_task => {}
    }
    debug!(target: "web", "client disconnected");
}

fn build_init(app: &AppState) -> String {
    let spreads = build_spreads(&app.cfg, &app.state);
    let trades: Vec<Value> = app.state.trade_history.read().iter().rev().take(50)
        .map(trade_to_json).collect();

    let status = build_status(&app.cfg, &app.state);

    json!({
        "type": "init",
        "spreads": spreads,
        "trades": trades,
        "status": status,
    }).to_string()
}

fn active_symbols(symbols: &[String]) -> Vec<String> {
    symbols.iter().filter(|s| min_spread_for(s) < 999.0).cloned().collect()
}

fn build_spreads(cfg: &Config, state: &State) -> Vec<Value> {
    let now = now_ms();
    let mut out = Vec::new();
    for sym in cfg.trading.symbols.iter().filter(|s| min_spread_for(s) < 999.0) {
        let mexc = state.mexc_prices.get(sym);
        let bnc = state.binance_prices.get(sym);
        let (mexc_bid, mexc_ask, m_age) = match mexc {
            Some(p) => (p.bid, p.ask, now.saturating_sub(p.timestamp_ms)),
            None => (0.0, 0.0, 0),
        };
        let (bnc_bid, bnc_ask, b_age) = match bnc {
            Some(p) => (p.bid, p.ask, now.saturating_sub(p.timestamp_ms)),
            None => (0.0, 0.0, 0),
        };
        let spread_pct = if mexc_ask > 0.0 && bnc_bid > 0.0 {
            let l = (bnc_bid - mexc_ask) / mexc_ask * 100.0;
            let s = (mexc_bid - bnc_ask) / bnc_ask * 100.0;
            l.max(s)
        } else { 0.0 };

        out.push(json!({
            "symbol": sym,
            "mexc_bid": mexc_bid,
            "mexc_ask": mexc_ask,
            "binance_bid": bnc_bid,
            "binance_ask": bnc_ask,
            "spread_pct": spread_pct,
            "mexc_age_ms": m_age,
            "binance_age_ms": b_age,
            "account_id": ACCOUNT_ID,
        }));
    }
    out
}

fn build_status(cfg: &Config, state: &State) -> Value {
    let running = state.running.load(std::sync::atomic::Ordering::Relaxed);
    let balance = *state.balance_usdt.lock();
    let total_pnl = *state.total_pnl.lock();
    let total = state.total_trades.load(std::sync::atomic::Ordering::Relaxed);
    let wins = state.winning_trades.load(std::sync::atomic::Ordering::Relaxed);

    // Розрізняємо: реально в позиції (verified=true) vs ще відкривається (verified=false)
    let (open_trade, in_trade, opening) = {
        let lock = state.open_trade.read();
        match lock.as_ref() {
            Some(t) => {
                let json = trade_to_json(t);
                if t.verified {
                    (Some(json), true, false)  // реально в позиції
                } else {
                    (Some(json), false, true)  // тільки що відкрили, чекаємо підтвердження
                }
            }
            None => (None, false, false),
        }
    };
    let has_uid = !cfg.account.mexc_uid.is_empty();
    let rc_blocked = state.risk_control_blocked.load(std::sync::atomic::Ordering::Relaxed);
    let blocked_status: serde_json::Value = if rc_blocked {
        json!("RISK CONTROL")
    } else {
        json!(false)
    };

    // Псевдо-правило для UI: будь-який баланс, будь-який спред, фіксований розмір
    let position_rules = vec![json!({
        "balance_min": 0.0,
        "balance_max": 9e9,
        "spread_min": cfg.trading.min_spread_pct,
        "spread_max": 9e9,
        "size_usdt": cfg.trading.size_usdt,
    })];

    json!({
        "running": running,
        "accounts": [{
            "id": ACCOUNT_ID,
            "label": cfg.account.label,
            "enabled": true,
            "symbols": active_symbols(&cfg.trading.symbols),
            "symbol": active_symbols(&cfg.trading.symbols).first().cloned().unwrap_or_default(),
            "has_uid": has_uid,
            "balance": balance,
            "total_pnl": total_pnl,
            "total_trades": total,
            "winning_trades": wins,
            "auto_trade": running,
            "in_trade": in_trade,
            "opening": opening,
            "blocked": blocked_status,
            "open_trade": open_trade,
            "position_rules": position_rules,
            "leverage": 200,
        }]
    })
}

fn trade_to_json(t: &crate::state::Trade) -> Value {
    json!({
        "id": t.id,
        "account_id": ACCOUNT_ID,
        "symbol": t.symbol,
        "side": t.side,
        "entry_price": t.entry_price,
        "exit_price": t.exit_price,
        "size": t.size,
        "pnl": t.pnl,
        "status": t.status,
        "open_time_ms": t.open_time_ms,
        "close_time_ms": t.close_time_ms,
        "spread_at_entry": t.spread_at_entry,
        "open_http_ms": t.open_http_ms,
        "close_reason": t.close_reason,
        "verified": t.verified,
    })
}

// === Background tasks ===

async fn spread_broadcaster(cfg: Arc<Config>, state: Arc<State>, tx: BroadcastTx) {
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    loop {
        tick.tick().await;
        if tx.receiver_count() == 0 {
            continue; // ніхто не слухає — не витрачаємо CPU
        }
        for sym_data in build_spreads(&cfg, &state) {
            let msg = json!({"type": "spread", "data": sym_data}).to_string();
            let _ = tx.send(msg);
        }
    }
}

/// Polling open_trade — відстежує open/close події і шле broadcast.
/// Це простіше ніж переробляти state на додаткові канали.
async fn trade_watcher(state: Arc<State>, tx: BroadcastTx) {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    let mut last_open_id: Option<String> = None;
    let mut last_history_len: usize = state.trade_history.read().len();

    loop {
        tick.tick().await;
        if tx.receiver_count() == 0 {
            continue;
        }

        // Нова відкрита позиція?
        let cur_open = state.open_trade.read().as_ref().map(|t| (t.id.clone(), trade_to_json(t)));
        match (&last_open_id, &cur_open) {
            (None, Some((id, j))) => {
                let msg = json!({"type": "trade_open", "data": j}).to_string();
                let _ = tx.send(msg);
                last_open_id = Some(id.clone());
            }
            (Some(prev), Some((id, _))) if prev != id => {
                last_open_id = Some(id.clone());
            }
            _ => {}
        }

        // Нові закриті trade у history?
        let hist = state.trade_history.read();
        if hist.len() > last_history_len {
            // Шлемо всі нові
            for t in &hist[last_history_len..] {
                let msg = json!({"type": "trade_close", "data": trade_to_json(t)}).to_string();
                let _ = tx.send(msg);
            }
            last_history_len = hist.len();
            // Якщо позиція закрилась — обнуляємо last_open_id
            if state.open_trade.read().is_none() {
                last_open_id = None;
            }
        }
    }
}

// === REST endpoints ===

async fn api_status(AxumState(app): AxumState<AppState>) -> impl IntoResponse {
    axum::Json(build_status(&app.cfg, &app.state))
}

async fn api_start(AxumState(app): AxumState<AppState>) -> impl IntoResponse {
    app.state.running.store(true, std::sync::atomic::Ordering::Relaxed);
    info!(target: "web", "API: start");
    axum::Json(json!({"running": true}))
}

async fn api_stop(AxumState(app): AxumState<AppState>) -> impl IntoResponse {
    app.state.running.store(false, std::sync::atomic::Ordering::Relaxed);
    info!(target: "web", "API: stop");
    axum::Json(json!({"running": false}))
}

async fn api_config(AxumState(app): AxumState<AppState>) -> impl IntoResponse {
    axum::Json(json!({
        "accounts": [{
            "id": ACCOUNT_ID,
            "label": app.cfg.account.label,
            "mexc_uid_set": !app.cfg.account.mexc_uid.is_empty(),
            "proxy_set": !app.cfg.account.proxy_url.is_empty(),
        }],
        "trading": {
            "symbols": active_symbols(&app.cfg.trading.symbols),
            "min_spread_pct": app.cfg.trading.min_spread_pct,
            "converge_threshold_pct": app.cfg.trading.converge_threshold_pct,
            "size_usdt": app.cfg.trading.size_usdt,
        }
    }))
}

async fn api_reset_stats(AxumState(app): AxumState<AppState>) -> impl IntoResponse {
    *app.state.total_pnl.lock() = 0.0;
    app.state.total_trades.store(0, std::sync::atomic::Ordering::Relaxed);
    app.state.winning_trades.store(0, std::sync::atomic::Ordering::Relaxed);
    app.state.trade_history.write().clear();
    info!(target: "web", "API: reset_stats");
    axum::Json(json!({"ok": true}))
}

async fn api_unblock(AxumState(app): AxumState<AppState>) -> impl IntoResponse {
    // Розблоковуємо risk control + ставимо running=true якщо було вимкнено
    app.state.risk_control_blocked.store(false, std::sync::atomic::Ordering::Relaxed);
    app.state.running.store(true, std::sync::atomic::Ordering::Relaxed);
    warn!(target: "web", "🔓 RISK CONTROL cleared manually via API");
    axum::Json(json!({"ok": true, "message": "risk control cleared"}))
}

async fn api_update_account(AxumState(_app): AxumState<AppState>) -> impl IntoResponse {
    // Single-account, runtime config update не реалізовано
    warn!(target: "web", "API: update_account ignored (read-only config in v1)");
    axum::Json(json!({"ok": false, "message": "config is read-only, edit config.toml and restart"}))
}


async fn api_spread_stats(AxumState(app): AxumState<AppState>) -> impl IntoResponse {
    axum::Json(spread_snapshot(&app.tracker))
}
