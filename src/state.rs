use dashmap::DashMap;
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, Default)]
pub struct PriceData {
    pub bid: f64,
    pub ask: f64,
    pub timestamp_ms: u64,
}

#[derive(Debug, Default)]
pub struct OrderBook {
    /// price -> volume (contracts)
    pub bids: BTreeMap<i64, f64>,  // price scaled to int (price * 1e8) for ordering
    pub asks: BTreeMap<i64, f64>,
    pub version: u64,
    pub last_update_ms: u64,
}

impl OrderBook {
    pub fn best_bid(&self) -> Option<(f64, f64)> {
        self.bids.iter().next_back().map(|(&p, &v)| (p as f64 / 1e8, v))
    }
    pub fn best_ask(&self) -> Option<(f64, f64)> {
        self.asks.iter().next().map(|(&p, &v)| (p as f64 / 1e8, v))
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Trade {
    pub id: String,
    pub symbol: String,
    pub side: String,           // "BUY" or "SELL"
    pub entry_price: f64,
    pub exit_price: f64,
    pub size: f64,              // contracts
    pub pnl: f64,
    pub status: String,         // "OPEN" / "CLOSED"
    pub close_order_id: String,
    pub position_id: i64,
    pub open_time_ms: u64,
    pub close_time_ms: u64,
    pub spread_at_entry: f64,
    pub stop_armed: bool,
    pub open_http_ms: u64,
    pub close_reason: String,
    pub verified: bool,  // true якщо MEXC реально підтвердив відкриття через WS
}

pub struct State {
    pub running: AtomicBool,
    pub started_at_ms: AtomicU64,
    /// symbol -> Binance bid/ask
    pub binance_prices: DashMap<String, PriceData>,
    /// symbol -> MEXC orderbook
    pub mexc_books: DashMap<String, RwLock<OrderBook>>,
    /// symbol -> MEXC bid/ask (top-of-book cache for fast read)
    pub mexc_prices: DashMap<String, PriceData>,
    /// Currently open trade (only one at a time for now)
    pub open_trade: RwLock<Option<Trade>>,
    /// History of closed trades (last N)
    pub trade_history: RwLock<Vec<Trade>>,
    /// Account balance
    pub balance_usdt: parking_lot::Mutex<f64>,
    /// Total stats
    pub total_pnl: parking_lot::Mutex<f64>,
    pub total_trades: AtomicU64,
    pub winning_trades: AtomicU64,
    /// Cooldown timestamp (ms) — no new trade until then
    pub cooldown_until_ms: AtomicU64,
    pub global_cooldown_until_ms: AtomicU64,
    pub risk_control_blocked: AtomicBool,
    /// Per-symbol cooldown (ms) — не відкривати новий ордер по символу до цього часу
    pub symbol_cooldown: DashMap<String, u64>,
    /// Set of close orderIds that have been WS-corrected (avoid double-correction)
    pub corrected_oids: DashMap<String, ()>,
    /// Broadcast канал — кожен write в *_prices шле тригер.
    /// Strategy підписується на цей канал і реагує миттєво.
    pub price_updates: broadcast::Sender<()>,
    pub position_closes: broadcast::Sender<(String, i64)>,
}

impl State {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            running: AtomicBool::new(false),
            started_at_ms: AtomicU64::new(now_ms()),
            binance_prices: DashMap::new(),
            mexc_books: DashMap::new(),
            mexc_prices: DashMap::new(),
            open_trade: RwLock::new(None),
            trade_history: RwLock::new(Vec::with_capacity(1000)),
            balance_usdt: parking_lot::Mutex::new(0.0),
            total_pnl: parking_lot::Mutex::new(0.0),
            total_trades: AtomicU64::new(0),
            winning_trades: AtomicU64::new(0),
            cooldown_until_ms: AtomicU64::new(0),
            global_cooldown_until_ms: AtomicU64::new(0),
            risk_control_blocked: AtomicBool::new(false),
            symbol_cooldown: DashMap::new(),
            corrected_oids: DashMap::new(),
            // Channel capacity 256 — якщо стратегія не встигає, broadcast скіпає старі повідомлення.
            // Це і потрібно: ми хочемо реагувати на ОСТАННІЙ стан, а не наздоганяти історію.
            price_updates: broadcast::channel(256).0,
            position_closes: broadcast::channel(64).0,
        })
    }
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH).unwrap()
        .as_millis() as u64
}
