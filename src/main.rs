mod config;
mod state;
mod spread_tracker;
mod mexc;
mod binance;
mod strategy;
mod monitor;
mod web;

use std::sync::Arc;

type DynErr = Box<dyn std::error::Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), DynErr> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = Arc::new(config::Config::load("config.toml")?);
    println!("Config loaded: account={}, symbols={:?}", cfg.account.label, cfg.trading.symbols);

    let state = state::State::new();

    // Завантажуємо trade history з диску (зберігається між рестартами)
    if let Ok(data) = std::fs::read_to_string("/root/arb_rust/trade_history.json") {
        if let Ok(trades) = serde_json::from_str::<Vec<state::Trade>>(&data) {
            *state.trade_history.write() = trades;
            println!("Loaded {} trades from history", state.trade_history.read().len());
        }
    }
    println!("State created");

    let client = mexc::http::MexcClient::new(cfg.account.mexc_uid.clone(), &cfg.account.proxy_url)?;
    println!("MEXC client created");

    let client_bal = client.clone();
    let state_bal = state.clone();
    tokio::spawn(async move {
        match client_bal.get_balance().await {
            Ok(bal) => {
                *state_bal.balance_usdt.lock() = bal;
                println!("Balance: {} USDT", bal);
            }
            Err(e) => eprintln!("Balance fetch failed: {}", e),
        }
    });

    let cfg_mexc = cfg.clone();
    let state_mexc = state.clone();
    tokio::spawn(async move {
        mexc::ws_public::run_mexc_public_ws(cfg_mexc, state_mexc).await;
    });

    let cfg_priv = cfg.clone();
    let state_priv = state.clone();
    tokio::spawn(async move {
        mexc::ws_private::run_mexc_private_ws(cfg_priv, state_priv).await;
    });

    let cfg_bnc = cfg.clone();
    let state_bnc = state.clone();
    tokio::spawn(async move {
        binance::ws::run_binance_ws(cfg_bnc, state_bnc).await;
    });

    let cfg_strat = cfg.clone();
    let state_strat = state.clone();
    let client_strat = client.clone();
    tokio::spawn(async move {
        strategy::run_strategy(cfg_strat, state_strat, client_strat).await;
    });

    let cfg_mon = cfg.clone();
    let state_mon = state.clone();
    let client_mon = client.clone();
    tokio::spawn(async move {
        monitor::run_monitor(cfg_mon, state_mon, client_mon).await;
    });

    state.running.store(true, std::sync::atomic::Ordering::Relaxed);

    println!("Starting web server on {}...", cfg.server.listen);
    let tracker = spread_tracker::new_tracker();
    spread_tracker::start_spread_tracker(state.clone(), tracker.clone());
    web::run_web(cfg.clone(), state.clone(), tracker).await;

    Ok(())
}
