//! Spread tracker — статистика спредів кожні 100мс без блокування торгівлі.
//!
//! Збирає в пам'яті: гістограму спредів, baseline (модальний), події ≥0.10%
//! з тривалістю і успішним поверненням до baseline.

use std::sync::Arc;
use dashmap::DashMap;
use parking_lot::Mutex;
use serde::Serialize;
use tokio::time::{interval, Duration};

use crate::state::{State, now_ms};

const TICK_MS: u64 = 100;
const EVENT_TRIGGER_PCT: f64 = 0.05;  // спред ≥ 0.05% починає "подію"
const SIGNIFICANT_PCT: f64 = 0.10;     // події ≥ 0.10% важливі
const MAX_EVENTS_PER_SYMBOL: usize = 1000;

/// Бакет гістограми (range у %)
#[derive(Default, Clone, Serialize)]
pub struct Bucket {
    pub range: String,
    pub count: u64,
}

#[derive(Clone, Serialize)]
pub struct SpreadEvent {
    pub start_ms: u64,
    pub max_pct: f64,
    pub duration_ms: u64,
    pub converged: bool,  // повернувся до baseline
}

#[derive(Default)]
pub struct SymbolStats {
    pub samples: u64,
    pub histogram: [u64; 11],  // <0.01, 0.01-0.02, 0.02-0.03, 0.03-0.04, 0.04-0.05, 0.05-0.06, 0.06-0.07, 0.07-0.08, 0.08-0.09, 0.09-0.10, >=0.10
    pub max_seen: f64,
    pub events: Vec<SpreadEvent>,
    pub current_event: Option<CurrentEvent>,
}

pub struct CurrentEvent {
    pub start_ms: u64,
    pub max_pct: f64,
}

#[derive(Serialize)]
pub struct SymbolStatsJson {
    pub samples: u64,
    pub histogram: Vec<Bucket>,
    pub baseline_pct: f64,         // модальний бакет (середина)
    pub max_seen: f64,
    pub total_events: usize,
    pub significant_events: usize,  // ≥0.10%
    pub converged_events: usize,    // зрівнялися до baseline
    pub avg_duration_ms: u64,
    pub events_by_threshold: Vec<(String, usize)>,  // ≥0.05/0.10/0.15/0.20/0.25/0.30
}

pub type Tracker = Arc<DashMap<String, Mutex<SymbolStats>>>;

pub fn new_tracker() -> Tracker {
    Arc::new(DashMap::new())
}

pub fn start_spread_tracker(state: Arc<State>, tracker: Tracker) {
    tokio::spawn(async move {
        let mut tick = interval(Duration::from_millis(TICK_MS));
        loop {
            tick.tick().await;
            let now = now_ms();
            // Йдемо по всіх mexc_prices (там символи, які точно слухаємо)
            for entry in state.mexc_prices.iter() {
                let sym = entry.key().clone();
                let mexc = entry.value();
                let bnc = match state.binance_prices.get(&sym) {
                    Some(p) => p.clone(),
                    None => continue,
                };
                if mexc.bid <= 0.0 || mexc.ask <= 0.0 || bnc.bid <= 0.0 || bnc.ask <= 0.0 {
                    continue;
                }
                // Спред = max з двох напрямків
                let l = (bnc.bid - mexc.ask) / mexc.ask * 100.0;
                let s = (mexc.bid - bnc.ask) / bnc.ask * 100.0;
                let spread = l.max(s);

                let entry = tracker.entry(sym.clone()).or_insert_with(|| Mutex::new(SymbolStats::default()));
                let mut st = entry.lock();
                st.samples += 1;
                if spread.abs() > st.max_seen {
                    st.max_seen = spread.abs();
                }
                // Гістограма
                let abs_sp = spread.abs();
                let idx = if abs_sp < 0.01 { 0 }
                    else if abs_sp < 0.02 { 1 }
                    else if abs_sp < 0.03 { 2 }
                    else if abs_sp < 0.04 { 3 }
                    else if abs_sp < 0.05 { 4 }
                    else if abs_sp < 0.06 { 5 }
                    else if abs_sp < 0.07 { 6 }
                    else if abs_sp < 0.08 { 7 }
                    else if abs_sp < 0.09 { 8 }
                    else if abs_sp < 0.10 { 9 }
                    else { 10 };
                st.histogram[idx] += 1;

                // Логіка подій
                if abs_sp >= EVENT_TRIGGER_PCT {
                    if let Some(ce) = &mut st.current_event {
                        if abs_sp > ce.max_pct {
                            ce.max_pct = abs_sp;
                        }
                    } else {
                        st.current_event = Some(CurrentEvent { start_ms: now, max_pct: abs_sp });
                    }
                } else {
                    // Спред впав нижче 0.05% — закриваємо подію
                    if let Some(ce) = st.current_event.take() {
                        let duration = now.saturating_sub(ce.start_ms);
                        // Беремо converged=true бо ми реально побачили що спред впав
                        st.events.push(SpreadEvent {
                            start_ms: ce.start_ms,
                            max_pct: ce.max_pct,
                            duration_ms: duration,
                            converged: true,
                        });
                        if st.events.len() > MAX_EVENTS_PER_SYMBOL {
                            st.events.remove(0);
                        }
                    }
                }
            }
        }
    });
}

/// Беремо snapshot для API
pub fn snapshot(tracker: &Tracker) -> std::collections::BTreeMap<String, SymbolStatsJson> {
    let mut out = std::collections::BTreeMap::new();
    for entry in tracker.iter() {
        let sym = entry.key().clone();
        let st = entry.value().lock();
        // Бакети
        let labels = ["<0.01%", "0.01-0.02%", "0.02-0.03%", "0.03-0.04%", "0.04-0.05%", "0.05-0.06%", "0.06-0.07%", "0.07-0.08%", "0.08-0.09%", "0.09-0.10%", ">=0.10%"];
        let buckets: Vec<Bucket> = labels.iter().enumerate()
            .map(|(i, l)| Bucket { range: l.to_string(), count: st.histogram[i] })
            .collect();
        // Baseline = середина модального бакета (без бакета 0)
        let mut max_idx = 1usize;
        let mut max_val = st.histogram[1];
        for i in 2..11 {
            if st.histogram[i] > max_val {
                max_val = st.histogram[i];
                max_idx = i;
            }
        }
        let baseline_pct = match max_idx {
            0 => 0.005, 1 => 0.015, 2 => 0.025, 3 => 0.035, 4 => 0.045,
            5 => 0.055, 6 => 0.065, 7 => 0.075, 8 => 0.085, 9 => 0.095,
            _ => 0.10,
        };
        let total_events = st.events.len();
        let significant: Vec<&SpreadEvent> = st.events.iter().filter(|e| e.max_pct >= SIGNIFICANT_PCT).collect();
        let converged_events = significant.iter().filter(|e| e.converged).count();
        let avg_duration_ms = if !significant.is_empty() {
            significant.iter().map(|e| e.duration_ms).sum::<u64>() / significant.len() as u64
        } else { 0 };
        // По порогах
        let thresholds = [0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10];
        let events_by_threshold: Vec<(String, usize)> = thresholds.iter()
            .map(|t| (
                format!(">={:.2}%", t),
                st.events.iter().filter(|e| e.max_pct >= *t).count()
            ))
            .collect();
        out.insert(sym, SymbolStatsJson {
            samples: st.samples,
            histogram: buckets,
            baseline_pct,
            max_seen: st.max_seen,
            total_events,
            significant_events: significant.len(),
            converged_events,
            avg_duration_ms,
            events_by_threshold,
        });
    }
    out
}
