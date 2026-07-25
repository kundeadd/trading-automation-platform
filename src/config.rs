use serde::Deserialize;
use std::fs;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub account: Account,
    pub trading: Trading,
    pub server: Server,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Account {
    pub label: String,
    pub mexc_uid: String,
    pub proxy_url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Trading {
    pub symbols: Vec<String>,
    pub min_spread_pct: f64,
    pub converge_threshold_pct: f64,
    pub size_usdt: f64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Server {
    pub listen: String,
}

impl Config {
    pub fn load(path: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let s = fs::read_to_string(path)?;
        let c: Config = toml::from_str(&s)?;
        Ok(c)
    }
}

/// Per-symbol metadata (contract size, leverage)
pub fn contract_size(sym: &str) -> f64 {
    match sym {
        "TAOUSDT" | "TAO_USDT" => 0.01,
        "ASTERUSDT" | "ASTER_USDT" => 1.0,
        "PENGUUSDT" | "PENGU_USDT" => 10.0,
        "ZECUSDT" | "ZEC_USDT" => 0.01,
        "ENAUSDT" | "ENA_USDT" => 10.0,
        "NVDAUSDT" | "NVIDIA_USDT" => 0.01,
        "BCHUSDT" | "BCH_USDT" => 0.01,
        "LINKUSDT" | "LINK_USDT" => 0.1,
        "HYPEUSDT" | "HYPE_USDT" => 0.1,
        "UNIUSDT" | "UNI_USDT" => 0.1,
        "XLMUSDT" | "XLM_USDT" => 10.0,
        "HBARUSDT" | "HBAR_USDT" => 1.0,
        "XAUTUSDT" | "XAUT_USDT" => 0.001,
        "XAGUSDT" | "SILVER_USDT" => 0.01,
        "SOLUSDT" | "SOL_USDT" => 0.1,
        "XRPUSDT" | "XRP_USDT" => 1.0,
        "SUIUSDT" | "SUI_USDT" => 1.0,
        "DASHUSDT" | "DASH_USDT" => 0.01,
        "ADAUSDT" | "ADA_USDT" => 1.0,
        "BNBUSDT" | "BNB_USDT" => 0.01,
        "LTCUSDT" | "LTC_USDT" => 0.01,
        "ZENUSDT" | "ZEN_USDT" => 0.1,
        "AVAXUSDT" | "AVAX_USDT" => 0.1,
        "WLDUSDT" | "WLD_USDT" => 1.0,
        "XMRUSDT" | "XMR_USDT" => 0.01,
        "ARBUSDT" | "ARB_USDT" => 1.0,
        "WIFUSDT" | "WIF_USDT" => 10.0,
        "KASUSDT" | "KAS_USDT" => 1000.0,
        "TRUMPUSDT" | "TRUMPOFFICIAL_USDT" => 0.1,
        "DOGEUSDT" | "DOGE_USDT" => 100.0,
        "ORDIUSDT" | "ORDI_USDT" => 0.1,
        "SIRENUSDT" | "SIREN_USDT" => 10.0,
        "PUMPUSDT" | "PUMPFUN_USDT" => 100.0,
        "HUSDT" | "H_USDT" => 1.0,
        "PIPPINUSDT" | "PIPPIN_USDT" => 10.0,
        "MUUSDT" | "MUSTOCK_USDT" => 0.01,
        "SNDKUSDT" | "SNDKSTOCK_USDT" => 0.001,
        "GIGGLEUSDT" | "GIGGLE_USDT" => 0.1,
        "FHEUSDT" | "FHE_USDT" => 10.0,
        "WLFIUSDT" | "WLFI_USDT" => 1.0,
        "MEGAUSDT" | "MEGA_USDT" => 1.0,
        "BLESSUSDT" | "BLESS_USDT" => 10.0,
        "CRVUSDT" | "CRV_USDT" => 0.1,
        "XPLUSDT" | "XPL_USDT" => 1.0,
        "ZROUSDT" | "ZRO_USDT" => 1.0,
        "LYNUSDT" | "LYN_USDT" => 1.0,
        "POLUSDT" | "POL_USDT" => 10.0,
        "INTCUSDT" | "INTCSTOCK_USDT" => 0.01,
        "ETCUSDT" | "ETC_USDT" => 0.1,
        "XANUSDT" | "XAN_USDT" => 10.0,
        "ETHFIUSDT" | "ETHFI_USDT" => 1.0,
        "MSTRUSDT" | "MSTRSTOCK_USDT" => 0.01,
        "STRKUSDT" | "STRK_USDT" => 1.0,
        "TSLAUSDT" | "TESLA_USDT" => 0.01,
        "JASMYUSDT" | "JASMY_USDT" => 10.0,
        "LIGHTUSDT" | "LIGHT_USDT" => 1.0,
        "AAPLUSDT" | "AAPLSTOCK_USDT" => 0.01,
        "BTCUSDT" | "BTC_USDT" => 0.0001,
        "ETHUSDT" | "ETH_USDT" => 0.01,
        "SOLUSDT" | "SOL_USDT" => 0.1,
        _ => 0.01,
    }
}

pub fn leverage(sym: &str) -> i32 {
    match sym {
        "TAOUSDT" | "TAO_USDT" => 300,
        "ASTERUSDT" | "ASTER_USDT" => 100,
        "PENGUUSDT" | "PENGU_USDT" => 125,
        "ZECUSDT" | "ZEC_USDT" => 100,
        "ENAUSDT" | "ENA_USDT" => 200,
        "NVDAUSDT" | "NVIDIA_USDT" => 100,
        "BCHUSDT" | "BCH_USDT" => 200,
        "LINKUSDT" | "LINK_USDT" => 300,
        "HYPEUSDT" | "HYPE_USDT" => 100,
        "UNIUSDT" | "UNI_USDT" => 200,
        "XLMUSDT" | "XLM_USDT" => 300,
        "HBARUSDT" | "HBAR_USDT" => 200,
        "XAUTUSDT" | "XAUT_USDT" => 1000,
        "XAGUSDT" | "SILVER_USDT" => 200,
        "SOLUSDT" | "SOL_USDT" => 300,
        "XRPUSDT" | "XRP_USDT" => 300,
        "SUIUSDT" | "SUI_USDT" => 300,
        "DASHUSDT" | "DASH_USDT" => 100,
        "ADAUSDT" | "ADA_USDT" => 300,
        "BNBUSDT" | "BNB_USDT" => 200,
        "LTCUSDT" | "LTC_USDT" => 200,
        "ZENUSDT" | "ZEN_USDT" => 100,
        "AVAXUSDT" | "AVAX_USDT" => 200,
        "WLDUSDT" | "WLD_USDT" => 300,
        "XMRUSDT" | "XMR_USDT" => 100,
        "ARBUSDT" | "ARB_USDT" => 200,
        "WIFUSDT" | "WIF_USDT" => 200,
        "KASUSDT" | "KAS_USDT" => 200,
        "TRUMPUSDT" | "TRUMPOFFICIAL_USDT" => 50,
        "DOGEUSDT" | "DOGE_USDT" => 300,
        "ORDIUSDT" | "ORDI_USDT" => 200,
        "SIRENUSDT" | "SIREN_USDT" => 50,
        "PUMPUSDT" | "PUMPFUN_USDT" => 100,
        "HUSDT" | "H_USDT" => 100,
        "PIPPINUSDT" | "PIPPIN_USDT" => 50,
        "MUUSDT" | "MUSTOCK_USDT" => 100,
        "SNDKUSDT" | "SNDKSTOCK_USDT" => 50,
        "GIGGLEUSDT" | "GIGGLE_USDT" => 50,
        "FHEUSDT" | "FHE_USDT" => 50,
        "WLFIUSDT" | "WLFI_USDT" => 100,
        "MEGAUSDT" | "MEGA_USDT" => 50,
        "BLESSUSDT" | "BLESS_USDT" => 100,
        "CRVUSDT" | "CRV_USDT" => 125,
        "XPLUSDT" | "XPL_USDT" => 100,
        "ZROUSDT" | "ZRO_USDT" => 125,
        "LYNUSDT" | "LYN_USDT" => 50,
        "POLUSDT" | "POL_USDT" => 125,
        "INTCUSDT" | "INTCSTOCK_USDT" => 100,
        "ETCUSDT" | "ETC_USDT" => 200,
        "XANUSDT" | "XAN_USDT" => 100,
        "ETHFIUSDT" | "ETHFI_USDT" => 125,
        "MSTRUSDT" | "MSTRSTOCK_USDT" => 100,
        "STRKUSDT" | "STRK_USDT" => 100,
        "TSLAUSDT" | "TESLA_USDT" => 100,
        "JASMYUSDT" | "JASMY_USDT" => 200,
        "LIGHTUSDT" | "LIGHT_USDT" => 50,
        "AAPLUSDT" | "AAPLSTOCK_USDT" => 100,
        "BTCUSDT" | "BTC_USDT" => 125,
        "ETHUSDT" | "ETH_USDT" => 100,
        _ => 50,
    }
}

pub fn to_mexc_symbol(sym: &str) -> String {
    // Special cases для токенів з різними іменами на біржах
    match sym {
        "NVDAUSDT" => return "NVIDIA_USDT".to_string(),
        "XAGUSDT" => return "SILVER_USDT".to_string(),
        "TRUMPUSDT" => return "TRUMPOFFICIAL_USDT".to_string(),
        "PUMPUSDT" => return "PUMPFUN_USDT".to_string(),
        "MUUSDT" => return "MUSTOCK_USDT".to_string(),
        "SNDKUSDT" => return "SNDKSTOCK_USDT".to_string(),
        "INTCUSDT" => return "INTCSTOCK_USDT".to_string(),
        "MSTRUSDT" => return "MSTRSTOCK_USDT".to_string(),
        "TSLAUSDT" => return "TESLA_USDT".to_string(),
        "AAPLUSDT" => return "AAPLSTOCK_USDT".to_string(),
        _ => {}
    }
    if sym.contains('_') { sym.to_string() }
    else { sym.replace("USDT", "_USDT") }
}
