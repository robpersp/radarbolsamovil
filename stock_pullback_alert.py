from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    import requests
except ImportError:
    requests = None

try:
    import yfinance as yf
except ImportError:
    yf = None

try:
    MADRID_TZ = ZoneInfo("Europe/Madrid")
except ZoneInfoNotFoundError:
    MADRID_TZ = dt.timezone(dt.timedelta(hours=1), name="Europe/Madrid")
MADRID_MARKET_OPEN = dt.time(9, 0)
MADRID_MARKET_CLOSE = dt.time(17, 30)
INTRADAY_INTERVAL = "1m"
INTRADAY_PERIOD = "7d"
BME_LISTED_COMPANIES_URL = "https://apiweb.bolsasymercados.es/Market/v1/EQ/ListedCompanies"
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search"
MADRID_CONTINUO_CACHE_MAX_AGE_SECONDS = 3 * 24 * 60 * 60


DEFAULT_MADRID_TICKERS = [
    "ACS.MC",
    "AENA.MC",
    "BBVA.MC",
    "CABK.MC",
    "FER.MC",
    "IBE.MC",
    "ITX.MC",
    "REP.MC",
    "SAN.MC",
    "TEF.MC",
]

PREMIUM_MADRID_TICKERS = [
    "ACS.MC",
    "AENA.MC",
    "ANA.MC",
    "CLNX.MC",
    "FER.MC",
    "IBE.MC",
    "IDR.MC",
    "ITX.MC",
    "LOG.MC",
    "REP.MC",
    "ROVI.MC",
    "VIS.MC",
]

TICKER_FULL_NAMES = {
    "ACS.MC": "ACS, Actividades de Construccion y Servicios, S.A.",
    "AENA.MC": "Aena S.M.E., S.A.",
    "BBVA.MC": "Banco Bilbao Vizcaya Argentaria, S.A.",
    "CABK.MC": "CaixaBank, S.A.",
    "FER.MC": "Ferrovial SE",
    "IBE.MC": "Iberdrola, S.A.",
    "IDR.MC": "Indra Sistemas, S.A.",
    "ITX.MC": "Industria de Diseno Textil, S.A. (Inditex)",
    "LOG.MC": "Logista Integral, S.A.",
    "REP.MC": "Repsol, S.A.",
    "ROVI.MC": "Laboratorios Farmaceuticos Rovi, S.A.",
    "SAN.MC": "Banco Santander, S.A.",
    "TEF.MC": "Telefonica, S.A.",
    "VIS.MC": "Viscofan, S.A.",
    "ANA.MC": "Acciona, S.A.",
    "CLNX.MC": "Cellnex Telecom, S.A.",
}

MANUAL_MADRID_TICKERS_BY_ISIN = {
    "NL0000235190": "AIR.MC",
    "ES0105375002": "EAT.MC",
    "LU0569974404": "APAM.MC",
    "LU1598757687": "MTS.MC",
    "AU000000BKY0": "BKY.MC",
    "ES0158300410": "CLEO.MC",
    "GB00BDCPN049": "CCEP.MC",
    "NL0015001FS8": "FER.MC",
    "ES0143421073": "ISE.MC",
    "ES0177542018": "IAG.MC",
    "ES0182280018": "UBS.MC",
}

_RUNTIME_STORAGE_DIR = Path(sys.executable).resolve().parent if getattr(sys, "frozen", False) else Path(__file__).resolve().parent
MADRID_CONTINUO_CACHE_PATH = _RUNTIME_STORAGE_DIR / "madrid_continuo_tickers_cache.json"
_MADRID_CONTINUO_TICKERS_CACHE: Optional[List[str]] = None
_MADRID_CONTINUO_NAME_CACHE: Dict[str, str] = {}

IMPACT_NEWS_KEYWORDS = [
    "profit warning",
    "guidance cut",
    "guidance raise",
    "earnings beat",
    "earnings miss",
    "resultado",
    "beneficio",
    "perdida",
    "revenue warning",
    "ingresos",
    "dividendo",
    "dividend",
    "opa",
    "takeover",
    "merger",
    "acquisition",
    "ampliacion de capital",
    "capital increase",
    "deuda",
    "downgrade",
    "upgrade",
    "sec investigation",
    "investigacion",
    "fraud",
    "bankruptcy",
    "concurso de acreedores",
    "suspension de cotizacion",
    "regulator",
    "regulatorio",
    "guidance",
]


@dataclass
class AnalysisResult:
    ticker: str
    company_name: str
    uptrend: bool
    price_filter_ok: bool
    pullback: bool
    breakout: bool
    rebound: bool
    current_price: float
    drawdown_pct: float
    recent_high: float
    resistance_level: float
    recent_low: float
    rebound_pct: float
    sma50: float
    sma200: float
    ma_gap_pct: float
    price_vs_sma50_pct: float
    sma50_slope_20d_pct: float
    reason: str
    volume_filter_ok: bool = False
    latest_daily_volume: float = 0.0
    daily_volume_sma20: float = 0.0
    daily_volume_ratio: float = 0.0
    current_intraday_volume: float = 0.0
    intraday_volume_sma20: float = 0.0
    intraday_volume_ratio: float = 0.0
    daily_prev_close: float = 0.0
    daily_change_pct_live: float = 0.0
    daily_20d_high: float = 0.0
    daily_20d_low: float = 0.0
    daily_range_position_pct: float = 0.0
    daily_bias_label: str = "neutro"
    return_5d_pct: float = 0.0
    return_20d_pct: float = 0.0
    return_60d_pct: float = 0.0
    benchmark_return_pct: float = 0.0
    relative_strength_pct: float = 0.0
    relative_strength_label: str = "sin comparar"
    average_traded_value_eur: float = 0.0
    liquidity_filter_ok: bool = False
    liquidity_label: str = "desconocida"
    bid_price: float = 0.0
    ask_price: float = 0.0
    spread_pct: float = 0.0
    spread_known: bool = False
    spread_filter_ok: bool = False
    chart_points: List[float] = field(default_factory=list)


def parse_tickers(raw: str) -> List[str]:
    tickers = [t.strip().upper() for t in raw.split(",") if t.strip()]
    return tickers


def _fetch_json_url(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if requests is not None:
        response = requests.get(
            url,
            params=params,
            timeout=25,
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
        )
        response.raise_for_status()
        return response.json()

    query = urllib.parse.urlencode(params)
    request = urllib.request.Request(
        f"{url}?{query}",
        headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
    )
    context = None
    try:
        import ssl
        context = ssl.create_default_context()
    except Exception:
        context = None
    with urllib.request.urlopen(request, timeout=25, context=context) as response:
        return json.load(response)


def _best_company_name(*names: object) -> str:
    best = ""
    for raw_name in names:
        candidate = str(raw_name or "").strip()
        if candidate and len(candidate) > len(best):
            best = candidate
    return best


def _load_cached_madrid_continuo_tickers(allow_stale: bool = False) -> Optional[List[str]]:
    global _MADRID_CONTINUO_TICKERS_CACHE, _MADRID_CONTINUO_NAME_CACHE
    if _MADRID_CONTINUO_TICKERS_CACHE:
        return _MADRID_CONTINUO_TICKERS_CACHE[:]

    try:
        if not MADRID_CONTINUO_CACHE_PATH.exists():
            return None
        payload = json.loads(MADRID_CONTINUO_CACHE_PATH.read_text(encoding="utf-8"))
        created_at = float(payload.get("created_at", 0.0))
        tickers = [normalize_madrid_ticker(str(t)) for t in payload.get("tickers", []) if str(t).strip()]
        names_payload = payload.get("names", {})
        if not tickers:
            return None
        is_fresh = (time.time() - created_at) <= MADRID_CONTINUO_CACHE_MAX_AGE_SECONDS
        if is_fresh or allow_stale:
            if isinstance(names_payload, dict):
                loaded_names = {}
                for raw_ticker, raw_name in names_payload.items():
                    ticker = normalize_madrid_ticker(str(raw_ticker))
                    name = str(raw_name or "").strip()
                    if ticker and name:
                        loaded_names[ticker] = name
                _MADRID_CONTINUO_NAME_CACHE = loaded_names
            _MADRID_CONTINUO_TICKERS_CACHE = tickers[:]
            return tickers
    except Exception:
        return None
    return None


def _save_madrid_continuo_tickers_cache(tickers: List[str], names: Optional[Dict[str, str]] = None) -> None:
    global _MADRID_CONTINUO_TICKERS_CACHE, _MADRID_CONTINUO_NAME_CACHE
    unique = []
    seen = set()
    for ticker in tickers:
        normalized = normalize_madrid_ticker(ticker)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique.append(normalized)
    clean_names: Dict[str, str] = {}
    for ticker, name in (names or {}).items():
        normalized = normalize_madrid_ticker(ticker)
        clean_name = str(name or "").strip()
        if normalized and clean_name:
            clean_names[normalized] = clean_name
    _MADRID_CONTINUO_TICKERS_CACHE = unique[:]
    if clean_names:
        _MADRID_CONTINUO_NAME_CACHE.update(clean_names)
    try:
        MADRID_CONTINUO_CACHE_PATH.write_text(
            json.dumps({"created_at": time.time(), "tickers": unique, "names": clean_names}, ensure_ascii=True, indent=2),
            encoding="utf-8",
        )
    except Exception:
        pass


def _resolve_madrid_yahoo_symbol(item: Dict[str, Any]) -> str:
    isin = str(item.get("isin", "")).strip().upper()
    if isin in MANUAL_MADRID_TICKERS_BY_ISIN:
        return MANUAL_MADRID_TICKERS_BY_ISIN[isin]

    queries = [
        isin,
        str(item.get("shareName", "")).strip(),
        str(item.get("name", "")).split(",")[0].strip(),
    ]
    seen_queries = set()
    for query in queries:
        query = str(query or "").strip()
        if not query or query in seen_queries:
            continue
        seen_queries.add(query)
        try:
            payload = _fetch_json_url(
                YAHOO_SEARCH_URL,
                {"q": query, "quotesCount": 10, "newsCount": 0},
            )
        except Exception:
            continue
        for quote in payload.get("quotes", []):
            symbol = str(quote.get("symbol", "")).strip().upper()
            if symbol.endswith(".MC"):
                return symbol
    return ""


def fetch_madrid_continuo_tickers(force_refresh: bool = False) -> List[str]:
    if not force_refresh:
        cached = _load_cached_madrid_continuo_tickers()
        if cached and _MADRID_CONTINUO_NAME_CACHE:
            return cached

    stale_cache = _load_cached_madrid_continuo_tickers(allow_stale=True)
    try:
        payload = _fetch_json_url(
            BME_LISTED_COMPANIES_URL,
            {
                "ISIN": "",
                "sectorKey": "",
                "subsectorKey": "",
                "tradingSystem": "SIBE",
                "page": 0,
                "pageSize": 0,
            },
        )
        tickers: List[str] = []
        names: Dict[str, str] = {}
        seen = set()
        for item in payload.get("data", []):
            if str(item.get("tradingSystem", "")).upper() != "SIBE":
                continue
            symbol = _resolve_madrid_yahoo_symbol(item)
            if symbol and symbol not in seen:
                seen.add(symbol)
                tickers.append(symbol)
                full_name = _best_company_name(item.get("name"), item.get("shareName"), TICKER_FULL_NAMES.get(symbol, ""))
                if full_name:
                    names[symbol] = full_name
        if tickers:
            tickers.sort()
            _save_madrid_continuo_tickers_cache(tickers, names)
            return tickers
    except Exception:
        pass

    if stale_cache:
        return stale_cache
    return DEFAULT_MADRID_TICKERS[:]


def resolve_tickers(market: str, raw_tickers: List[str]) -> List[str]:
    if market == "madrid-continuo":
        return fetch_madrid_continuo_tickers()
    if not raw_tickers:
        raise ValueError("En --market custom debes indicar --tickers.")
    return raw_tickers


def normalize_madrid_ticker(ticker: str) -> str:
    t = ticker.strip().upper()
    if not t:
        return t
    if not t.endswith(".MC"):
        t = f"{t}.MC"
    return t


def company_name_for_ticker(ticker: str) -> str:
    raw = str(ticker or "").strip().upper()
    normalized = normalize_madrid_ticker(raw) if raw.endswith(".MC") or "." not in raw else raw
    fallback = raw.replace(".MC", "").strip().upper()
    dynamic_name = _MADRID_CONTINUO_NAME_CACHE.get(normalized, "").strip() if normalized.endswith(".MC") else ""
    static_name = TICKER_FULL_NAMES.get(normalized, "").strip()
    best_name = _best_company_name(dynamic_name, static_name)
    return best_name or fallback


def ticker_label(ticker: str) -> str:
    return f"{ticker} ({company_name_for_ticker(ticker)})"


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def now_madrid() -> dt.datetime:
    return now_utc().astimezone(MADRID_TZ)


def is_madrid_market_open(now_local: dt.datetime) -> bool:
    if now_local.weekday() >= 5:
        return False
    current_time = now_local.time()
    return MADRID_MARKET_OPEN <= current_time < MADRID_MARKET_CLOSE


def seconds_until_next_interval(now_local: dt.datetime, interval_minutes: int) -> int:
    seconds_now = now_local.minute * 60 + now_local.second
    interval_seconds = interval_minutes * 60
    remainder = seconds_now % interval_seconds
    if remainder == 0:
        return 1
    return interval_seconds - remainder


def safe_float(value) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def ensure_yfinance_available() -> None:
    if yf is None:
        raise RuntimeError(
            "yfinance no esta instalado. El script CLI necesita esa dependencia para funcionar."
        )


def parse_any_datetime(value: Any) -> Optional[dt.datetime]:
    if value is None:
        return None
    if isinstance(value, (list, tuple)) and len(value) > 0:
        return parse_any_datetime(value[0])
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=dt.timezone.utc)
        return value.astimezone(dt.timezone.utc)
    if isinstance(value, dt.date):
        return dt.datetime.combine(value, dt.time(0, 0), tzinfo=dt.timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
        except (TypeError, ValueError, OSError):
            return None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            parsed = dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        except ValueError:
            return None
    return None


def extract_calendar_field(calendar_obj: Any, candidate_keys: List[str]) -> Any:
    if calendar_obj is None:
        return None
    if isinstance(calendar_obj, dict):
        for key in candidate_keys:
            if key in calendar_obj:
                return calendar_obj.get(key)
        return None

    index = getattr(calendar_obj, "index", None)
    loc = getattr(calendar_obj, "loc", None)
    if index is not None and loc is not None:
        for key in candidate_keys:
            try:
                if key in index:
                    row = loc[key]
                    if hasattr(row, "iloc"):
                        return row.iloc[0]
                    return row
            except Exception:
                continue
    return None


def get_upcoming_dividend_event(
    ticker_obj: yf.Ticker,
    ticker: str,
    now_local: dt.datetime,
    horizon_days: int,
) -> Optional[Dict[str, Any]]:
    try:
        calendar_obj = ticker_obj.calendar
    except Exception:
        return None

    ex_div_raw = extract_calendar_field(calendar_obj, ["Ex-Dividend Date", "Ex Dividend Date"])
    pay_raw = extract_calendar_field(calendar_obj, ["Dividend Date", "Payment Date"])

    ex_div_dt = parse_any_datetime(ex_div_raw)
    pay_dt = parse_any_datetime(pay_raw)

    candidates: List[Tuple[str, dt.datetime]] = []
    if ex_div_dt is not None:
        candidates.append(("ex-dividendo", ex_div_dt.astimezone(MADRID_TZ)))
    if pay_dt is not None:
        candidates.append(("pago", pay_dt.astimezone(MADRID_TZ)))
    if not candidates:
        return None

    today = now_local.date()
    future_candidates = [(kind, when) for kind, when in candidates if when.date() >= today]
    if not future_candidates:
        return None

    kind, event_dt = sorted(future_candidates, key=lambda x: x[1])[0]
    days_to = (event_dt.date() - today).days
    if days_to > horizon_days:
        return None

    event_key = f"dividend|{ticker}|{kind}|{event_dt.date().isoformat()}"
    return {
        "kind": "dividend",
        "event_key": event_key,
        "dividend_kind": kind,
        "event_dt": event_dt,
        "days_to": days_to,
    }


def extract_news_url(item: Dict[str, Any]) -> str:
    url = item.get("link")
    if isinstance(url, str) and url:
        return url
    canonical = item.get("canonicalUrl")
    if isinstance(canonical, dict):
        raw = canonical.get("url")
        if isinstance(raw, str):
            return raw
    return ""


def headline_impact_keyword(title: str) -> Optional[str]:
    title_l = title.lower()
    for kw in IMPACT_NEWS_KEYWORDS:
        if kw in title_l:
            return kw
    return None


def get_high_impact_news_events(
    ticker_obj: yf.Ticker,
    ticker: str,
    now_utc_value: dt.datetime,
    lookback_hours: int,
    max_items: int,
) -> List[Dict[str, Any]]:
    try:
        items = ticker_obj.news or []
    except Exception:
        return []

    if not isinstance(items, list):
        return []

    cutoff = now_utc_value - dt.timedelta(hours=lookback_hours)
    events: List[Dict[str, Any]] = []

    for item in items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        if not title:
            continue

        kw = headline_impact_keyword(title)
        if kw is None:
            continue

        published_dt = parse_any_datetime(item.get("providerPublishTime"))
        if published_dt is None:
            published_dt = parse_any_datetime(item.get("pubDate"))
        if published_dt is not None and published_dt < cutoff:
            continue

        url = extract_news_url(item)
        uid_raw = item.get("uuid")
        if isinstance(uid_raw, str) and uid_raw:
            uid = uid_raw
        elif url:
            uid = url
        else:
            ts = int(published_dt.timestamp()) if published_dt is not None else 0
            uid = f"{title}|{ts}"

        event_key = f"news|{ticker}|{uid}"
        events.append(
            {
                "kind": "news",
                "event_key": event_key,
                "title": title,
                "keyword": kw,
                "published_dt": published_dt,
                "url": url,
            }
        )

    def sort_key(x: Dict[str, Any]) -> float:
        published_dt = x.get("published_dt")
        if isinstance(published_dt, dt.datetime):
            return published_dt.timestamp()
        return 0.0

    events.sort(key=sort_key, reverse=True)
    return events[:max_items]


def send_telegram_message(message: str) -> bool:
    token = None
    chat_id = None
    try:
        with open("telegram_config.json", "r", encoding="utf-8") as f:
            cfg = json.load(f)
            token = cfg.get("bot_token")
            chat_id = cfg.get("chat_id")
    except FileNotFoundError:
        token = None
        chat_id = None
    except Exception:
        return False

    if not token or not chat_id:
        return False

    params = urllib.parse.urlencode({"chat_id": chat_id, "text": message})
    url = f"https://api.telegram.org/bot{token}/sendMessage?{params}"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            return response.status == 200
    except Exception:
        return False


def analyze_ticker(
    ticker: str,
    min_pullback_pct: float,
    max_pullback_pct: float,
    lookback_bars: int,
    resistance_lookback_bars: int,
    min_breakout_buffer_pct: float,
    rebound_lookback_bars: int,
    rebound_recent_bars: int,
    min_rebound_pct: float,
    max_price_eur: float,
    min_ma_gap_pct: float,
    min_price_above_sma50_pct: float,
    min_sma50_slope_20d_pct: float,
    min_daily_volume_ratio: float,
    min_intraday_volume_ratio: float,
) -> AnalysisResult:
    ensure_yfinance_available()
    daily = yf.Ticker(ticker).history(period="1y", interval="1d", auto_adjust=True)
    if daily.empty or len(daily) < 210:
        return AnalysisResult(
            ticker=ticker,
            company_name=company_name_for_ticker(ticker),
            uptrend=False,
            price_filter_ok=False,
            pullback=False,
            breakout=False,
            rebound=False,
            current_price=0.0,
            drawdown_pct=0.0,
            recent_high=0.0,
            resistance_level=0.0,
            recent_low=0.0,
            rebound_pct=0.0,
            sma50=0.0,
            sma200=0.0,
            ma_gap_pct=0.0,
            price_vs_sma50_pct=0.0,
            sma50_slope_20d_pct=0.0,
            reason="No hay datos diarios suficientes para calcular SMA50/SMA200.",
        )

    daily_close = daily["Close"]
    daily_high = daily["High"]
    daily_low = daily["Low"]
    sma50 = safe_float(daily_close.rolling(50).mean().iloc[-1])
    sma200 = safe_float(daily_close.rolling(200).mean().iloc[-1])
    sma50_prev20 = safe_float(daily_close.rolling(50).mean().iloc[-20])
    last_daily_close = safe_float(daily_close.iloc[-1])
    prev_daily_close = safe_float(daily_close.iloc[-2]) if len(daily_close) >= 2 else last_daily_close
    if prev_daily_close is None:
        prev_daily_close = last_daily_close
    daily_20d_high = safe_float(daily_high.tail(20).max()) or 0.0
    daily_20d_low = safe_float(daily_low.tail(20).min()) or 0.0
    daily_volume = daily["Volume"]
    latest_daily_volume = safe_float(daily_volume.iloc[-1]) or 0.0
    daily_volume_sma20 = safe_float(daily_volume.rolling(20).mean().iloc[-1]) or 0.0
    daily_volume_ratio = (
        (latest_daily_volume / daily_volume_sma20) if daily_volume_sma20 > 0 else 0.0
    )
    daily_volume_ok = daily_volume_ratio >= min_daily_volume_ratio

    if None in (sma50, sma200, sma50_prev20, last_daily_close):
        return AnalysisResult(
            ticker=ticker,
            company_name=company_name_for_ticker(ticker),
            uptrend=False,
            price_filter_ok=False,
            pullback=False,
            breakout=False,
            rebound=False,
            current_price=0.0,
            drawdown_pct=0.0,
            recent_high=0.0,
            resistance_level=0.0,
            recent_low=0.0,
            rebound_pct=0.0,
            sma50=0.0,
            sma200=0.0,
            ma_gap_pct=0.0,
            price_vs_sma50_pct=0.0,
            sma50_slope_20d_pct=0.0,
            reason="No se pudieron calcular medias moviles.",
        )

    ma_gap_pct = ((sma50 - sma200) / sma200) * 100.0 if sma200 > 0 else 0.0
    price_vs_sma50_pct = ((last_daily_close - sma50) / sma50) * 100.0 if sma50 > 0 else 0.0
    sma50_slope_20d_pct = (
        ((sma50 - sma50_prev20) / sma50_prev20) * 100.0 if sma50_prev20 > 0 else 0.0
    )

    uptrend = (
        last_daily_close > sma50
        and sma50 > sma200
        and ma_gap_pct >= min_ma_gap_pct
        and price_vs_sma50_pct >= min_price_above_sma50_pct
        and sma50_slope_20d_pct >= min_sma50_slope_20d_pct
    )
    daily_change_pct_live = (
        ((last_daily_close - prev_daily_close) / prev_daily_close) * 100.0
        if prev_daily_close and prev_daily_close > 0
        else 0.0
    )
    daily_range_span = daily_20d_high - daily_20d_low
    daily_range_position_pct = (
        ((last_daily_close - daily_20d_low) / daily_range_span) * 100.0
        if daily_range_span > 0
        else 50.0
    )
    if uptrend and daily_range_position_pct >= 60.0:
        daily_bias_label = "alcista fuerte"
    elif uptrend:
        daily_bias_label = "alcista"
    elif last_daily_close > sma200:
        daily_bias_label = "neutra-alcista"
    else:
        daily_bias_label = "neutra-bajista"

    intraday = yf.Ticker(ticker).history(
        period=INTRADAY_PERIOD,
        interval=INTRADAY_INTERVAL,
        auto_adjust=True,
    )
    if intraday.empty or len(intraday) < max(lookback_bars, 20):
        price_filter_ok = last_daily_close <= max_price_eur
        return AnalysisResult(
            ticker=ticker,
            company_name=company_name_for_ticker(ticker),
            uptrend=uptrend,
            price_filter_ok=price_filter_ok,
            pullback=False,
            breakout=False,
            rebound=False,
            current_price=last_daily_close,
            drawdown_pct=0.0,
            recent_high=last_daily_close,
            resistance_level=last_daily_close,
            recent_low=last_daily_close,
            rebound_pct=0.0,
            sma50=sma50,
            sma200=sma200,
            ma_gap_pct=ma_gap_pct,
            price_vs_sma50_pct=price_vs_sma50_pct,
            sma50_slope_20d_pct=sma50_slope_20d_pct,
            volume_filter_ok=daily_volume_ok,
            latest_daily_volume=latest_daily_volume,
            daily_volume_sma20=daily_volume_sma20,
            daily_volume_ratio=daily_volume_ratio,
            daily_prev_close=prev_daily_close or 0.0,
            daily_change_pct_live=daily_change_pct_live,
            daily_20d_high=daily_20d_high,
            daily_20d_low=daily_20d_low,
            daily_range_position_pct=daily_range_position_pct,
            daily_bias_label=daily_bias_label,
            reason="Sin suficientes velas intradia para detectar pullback.",
        )

    intraday_close = intraday["Close"].tail(lookback_bars)
    intraday_volume = intraday["Volume"].tail(max(lookback_bars, 20))
    current_price = safe_float(intraday_close.iloc[-1]) or 0.0
    current_intraday_volume = safe_float(intraday_volume.iloc[-1]) or 0.0
    intraday_volume_sma20 = safe_float(intraday_volume.tail(20).mean()) or 0.0
    intraday_volume_ratio = (
        (current_intraday_volume / intraday_volume_sma20) if intraday_volume_sma20 > 0 else 0.0
    )
    intraday_volume_ok = intraday_volume_ratio >= min_intraday_volume_ratio
    recent_high = safe_float(intraday_close.max()) or 0.0
    if recent_high <= 0:
        price_filter_ok = current_price <= max_price_eur
        return AnalysisResult(
            ticker=ticker,
            company_name=company_name_for_ticker(ticker),
            uptrend=uptrend,
            price_filter_ok=price_filter_ok,
            pullback=False,
            breakout=False,
            rebound=False,
            current_price=current_price,
            drawdown_pct=0.0,
            recent_high=recent_high,
            resistance_level=recent_high,
            recent_low=current_price,
            rebound_pct=0.0,
            sma50=sma50,
            sma200=sma200,
            ma_gap_pct=ma_gap_pct,
            price_vs_sma50_pct=price_vs_sma50_pct,
            sma50_slope_20d_pct=sma50_slope_20d_pct,
            volume_filter_ok=daily_volume_ok,
            latest_daily_volume=latest_daily_volume,
            daily_volume_sma20=daily_volume_sma20,
            daily_volume_ratio=daily_volume_ratio,
            current_intraday_volume=current_intraday_volume,
            intraday_volume_sma20=intraday_volume_sma20,
            intraday_volume_ratio=intraday_volume_ratio,
            daily_prev_close=prev_daily_close or 0.0,
            daily_change_pct_live=daily_change_pct_live,
            daily_20d_high=daily_20d_high,
            daily_20d_low=daily_20d_low,
            daily_range_position_pct=daily_range_position_pct,
            daily_bias_label=daily_bias_label,
            reason="No se pudo calcular el maximo reciente.",
        )

    daily_change_pct_live = (
        ((current_price - prev_daily_close) / prev_daily_close) * 100.0
        if prev_daily_close and prev_daily_close > 0
        else 0.0
    )
    drawdown_pct = ((recent_high - current_price) / recent_high) * 100.0
    price_filter_ok = current_price <= max_price_eur
    still_structurally_bullish = current_price >= (sma50 * 0.98)
    pullback = (
        uptrend
        and price_filter_ok
        and daily_volume_ok
        and still_structurally_bullish
        and min_pullback_pct <= drawdown_pct <= max_pullback_pct
    )

    resistance_window = intraday_close.tail(max(resistance_lookback_bars + 1, 3))
    previous_resistance = safe_float(resistance_window.iloc[:-1].max()) or recent_high
    prev_close = safe_float(resistance_window.iloc[-2]) or current_price
    breakout = (
        uptrend
        and price_filter_ok
        and daily_volume_ok
        and intraday_volume_ok
        and current_price > previous_resistance * (1 + (min_breakout_buffer_pct / 100.0))
        and prev_close <= previous_resistance
    )

    rebound_window = intraday_close.tail(max(rebound_lookback_bars, 5))
    recent_low = safe_float(rebound_window.min()) or current_price
    low_pos = int(rebound_window.values.argmin())
    bars_since_low = len(rebound_window) - 1 - low_pos
    rebound_pct = ((current_price - recent_low) / recent_low) * 100.0 if recent_low > 0 else 0.0
    prev_1 = safe_float(rebound_window.iloc[-2]) or current_price
    rebound = (
        uptrend
        and price_filter_ok
        and daily_volume_ok
        and intraday_volume_ok
        and recent_low > 0
        and 1 <= bars_since_low <= rebound_recent_bars
        and rebound_pct >= min_rebound_pct
        and current_price > prev_1
    )

    if not price_filter_ok:
        reason = f"Precio actual ({current_price:.2f} EUR) por encima de {max_price_eur:.2f} EUR."
    elif not daily_volume_ok:
        reason = (
            f"Volumen diario insuficiente ({daily_volume_ratio:.2f}x < "
            f"{min_daily_volume_ratio:.2f}x de media 20d)."
        )
    elif breakout:
        reason = (
            f"Ruptura de resistencia ({previous_resistance:.2f}) con buffer "
            f"{min_breakout_buffer_pct:.2f}%."
        )
    elif rebound:
        reason = (
            f"Rebote confirmado desde minimo reciente ({recent_low:.2f}), "
            f"subiendo {rebound_pct:.2f}%."
        )
    elif pullback:
        reason = "Tendencia alcista clara y retroceso en rango."
    elif not intraday_volume_ok:
        reason = (
            f"Volumen intradia bajo ({intraday_volume_ratio:.2f}x < "
            f"{min_intraday_volume_ratio:.2f}x de media 20 velas)."
        )
    elif not uptrend:
        reason = "No cumple criterios de tendencia alcista clara."
    elif not still_structurally_bullish:
        reason = "La caida perfora demasiado la zona de SMA50."
    else:
        reason = "Retroceso fuera del rango configurado."

    return AnalysisResult(
        ticker=ticker,
        company_name=company_name_for_ticker(ticker),
        uptrend=uptrend,
        price_filter_ok=price_filter_ok,
        pullback=pullback,
        breakout=breakout,
        rebound=rebound,
        current_price=current_price,
        drawdown_pct=drawdown_pct,
        recent_high=recent_high,
        resistance_level=previous_resistance,
        recent_low=recent_low,
        rebound_pct=rebound_pct,
        sma50=sma50,
        sma200=sma200,
        ma_gap_pct=ma_gap_pct,
        price_vs_sma50_pct=price_vs_sma50_pct,
        sma50_slope_20d_pct=sma50_slope_20d_pct,
        volume_filter_ok=daily_volume_ok,
        latest_daily_volume=latest_daily_volume,
        daily_volume_sma20=daily_volume_sma20,
        daily_volume_ratio=daily_volume_ratio,
        current_intraday_volume=current_intraday_volume,
        intraday_volume_sma20=intraday_volume_sma20,
        intraday_volume_ratio=intraday_volume_ratio,
        daily_prev_close=prev_daily_close or 0.0,
        daily_change_pct_live=daily_change_pct_live,
        daily_20d_high=daily_20d_high,
        daily_20d_low=daily_20d_low,
        daily_range_position_pct=daily_range_position_pct,
        daily_bias_label=daily_bias_label,
        reason=reason,
    )


def format_alert(result: AnalysisResult, alert_kind: str) -> str:
    title_map = {
        "uptrend": "[ALERTA TENDENCIA ALCISTA]",
        "pullback": "[ALERTA PULLBACK]",
        "breakout": "[ALERTA RUPTURA]",
        "rebound": "[ALERTA REBOTE]",
    }
    title = title_map.get(alert_kind, "[ALERTA]")
    extra = ""
    if alert_kind == "breakout":
        extra = f"Resistencia rota: {result.resistance_level:.2f}\n"
    elif alert_kind == "rebound":
        extra = (
            f"Minimo reciente: {result.recent_low:.2f}\n"
            f"Rebote desde minimo: {result.rebound_pct:.2f}%\n"
        )

    return (
        f"{title} {result.ticker} - {result.company_name}\n"
        f"Precio actual: {result.current_price:.2f}\n"
        f"Sesgo diario: {result.daily_bias_label}\n"
        f"Movimiento diario (vs cierre previo {result.daily_prev_close:.2f}): "
        f"{result.daily_change_pct_live:.2f}%\n"
        f"Rango 20d: {result.daily_20d_low:.2f} - {result.daily_20d_high:.2f} "
        f"(posicion {result.daily_range_position_pct:.1f}%)\n"
        f"Maximo reciente: {result.recent_high:.2f}\n"
        f"Retroceso: {result.drawdown_pct:.2f}%\n"
        f"{extra}"
        f"SMA50: {result.sma50:.2f} | SMA200: {result.sma200:.2f}\n"
        f"Gap SMA50>SMA200: {result.ma_gap_pct:.2f}% | Precio>SMA50: {result.price_vs_sma50_pct:.2f}%\n"
        f"Pendiente SMA50 (20d): {result.sma50_slope_20d_pct:.2f}%\n"
        f"Vol diario: {result.latest_daily_volume:.0f} | SMA20: {result.daily_volume_sma20:.0f} "
        f"| Ratio: {result.daily_volume_ratio:.2f}x\n"
        f"Vol 1m: {result.current_intraday_volume:.0f} | SMA20: {result.intraday_volume_sma20:.0f} "
        f"| Ratio: {result.intraday_volume_ratio:.2f}x\n"
        f"Motivo: {result.reason}"
    )


def format_dividend_alert(
    ticker: str,
    company_name: str,
    dividend_kind: str,
    event_dt: dt.datetime,
    days_to: int,
) -> str:
    date_text = event_dt.strftime("%Y-%m-%d")
    return (
        f"[ALERTA DIVIDENDO] {ticker} - {company_name}\n"
        f"Evento: {dividend_kind}\n"
        f"Fecha: {date_text} ({days_to} dias)\n"
        "Motivo: Proximo evento de dividendo detectado."
    )


def format_news_alert(
    ticker: str,
    company_name: str,
    title: str,
    keyword: str,
    published_dt: Optional[dt.datetime],
    url: str,
) -> str:
    if published_dt is None:
        published_text = "desconocida"
    else:
        published_text = published_dt.astimezone(MADRID_TZ).strftime("%Y-%m-%d %H:%M:%S %Z")
    extra_url = f"\nEnlace: {url}" if url else ""
    return (
        f"[ALERTA NOTICIA IMPACTO] {ticker} - {company_name}\n"
        f"Keyword: {keyword}\n"
        f"Publicada: {published_text}\n"
        f"Titular: {title}"
        f"{extra_url}"
    )


def allowed_alert_kinds(alert_mode: str) -> Set[str]:
    if alert_mode == "both":
        return {"uptrend", "pullback"}
    if alert_mode == "events":
        return {"dividend", "news"}
    if alert_mode == "all":
        return {"uptrend", "pullback", "breakout", "rebound", "dividend", "news"}
    return {alert_mode}


def active_signal_kinds(result: AnalysisResult) -> List[str]:
    active_kinds: List[str] = []
    if result.breakout:
        active_kinds.append("breakout")
    if result.rebound:
        active_kinds.append("rebound")
    if result.pullback:
        active_kinds.append("pullback")
    if result.uptrend and result.price_filter_ok and result.volume_filter_ok:
        active_kinds.append("uptrend")
    return active_kinds


def pick_signal_kind(result: AnalysisResult, alert_mode: str) -> Optional[str]:
    allowed = allowed_alert_kinds(alert_mode)
    priority = ["breakout", "rebound", "pullback", "uptrend"]
    active_kinds = active_signal_kinds(result)
    return next((kind for kind in priority if kind in active_kinds and kind in allowed), None)


def signal_kind_label(signal_kind: Optional[str]) -> str:
    label_map = {
        "uptrend": "Tendencia",
        "pullback": "Pullback",
        "breakout": "Ruptura",
        "rebound": "Rebote",
        "dividend": "Dividendo",
        "news": "Noticia",
    }
    if signal_kind is None:
        return "Sin senal"
    return label_map.get(signal_kind, signal_kind)


def run_cycle(
    tickers: List[str],
    alert_mode: str,
    min_pullback_pct: float,
    max_pullback_pct: float,
    lookback_bars: int,
    resistance_lookback_bars: int,
    min_breakout_buffer_pct: float,
    rebound_lookback_bars: int,
    rebound_recent_bars: int,
    min_rebound_pct: float,
    max_price_eur: float,
    min_ma_gap_pct: float,
    min_price_above_sma50_pct: float,
    min_sma50_slope_20d_pct: float,
    min_daily_volume_ratio: float,
    min_intraday_volume_ratio: float,
    enable_dividend_alerts: bool,
    dividend_horizon_days: int,
    enable_news_alerts: bool,
    news_lookback_hours: int,
    max_news_items_per_ticker: int,
    events_ignore_price_filter: bool,
    cooldown_minutes: int,
    last_alerts: Dict[str, dt.datetime],
    seen_event_keys: Set[str],
    verbose: bool,
) -> Tuple[int, int]:
    ensure_yfinance_available()
    checked = 0
    alerts = 0
    now = now_utc()
    allowed = allowed_alert_kinds(alert_mode)

    for ticker in tickers:
        checked += 1
        try:
            result = analyze_ticker(
                ticker=ticker,
                min_pullback_pct=min_pullback_pct,
                max_pullback_pct=max_pullback_pct,
                lookback_bars=lookback_bars,
                resistance_lookback_bars=resistance_lookback_bars,
                min_breakout_buffer_pct=min_breakout_buffer_pct,
                rebound_lookback_bars=rebound_lookback_bars,
                rebound_recent_bars=rebound_recent_bars,
                min_rebound_pct=min_rebound_pct,
                max_price_eur=max_price_eur,
                min_ma_gap_pct=min_ma_gap_pct,
                min_price_above_sma50_pct=min_price_above_sma50_pct,
                min_sma50_slope_20d_pct=min_sma50_slope_20d_pct,
                min_daily_volume_ratio=min_daily_volume_ratio,
                min_intraday_volume_ratio=min_intraday_volume_ratio,
            )
        except Exception as exc:
            print(f"[{ticker_label(ticker)}] Error durante analisis: {exc}")
            continue

        if verbose:
            print(
                f"[{ticker_label(ticker)}] uptrend={result.uptrend} price_ok={result.price_filter_ok} "
                f"pullback={result.pullback} breakout={result.breakout} rebound={result.rebound} "
                f"drawdown={result.drawdown_pct:.2f}% precio={result.current_price:.2f} "
                f"gap={result.ma_gap_pct:.2f}% p>sma50={result.price_vs_sma50_pct:.2f}% "
                f"sma50_20d={result.sma50_slope_20d_pct:.2f}% "
                f"volD={result.daily_volume_ratio:.2f}x vol1m={result.intraday_volume_ratio:.2f}x "
                f"biasD={result.daily_bias_label} day={result.daily_change_pct_live:.2f}% "
                f"motivo='{result.reason}'"
            )

        signal_kind = pick_signal_kind(result, alert_mode)
        if signal_kind is not None:
            cooldown_key = f"{ticker}|{signal_kind}"
            last_sent = last_alerts.get(cooldown_key)
            if last_sent is not None:
                delta_minutes = (now - last_sent).total_seconds() / 60.0
                if delta_minutes < cooldown_minutes:
                    if verbose:
                        print(
                            f"[{ticker_label(ticker)}] Senal {signal_kind} detectada, pero en cooldown "
                            f"({delta_minutes:.1f}/{cooldown_minutes} min)."
                        )
                else:
                    alert_msg = format_alert(result, signal_kind)
                    stamp = now.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                    print(f"\n{stamp}\n{alert_msg}\n")
                    telegram_ok = send_telegram_message(alert_msg)
                    if verbose:
                        status = "enviado" if telegram_ok else "no enviado"
                        print(f"[{ticker_label(ticker)}] Telegram: {status}")
                    last_alerts[cooldown_key] = now
                    alerts += 1
            else:
                alert_msg = format_alert(result, signal_kind)
                stamp = now.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                print(f"\n{stamp}\n{alert_msg}\n")
                telegram_ok = send_telegram_message(alert_msg)
                if verbose:
                    status = "enviado" if telegram_ok else "no enviado"
                    print(f"[{ticker_label(ticker)}] Telegram: {status}")
                last_alerts[cooldown_key] = now
                alerts += 1

        events_price_ok = events_ignore_price_filter or result.current_price <= max_price_eur
        if not events_price_ok and verbose and ("dividend" in allowed or "news" in allowed):
            print(
                f"[{ticker_label(ticker)}] Eventos omitidos por filtro de precio "
                f"({result.current_price:.2f} > {max_price_eur:.2f})."
            )

        if events_price_ok and enable_dividend_alerts and "dividend" in allowed:
            dividend_event = get_upcoming_dividend_event(
                ticker_obj=yf.Ticker(ticker),
                ticker=ticker,
                now_local=now.astimezone(MADRID_TZ),
                horizon_days=dividend_horizon_days,
            )
            if dividend_event is not None:
                event_key = str(dividend_event["event_key"])
                if event_key not in seen_event_keys:
                    alert_msg = format_dividend_alert(
                        ticker=ticker,
                        company_name=company_name_for_ticker(ticker),
                        dividend_kind=str(dividend_event["dividend_kind"]),
                        event_dt=dividend_event["event_dt"],
                        days_to=int(dividend_event["days_to"]),
                    )
                    stamp = now.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                    print(f"\n{stamp}\n{alert_msg}\n")
                    telegram_ok = send_telegram_message(alert_msg)
                    if verbose:
                        status = "enviado" if telegram_ok else "no enviado"
                        print(f"[{ticker_label(ticker)}] Telegram: {status}")
                    seen_event_keys.add(event_key)
                    alerts += 1

        if events_price_ok and enable_news_alerts and "news" in allowed:
            news_events = get_high_impact_news_events(
                ticker_obj=yf.Ticker(ticker),
                ticker=ticker,
                now_utc_value=now,
                lookback_hours=news_lookback_hours,
                max_items=max_news_items_per_ticker,
            )
            sent_news = 0
            for event in news_events:
                event_key = str(event["event_key"])
                if event_key in seen_event_keys:
                    continue
                alert_msg = format_news_alert(
                    ticker=ticker,
                    company_name=company_name_for_ticker(ticker),
                    title=str(event["title"]),
                    keyword=str(event["keyword"]),
                    published_dt=event.get("published_dt"),
                    url=str(event.get("url", "")),
                )
                stamp = now.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
                print(f"\n{stamp}\n{alert_msg}\n")
                telegram_ok = send_telegram_message(alert_msg)
                if verbose:
                    status = "enviado" if telegram_ok else "no enviado"
                    print(f"[{ticker_label(ticker)}] Telegram: {status}")
                seen_event_keys.add(event_key)
                alerts += 1
                sent_news += 1
                if sent_news >= max_news_items_per_ticker:
                    break

    return checked, alerts


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Detecta tendencia alcista, pullbacks, rupturas, rebotes, proximos dividendos "
            "y noticias de impacto con datos de Yahoo Finance."
        )
    )
    parser.add_argument(
        "--tickers",
        default="",
        help=(
            "Lista de tickers separados por coma. En modo madrid-continuo puedes usar "
            "simbolos con o sin .MC, por ejemplo: SAN,BBVA,ITX."
        ),
    )
    parser.add_argument(
        "--market",
        choices=["madrid-continuo", "custom"],
        default="madrid-continuo",
        help=(
            "Universo de seguimiento. 'madrid-continuo' usa valores de Bolsa de Madrid; "
            "'custom' usa exactamente los tickers indicados en --tickers."
        ),
    )
    parser.add_argument(
        "--interval-minutes",
        type=int,
        default=1,
        help="Cada cuantos minutos repetir el analisis (default: 1).",
    )
    parser.add_argument(
        "--alert-mode",
        choices=[
            "uptrend",
            "pullback",
            "breakout",
            "rebound",
            "dividend",
            "news",
            "events",
            "both",
            "all",
        ],
        default="all",
        help=(
            "Tipo de alerta: uptrend, pullback, breakout, rebound, dividend, news, "
            "events (dividend+news), both (uptrend+pullback) o all (todas) "
            "(default: all)."
        ),
    )
    parser.add_argument(
        "--disable-dividend-alerts",
        action="store_true",
        help="Desactiva alertas de dividendos proximos.",
    )
    parser.add_argument(
        "--dividend-horizon-days",
        type=int,
        default=30,
        help="Dias maximos hacia adelante para alertar dividendos (default: 30).",
    )
    parser.add_argument(
        "--disable-news-alerts",
        action="store_true",
        help="Desactiva alertas de noticias de impacto.",
    )
    parser.add_argument(
        "--news-lookback-hours",
        type=int,
        default=24,
        help="Ventana de horas para considerar noticias recientes (default: 24).",
    )
    parser.add_argument(
        "--max-news-items-per-ticker",
        type=int,
        default=2,
        help="Maximo de noticias de impacto por ticker y ciclo (default: 2).",
    )
    parser.add_argument(
        "--events-ignore-price-filter",
        action="store_true",
        help=(
            "Si se indica, dividendos/noticias no aplican el filtro de precio <= max-price-eur."
        ),
    )
    parser.add_argument(
        "--run-outside-market-hours",
        action="store_true",
        help=(
            "Si se indica, analiza tambien fuera de horario de mercado. "
            "Por defecto solo analiza con mercado abierto."
        ),
    )
    parser.add_argument(
        "--min-pullback-pct",
        type=float,
        default=2.0,
        help="Retroceso minimo para alertar (default: 2.0).",
    )
    parser.add_argument(
        "--max-pullback-pct",
        type=float,
        default=8.0,
        help="Retroceso maximo para alertar (default: 8.0).",
    )
    parser.add_argument(
        "--max-price-eur",
        type=float,
        default=10.0,
        help="Solo acciones con precio actual <= este valor en EUR (default: 10.0).",
    )
    parser.add_argument(
        "--min-ma-gap-pct",
        type=float,
        default=2.0,
        help="Minimo % de separacion entre SMA50 y SMA200 para tendencia clara (default: 2.0).",
    )
    parser.add_argument(
        "--min-price-above-sma50-pct",
        type=float,
        default=1.0,
        help="Minimo % del precio por encima de SMA50 para tendencia clara (default: 1.0).",
    )
    parser.add_argument(
        "--min-sma50-slope-20d-pct",
        type=float,
        default=1.0,
        help="Minimo % de subida de SMA50 vs hace 20 sesiones (default: 1.0).",
    )
    parser.add_argument(
        "--min-daily-volume-ratio",
        type=float,
        default=1.05,
        help=(
            "Minimo ratio de volumen diario actual / media 20 sesiones para validar senales "
            "(default: 1.05)."
        ),
    )
    parser.add_argument(
        "--min-intraday-volume-ratio",
        type=float,
        default=1.10,
        help=(
            "Minimo ratio de volumen de vela 1m actual / media 20 velas para validar "
            "rupturas/rebotes (default: 1.10)."
        ),
    )
    parser.add_argument(
        "--lookback-bars",
        type=int,
        default=510,
        help="Numero de velas 1m para buscar maximo reciente (default: 510 ~ 1 sesion).",
    )
    parser.add_argument(
        "--resistance-lookback-bars",
        type=int,
        default=180,
        help="Velas 1m para calcular resistencia previa (default: 180).",
    )
    parser.add_argument(
        "--min-breakout-buffer-pct",
        type=float,
        default=0.10,
        help="Buffer minimo (%) por encima de resistencia para validar ruptura (default: 0.10).",
    )
    parser.add_argument(
        "--rebound-lookback-bars",
        type=int,
        default=120,
        help="Ventana de velas 1m para buscar minimo reciente del rebote (default: 120).",
    )
    parser.add_argument(
        "--rebound-recent-bars",
        type=int,
        default=30,
        help="Maximo de velas desde el minimo para considerar rebote vigente (default: 30).",
    )
    parser.add_argument(
        "--min-rebound-pct",
        type=float,
        default=0.50,
        help="Subida minima (%) desde minimo reciente para validar rebote (default: 0.50).",
    )
    parser.add_argument(
        "--cooldown-minutes",
        type=int,
        default=60,
        help="Evita alertas repetidas por ticker durante este tiempo (default: 60).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Ejecuta un solo ciclo y termina.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Muestra diagnostico de cada ticker.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    ensure_yfinance_available()

    raw_tickers = parse_tickers(args.tickers)
    try:
        tickers = resolve_tickers(args.market, raw_tickers)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    if args.interval_minutes <= 0:
        parser.error("--interval-minutes debe ser mayor que 0.")
        return 2

    if args.min_pullback_pct < 0 or args.max_pullback_pct < 0:
        parser.error("Los porcentajes de pullback no pueden ser negativos.")
        return 2

    if args.min_pullback_pct > args.max_pullback_pct:
        parser.error("--min-pullback-pct no puede ser mayor que --max-pullback-pct.")
        return 2

    if args.max_price_eur <= 0:
        parser.error("--max-price-eur debe ser mayor que 0.")
        return 2

    if args.min_daily_volume_ratio <= 0:
        parser.error("--min-daily-volume-ratio debe ser mayor que 0.")
        return 2

    if args.min_intraday_volume_ratio <= 0:
        parser.error("--min-intraday-volume-ratio debe ser mayor que 0.")
        return 2

    if args.resistance_lookback_bars < 3:
        parser.error("--resistance-lookback-bars debe ser >= 3.")
        return 2

    if args.rebound_lookback_bars < 5:
        parser.error("--rebound-lookback-bars debe ser >= 5.")
        return 2

    if args.rebound_recent_bars < 1:
        parser.error("--rebound-recent-bars debe ser >= 1.")
        return 2

    if args.dividend_horizon_days < 0:
        parser.error("--dividend-horizon-days no puede ser negativo.")
        return 2

    if args.news_lookback_hours < 1:
        parser.error("--news-lookback-hours debe ser >= 1.")
        return 2

    if args.max_news_items_per_ticker < 1:
        parser.error("--max-news-items-per-ticker debe ser >= 1.")
        return 2

    enable_dividend_alerts = not args.disable_dividend_alerts
    enable_news_alerts = not args.disable_news_alerts

    print("Iniciando monitor de tendencias...")
    print(f"Mercado: {args.market}")
    print(f"Modo alerta: {args.alert_mode}")
    print(f"Tickers: {', '.join(tickers)}")
    if args.run_outside_market_hours:
        print("Horario: analisis tambien fuera de mercado.")
    else:
        print("Horario: solo L-V 09:00-17:30 Europe/Madrid.")
    print("Seguimiento:")
    for ticker in tickers:
        print(f"- {ticker_label(ticker)}")
    print(
        "Marco temporal: ejecucion en tiempo real (1m) con sesgo y contexto principal en diario.\n"
        "Criterio tendencia clara: cierre diario > SMA50 > SMA200, con umbrales minimos de fuerza.\n"
        f"Filtro precio: <= {args.max_price_eur:.2f} EUR.\n"
        f"Umbrales: gap SMA50>SMA200 >= {args.min_ma_gap_pct:.2f}%, "
        f"precio>SMA50 >= {args.min_price_above_sma50_pct:.2f}%, "
        f"SMA50(20d) >= {args.min_sma50_slope_20d_pct:.2f}%.\n"
        f"Volumen: diario >= {args.min_daily_volume_ratio:.2f}x media 20 sesiones; "
        f"1m >= {args.min_intraday_volume_ratio:.2f}x media 20 velas.\n"
        f"Ruptura: cierre actual > resistencia previa por al menos {args.min_breakout_buffer_pct:.2f}% "
        f"(resistencia en {args.resistance_lookback_bars} velas).\n"
        f"Rebote: minimo reciente en {args.rebound_lookback_bars} velas, recuperacion >= "
        f"{args.min_rebound_pct:.2f}% dentro de {args.rebound_recent_bars} velas.\n"
        f"Dividendos: {'on' if enable_dividend_alerts else 'off'} "
        f"(horizonte {args.dividend_horizon_days} dias).\n"
        f"Noticias impacto: {'on' if enable_news_alerts else 'off'} "
        f"(lookback {args.news_lookback_hours}h, max {args.max_news_items_per_ticker}/ticker/ciclo).\n"
        f"Eventos y filtro precio: {'ignora' if args.events_ignore_price_filter else 'respeta'} <= "
        f"{args.max_price_eur:.2f} EUR.\n"
        f"Pullback (si aplica): entre {args.min_pullback_pct:.2f}% y {args.max_pullback_pct:.2f}% "
        f"desde maximo reciente ({args.lookback_bars} velas de {INTRADAY_INTERVAL})."
    )

    last_alerts: Dict[str, dt.datetime] = {}
    seen_event_keys: Set[str] = set()

    while True:
        madrid_now = now_madrid()
        market_open = is_madrid_market_open(madrid_now)
        if not args.run_outside_market_hours and not market_open:
            stamp = madrid_now.strftime("%Y-%m-%d %H:%M:%S %Z")
            print(f"[{stamp}] Mercado cerrado. Esperando siguiente ventana...")
            time.sleep(60)
            if args.once:
                break
            continue

        start = now_utc()
        checked, alerts = run_cycle(
            tickers=tickers,
            alert_mode=args.alert_mode,
            min_pullback_pct=args.min_pullback_pct,
            max_pullback_pct=args.max_pullback_pct,
            lookback_bars=args.lookback_bars,
            resistance_lookback_bars=args.resistance_lookback_bars,
            min_breakout_buffer_pct=args.min_breakout_buffer_pct,
            rebound_lookback_bars=args.rebound_lookback_bars,
            rebound_recent_bars=args.rebound_recent_bars,
            min_rebound_pct=args.min_rebound_pct,
            max_price_eur=args.max_price_eur,
            min_ma_gap_pct=args.min_ma_gap_pct,
            min_price_above_sma50_pct=args.min_price_above_sma50_pct,
            min_sma50_slope_20d_pct=args.min_sma50_slope_20d_pct,
            min_daily_volume_ratio=args.min_daily_volume_ratio,
            min_intraday_volume_ratio=args.min_intraday_volume_ratio,
            enable_dividend_alerts=enable_dividend_alerts,
            dividend_horizon_days=args.dividend_horizon_days,
            enable_news_alerts=enable_news_alerts,
            news_lookback_hours=args.news_lookback_hours,
            max_news_items_per_ticker=args.max_news_items_per_ticker,
            events_ignore_price_filter=args.events_ignore_price_filter,
            cooldown_minutes=args.cooldown_minutes,
            last_alerts=last_alerts,
            seen_event_keys=seen_event_keys,
            verbose=args.verbose,
        )
        end = now_utc()
        duration = (end - start).total_seconds()
        stamp = end.astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")
        print(
            f"[{stamp}] Ciclo completado. Tickers analizados: {checked}. "
            f"Alertas: {alerts}. Duracion: {duration:.1f}s."
        )

        if args.once:
            break

        if args.run_outside_market_hours:
            time.sleep(args.interval_minutes * 60)
        else:
            sleep_seconds = seconds_until_next_interval(now_madrid(), args.interval_minutes)
            time.sleep(sleep_seconds)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
