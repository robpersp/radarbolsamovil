import json
import time
import urllib.parse
import urllib.request
from statistics import mean
from typing import Dict, List, Optional, Tuple

from stock_pullback_alert import (
    AnalysisResult,
    INTRADAY_INTERVAL,
    INTRADAY_PERIOD,
    MADRID_TZ,
    company_name_for_ticker,
    headline_impact_keyword,
    parse_any_datetime,
)

YAHOO_CHART_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?{query}"
YAHOO_QUOTE_SUMMARY_URL = "https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?{query}"
YAHOO_SEARCH_URL = "https://query1.finance.yahoo.com/v1/finance/search?{query}"
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}
_CACHE_TTL_SECONDS = 120.0
_JSON_CACHE: Dict[str, Tuple[float, object]] = {}


def safe_mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return float(mean(values))


def pct_change(new_value: float, old_value: float) -> float:
    if old_value <= 0:
        return 0.0
    return ((new_value - old_value) / old_value) * 100.0


def _fetch_json(url: str) -> object:
    now_ts = time.time()
    cached = _JSON_CACHE.get(url)
    if cached is not None and (now_ts - cached[0]) <= _CACHE_TTL_SECONDS:
        return cached[1]

    request = urllib.request.Request(url, headers=DEFAULT_HEADERS)
    with urllib.request.urlopen(request, timeout=20) as response:
        payload = json.load(response)
    _JSON_CACHE[url] = (now_ts, payload)
    return payload


def fetch_chart_rows(ticker: str, period: str, interval: str) -> List[Dict[str, float]]:
    query = urllib.parse.urlencode(
        {
            "range": period,
            "interval": interval,
            "includePrePost": "false",
            "events": "div,splits",
        }
    )
    url = YAHOO_CHART_URL.format(ticker=urllib.parse.quote(ticker), query=query)
    payload = _fetch_json(url)

    chart = payload.get("chart", {})
    error = chart.get("error")
    if error:
        description = error.get("description") or "error desconocido"
        raise RuntimeError(f"Yahoo Finance devolvio un error para {ticker}: {description}")

    results = chart.get("result") or []
    if not results:
        return []

    result = results[0]
    quotes = (result.get("indicators") or {}).get("quote") or []
    if not quotes:
        return []

    quote = quotes[0]
    adjusted = None
    adjclose_list = (result.get("indicators") or {}).get("adjclose") or []
    if adjclose_list:
        adjusted = adjclose_list[0].get("adjclose")

    timestamps = result.get("timestamp") or []
    close_values = adjusted or quote.get("close") or []
    high_values = quote.get("high") or []
    low_values = quote.get("low") or []
    volume_values = quote.get("volume") or []

    rows: List[Dict[str, float]] = []
    for index, timestamp in enumerate(timestamps):
        close_value = close_values[index] if index < len(close_values) else None
        high_value = high_values[index] if index < len(high_values) else None
        low_value = low_values[index] if index < len(low_values) else None
        volume_value = volume_values[index] if index < len(volume_values) else 0

        if close_value is None or high_value is None or low_value is None:
            continue

        rows.append(
            {
                "timestamp": float(timestamp),
                "close": float(close_value),
                "high": float(high_value),
                "low": float(low_value),
                "volume": float(volume_value or 0),
            }
        )

    return rows


def trailing_mean(values: List[float], window: int, offset_from_end: int = 0) -> Optional[float]:
    if window <= 0:
        return None
    if offset_from_end < 0:
        return None
    end_index = len(values) - offset_from_end
    start_index = end_index - window
    if start_index < 0 or end_index > len(values) or start_index >= end_index:
        return None
    return safe_mean(values[start_index:end_index])


def extract_raw_or_value(value, default=None):
    if isinstance(value, dict):
        if "raw" in value and value["raw"] is not None:
            return value["raw"]
        if "fmt" in value and value["fmt"] is not None:
            return value["fmt"]
    if value is None:
        return default
    return value


def fetch_quote_summary(ticker: str, modules: List[str]) -> Dict[str, object]:
    query = urllib.parse.urlencode({"modules": ",".join(modules)})
    url = YAHOO_QUOTE_SUMMARY_URL.format(ticker=urllib.parse.quote(ticker), query=query)
    payload = _fetch_json(url)

    summary = payload.get("quoteSummary", {})
    error = summary.get("error")
    if error:
        return {}
    results = summary.get("result") or []
    if not results:
        return {}
    return results[0]


def fetch_search_news(ticker: str) -> List[Dict[str, object]]:
    query = urllib.parse.urlencode({"q": ticker, "quotesCount": 1, "newsCount": 8})
    url = YAHOO_SEARCH_URL.format(query=query)
    payload = _fetch_json(url)
    items = payload.get("news") or []
    if isinstance(items, list):
        return items
    return []


def compute_return_pct(closes: List[float], lookback_sessions: int = 20) -> float:
    if len(closes) <= lookback_sessions:
        return 0.0
    start_value = closes[-(lookback_sessions + 1)]
    end_value = closes[-1]
    return pct_change(end_value, start_value)


def benchmark_ticker_for_security(ticker: str) -> Optional[str]:
    if ticker.upper().endswith(".MC"):
        return "^IBEX"
    return None


def relative_strength_profile(ticker: str, daily_close: List[float]) -> Dict[str, object]:
    benchmark_ticker = benchmark_ticker_for_security(ticker)
    if benchmark_ticker is None:
        return {
            "benchmark_return_pct": 0.0,
            "relative_strength_pct": 0.0,
            "relative_strength_label": "sin comparar",
        }

    stock_return_pct = compute_return_pct(daily_close, 20)
    benchmark_return_pct = 0.0
    relative_strength_pct = 0.0
    relative_strength_label = "alineada con el mercado"
    try:
        benchmark_rows = fetch_chart_rows(benchmark_ticker, period="1y", interval="1d")
        benchmark_close = [row["close"] for row in benchmark_rows]
        if benchmark_close:
            benchmark_return_pct = compute_return_pct(benchmark_close, 20)
            relative_strength_pct = stock_return_pct - benchmark_return_pct
            if relative_strength_pct >= 4.0:
                relative_strength_label = "liderando claramente al mercado"
            elif relative_strength_pct >= 1.5:
                relative_strength_label = "mejor que el mercado"
            elif relative_strength_pct <= -4.0:
                relative_strength_label = "mucho peor que el mercado"
            elif relative_strength_pct <= -1.5:
                relative_strength_label = "mas floja que el mercado"
    except Exception:
        pass

    return {
        "benchmark_return_pct": benchmark_return_pct,
        "relative_strength_pct": relative_strength_pct,
        "relative_strength_label": relative_strength_label,
    }


def liquidity_profile(ticker: str, fallback_price: float, daily_volume_sma20: float) -> Dict[str, object]:
    bid_price = 0.0
    ask_price = 0.0
    spread_pct = 0.0
    spread_known = False
    spread_filter_ok = True
    average_volume = max(daily_volume_sma20, 0.0)
    average_traded_value_eur = fallback_price * average_volume

    try:
        summary = fetch_quote_summary(ticker, ["price", "summaryDetail"])
    except Exception:
        summary = {}

    price = summary.get("price") if isinstance(summary, dict) else {}
    summary_detail = summary.get("summaryDetail") if isinstance(summary, dict) else {}
    bid_value = extract_raw_or_value((summary_detail or {}).get("bid"))
    ask_value = extract_raw_or_value((summary_detail or {}).get("ask"))
    avg_daily_volume_3m = extract_raw_or_value((summary_detail or {}).get("averageVolume"))
    if avg_daily_volume_3m in (None, ""):
        avg_daily_volume_3m = extract_raw_or_value((price or {}).get("averageDailyVolume3Month"))

    if avg_daily_volume_3m not in (None, ""):
        try:
            average_volume = max(float(avg_daily_volume_3m), average_volume)
            average_traded_value_eur = fallback_price * average_volume
        except (TypeError, ValueError):
            pass

    try:
        bid_price = float(bid_value) if bid_value not in (None, "") else 0.0
    except (TypeError, ValueError):
        bid_price = 0.0
    try:
        ask_price = float(ask_value) if ask_value not in (None, "") else 0.0
    except (TypeError, ValueError):
        ask_price = 0.0

    if bid_price > 0 and ask_price > 0 and ask_price >= bid_price:
        mid_price = (bid_price + ask_price) / 2.0
        if mid_price > 0:
            spread_pct = ((ask_price - bid_price) / mid_price) * 100.0
            spread_known = True
            spread_filter_ok = spread_pct <= 0.60

    if average_traded_value_eur >= 5_000_000:
        liquidity_label = "alta"
    elif average_traded_value_eur >= 1_500_000:
        liquidity_label = "media"
    elif average_traded_value_eur >= 300_000:
        liquidity_label = "justa"
    else:
        liquidity_label = "baja"
    liquidity_filter_ok = average_traded_value_eur >= 300_000

    return {
        "average_traded_value_eur": average_traded_value_eur,
        "liquidity_filter_ok": liquidity_filter_ok,
        "liquidity_label": liquidity_label,
        "bid_price": bid_price,
        "ask_price": ask_price,
        "spread_pct": spread_pct,
        "spread_known": spread_known,
        "spread_filter_ok": spread_filter_ok,
    }


def liquidity_profile_from_daily(fallback_price: float, daily_volume_sma20: float) -> Dict[str, object]:
    average_volume = max(daily_volume_sma20, 0.0)
    average_traded_value_eur = fallback_price * average_volume
    if average_traded_value_eur >= 5_000_000:
        liquidity_label = "alta"
    elif average_traded_value_eur >= 1_500_000:
        liquidity_label = "media"
    elif average_traded_value_eur >= 300_000:
        liquidity_label = "justa"
    else:
        liquidity_label = "baja"
    return {
        "average_traded_value_eur": average_traded_value_eur,
        "liquidity_filter_ok": average_traded_value_eur >= 300_000,
        "liquidity_label": liquidity_label,
        "bid_price": 0.0,
        "ask_price": 0.0,
        "spread_pct": 0.0,
        "spread_known": False,
        "spread_filter_ok": True,
    }


def collect_company_events(
    ticker: str,
    news_lookback_hours: int = 72,
    max_news_items: int = 3,
) -> List[Dict[str, str]]:
    events: List[Dict[str, str]] = []

    try:
        summary = fetch_quote_summary(
            ticker,
            ["calendarEvents", "price", "summaryDetail", "defaultKeyStatistics"],
        )
    except Exception:
        summary = {}

    price = summary.get("price") if isinstance(summary, dict) else {}
    summary_detail = summary.get("summaryDetail") if isinstance(summary, dict) else {}
    calendar_events = summary.get("calendarEvents") if isinstance(summary, dict) else {}

    currency = str(extract_raw_or_value((price or {}).get("currency"), "EUR"))
    dividend_rate = extract_raw_or_value((summary_detail or {}).get("dividendRate"))
    dividend_yield = extract_raw_or_value((summary_detail or {}).get("dividendYield"))
    ex_div_value = extract_raw_or_value((calendar_events or {}).get("exDividendDate"))
    pay_value = extract_raw_or_value((calendar_events or {}).get("dividendDate"))
    ex_div_dt = parse_any_datetime(ex_div_value)
    pay_dt = parse_any_datetime(pay_value)

    if ex_div_dt is not None or pay_dt is not None or dividend_rate not in (None, ""):
        parts = []
        if dividend_rate not in (None, ""):
            parts.append(f"Importe aprox.: {float(dividend_rate):.4f} {currency}")
        if dividend_yield not in (None, ""):
            try:
                parts.append(f"Rentabilidad: {float(dividend_yield) * 100:.2f}%")
            except (TypeError, ValueError):
                pass
        if ex_div_dt is not None:
            parts.append(f"Ex-dividendo: {ex_div_dt.astimezone(MADRID_TZ).strftime('%d/%m/%Y')}")
        if pay_dt is not None:
            parts.append(f"Pago: {pay_dt.astimezone(MADRID_TZ).strftime('%d/%m/%Y')}")
        events.append(
            {
                "kind": "dividend",
                "title": "Dividendo detectado",
                "summary": " | ".join(parts) if parts else "Hay un evento de dividendo en calendario.",
                "url": "",
            }
        )

    try:
        news_items = fetch_search_news(ticker)
    except Exception:
        news_items = []

    import datetime as dt

    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=news_lookback_hours)
    for item in news_items:
        if not isinstance(item, dict):
            continue
        title = str(item.get("title", "")).strip()
        if not title:
            continue
        keyword = headline_impact_keyword(title)
        if keyword is None:
            continue
        published_dt = parse_any_datetime(item.get("providerPublishTime"))
        if published_dt is None:
            published_dt = parse_any_datetime(item.get("pubDate"))
        if published_dt is not None and published_dt < cutoff:
            continue
        publisher = str(item.get("publisher", "")).strip()
        link = str(item.get("link", "")).strip()
        category = "Noticia relevante"
        if keyword in {"opa", "takeover", "merger", "acquisition"}:
            category = "Posible OPA / operacion corporativa"
        elif keyword in {"dividendo", "dividend"}:
            category = "Noticia de dividendo"
        elif keyword in {"upgrade", "downgrade"}:
            category = "Cambio de recomendacion"
        elif keyword in {"earnings beat", "earnings miss", "resultado", "beneficio", "perdida"}:
            category = "Resultados empresariales"
        when_text = (
            published_dt.astimezone(MADRID_TZ).strftime("%d/%m %H:%M")
            if published_dt is not None
            else "fecha no disponible"
        )
        summary_parts = [category, f"Detectado por keyword: {keyword}", f"Hora: {when_text}"]
        if publisher:
            summary_parts.append(f"Fuente: {publisher}")
        events.append(
            {
                "kind": "news",
                "title": title,
                "summary": " | ".join(summary_parts),
                "url": link,
            }
        )
        if len([event for event in events if event["kind"] == "news"]) >= max_news_items:
            break

    return events


def analyze_ticker_desktop(
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
    daily_rows = fetch_chart_rows(ticker=ticker, period="1y", interval="1d")
    # Guardamos mas historial para que las graficas de la app tengan contexto real.
    chart_points = [row["close"] for row in daily_rows[-180:]]
    if len(daily_rows) < 210:
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
            chart_points=chart_points,
        )

    daily_close = [row["close"] for row in daily_rows]
    daily_high = [row["high"] for row in daily_rows]
    daily_low = [row["low"] for row in daily_rows]
    daily_volume = [row["volume"] for row in daily_rows]

    sma50 = trailing_mean(daily_close, 50)
    sma200 = trailing_mean(daily_close, 200)
    sma50_prev20 = trailing_mean(daily_close, 50, offset_from_end=19)
    last_daily_close = daily_close[-1]
    prev_daily_close = daily_close[-2] if len(daily_close) >= 2 else last_daily_close
    daily_20d_high = max(daily_high[-20:])
    daily_20d_low = min(daily_low[-20:])
    latest_daily_volume = daily_volume[-1]
    daily_volume_sma20 = safe_mean(daily_volume[-20:]) or 0.0
    daily_volume_ratio = (
        (latest_daily_volume / daily_volume_sma20) if daily_volume_sma20 > 0 else 0.0
    )
    daily_volume_ok = daily_volume_ratio >= min_daily_volume_ratio
    return_5d_pct = compute_return_pct(daily_close, 5)
    return_20d_pct = compute_return_pct(daily_close, 20)
    return_60d_pct = compute_return_pct(daily_close, 60)
    if sma50 is None or sma200 is None or sma50_prev20 is None:
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
            chart_points=chart_points,
        )

    ma_gap_pct = pct_change(sma50, sma200)
    price_vs_sma50_pct = pct_change(last_daily_close, sma50)
    sma50_slope_20d_pct = pct_change(sma50, sma50_prev20)
    uptrend = (
        last_daily_close > sma50
        and sma50 > sma200
        and ma_gap_pct >= min_ma_gap_pct
        and price_vs_sma50_pct >= min_price_above_sma50_pct
        and sma50_slope_20d_pct >= min_sma50_slope_20d_pct
    )
    daily_change_pct_live = pct_change(last_daily_close, prev_daily_close)
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

    if last_daily_close > max_price_eur:
        liq_profile = liquidity_profile_from_daily(last_daily_close, daily_volume_sma20)
        return AnalysisResult(
            ticker=ticker,
            company_name=company_name_for_ticker(ticker),
            uptrend=uptrend,
            price_filter_ok=False,
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
            reason=f"Precio actual ({last_daily_close:.2f} EUR) por encima de {max_price_eur:.2f} EUR.",
            volume_filter_ok=daily_volume_ok,
            latest_daily_volume=latest_daily_volume,
            daily_volume_sma20=daily_volume_sma20,
            daily_volume_ratio=daily_volume_ratio,
            daily_prev_close=prev_daily_close,
            daily_change_pct_live=daily_change_pct_live,
            daily_20d_high=daily_20d_high,
            daily_20d_low=daily_20d_low,
            daily_range_position_pct=daily_range_position_pct,
            daily_bias_label=daily_bias_label,
            return_5d_pct=return_5d_pct,
            return_20d_pct=return_20d_pct,
            return_60d_pct=return_60d_pct,
            benchmark_return_pct=0.0,
            relative_strength_pct=0.0,
            relative_strength_label="sin comparar",
            average_traded_value_eur=float(liq_profile["average_traded_value_eur"]),
            liquidity_filter_ok=bool(liq_profile["liquidity_filter_ok"]),
            liquidity_label=str(liq_profile["liquidity_label"]),
            bid_price=float(liq_profile["bid_price"]),
            ask_price=float(liq_profile["ask_price"]),
            spread_pct=float(liq_profile["spread_pct"]),
            spread_known=bool(liq_profile["spread_known"]),
            spread_filter_ok=bool(liq_profile["spread_filter_ok"]),
            chart_points=chart_points,
        )

    rs_profile = relative_strength_profile(ticker, daily_close)

    intraday_rows = fetch_chart_rows(
        ticker=ticker,
        period=INTRADAY_PERIOD,
        interval=INTRADAY_INTERVAL,
    )
    while len(intraday_rows) > 1 and float(intraday_rows[-1].get("volume") or 0.0) <= 0.0:
        intraday_rows.pop()

    min_required_intraday_bars = 20
    effective_lookback_bars = min(lookback_bars, len(intraday_rows))
    if len(intraday_rows) < max(min_required_intraday_bars, 20):
        price_filter_ok = last_daily_close <= max_price_eur
        liq_profile = liquidity_profile(ticker, last_daily_close, daily_volume_sma20)
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
            return_5d_pct=return_5d_pct,
            return_20d_pct=return_20d_pct,
            return_60d_pct=return_60d_pct,
            benchmark_return_pct=float(rs_profile["benchmark_return_pct"]),
            relative_strength_pct=float(rs_profile["relative_strength_pct"]),
            relative_strength_label=str(rs_profile["relative_strength_label"]),
            average_traded_value_eur=float(liq_profile["average_traded_value_eur"]),
            liquidity_filter_ok=bool(liq_profile["liquidity_filter_ok"]),
            liquidity_label=str(liq_profile["liquidity_label"]),
            bid_price=float(liq_profile["bid_price"]),
            ask_price=float(liq_profile["ask_price"]),
            spread_pct=float(liq_profile["spread_pct"]),
            spread_known=bool(liq_profile["spread_known"]),
            spread_filter_ok=bool(liq_profile["spread_filter_ok"]),
            daily_prev_close=prev_daily_close,
            daily_change_pct_live=daily_change_pct_live,
            daily_20d_high=daily_20d_high,
            daily_20d_low=daily_20d_low,
            daily_range_position_pct=daily_range_position_pct,
            daily_bias_label=daily_bias_label,
            reason="Sin suficientes velas intradia utiles para detectar pullback.",
            chart_points=chart_points,
        )

    intraday_close_full = [row["close"] for row in intraday_rows]
    intraday_volume_full = [row["volume"] for row in intraday_rows]
    intraday_close = intraday_close_full[-effective_lookback_bars:]
    intraday_volume = intraday_volume_full[-max(effective_lookback_bars, 20):]
    current_price = intraday_close[-1]
    current_intraday_volume = intraday_volume[-1]
    intraday_volume_sma20 = safe_mean(intraday_volume[-20:]) or 0.0
    intraday_volume_ratio = (
        (current_intraday_volume / intraday_volume_sma20) if intraday_volume_sma20 > 0 else 0.0
    )
    intraday_volume_ok = intraday_volume_ratio >= min_intraday_volume_ratio
    recent_high = max(intraday_close)
    if recent_high <= 0:
        price_filter_ok = current_price <= max_price_eur
        liq_profile = liquidity_profile(ticker, current_price, daily_volume_sma20)
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
            return_5d_pct=return_5d_pct,
            return_20d_pct=return_20d_pct,
            return_60d_pct=return_60d_pct,
            benchmark_return_pct=float(rs_profile["benchmark_return_pct"]),
            relative_strength_pct=float(rs_profile["relative_strength_pct"]),
            relative_strength_label=str(rs_profile["relative_strength_label"]),
            average_traded_value_eur=float(liq_profile["average_traded_value_eur"]),
            liquidity_filter_ok=bool(liq_profile["liquidity_filter_ok"]),
            liquidity_label=str(liq_profile["liquidity_label"]),
            bid_price=float(liq_profile["bid_price"]),
            ask_price=float(liq_profile["ask_price"]),
            spread_pct=float(liq_profile["spread_pct"]),
            spread_known=bool(liq_profile["spread_known"]),
            spread_filter_ok=bool(liq_profile["spread_filter_ok"]),
            daily_prev_close=prev_daily_close,
            daily_change_pct_live=daily_change_pct_live,
            daily_20d_high=daily_20d_high,
            daily_20d_low=daily_20d_low,
            daily_range_position_pct=daily_range_position_pct,
            daily_bias_label=daily_bias_label,
            reason="No se pudo calcular el maximo reciente.",
            chart_points=chart_points,
        )

    daily_change_pct_live = pct_change(current_price, prev_daily_close)
    drawdown_pct = ((recent_high - current_price) / recent_high) * 100.0
    price_filter_ok = current_price <= max_price_eur
    liq_profile = liquidity_profile(ticker, current_price, daily_volume_sma20)
    still_structurally_bullish = current_price >= (sma50 * 0.98)
    pullback = (
        uptrend
        and price_filter_ok
        and still_structurally_bullish
        and min_pullback_pct <= drawdown_pct <= max_pullback_pct
    )

    resistance_window = intraday_close[-max(resistance_lookback_bars + 1, 3):]
    previous_resistance = max(resistance_window[:-1]) if len(resistance_window) > 1 else recent_high
    prev_close = resistance_window[-2] if len(resistance_window) >= 2 else current_price
    breakout = (
        uptrend
        and price_filter_ok
        and daily_volume_ok
        and intraday_volume_ok
        and current_price > previous_resistance * (1 + (min_breakout_buffer_pct / 100.0))
        and prev_close <= previous_resistance
    )

    rebound_window = intraday_close[-max(rebound_lookback_bars, 5):]
    recent_low = min(rebound_window)
    low_pos = rebound_window.index(recent_low)
    bars_since_low = len(rebound_window) - 1 - low_pos
    rebound_pct = ((current_price - recent_low) / recent_low) * 100.0 if recent_low > 0 else 0.0
    prev_1 = rebound_window[-2] if len(rebound_window) >= 2 else current_price
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
    elif not bool(liq_profile["liquidity_filter_ok"]):
        reason = (
            f"Liquidez limitada (negocia aprox. {float(liq_profile['average_traded_value_eur']):.0f} EUR al dia)."
        )
    elif bool(liq_profile["spread_known"]) and not bool(liq_profile["spread_filter_ok"]):
        reason = f"Spread amplio ({float(liq_profile['spread_pct']):.2f}%), ejecucion poco limpia."
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
        return_5d_pct=return_5d_pct,
        return_20d_pct=return_20d_pct,
        return_60d_pct=return_60d_pct,
        benchmark_return_pct=float(rs_profile["benchmark_return_pct"]),
        relative_strength_pct=float(rs_profile["relative_strength_pct"]),
        relative_strength_label=str(rs_profile["relative_strength_label"]),
        average_traded_value_eur=float(liq_profile["average_traded_value_eur"]),
        liquidity_filter_ok=bool(liq_profile["liquidity_filter_ok"]),
        liquidity_label=str(liq_profile["liquidity_label"]),
        bid_price=float(liq_profile["bid_price"]),
        ask_price=float(liq_profile["ask_price"]),
        spread_pct=float(liq_profile["spread_pct"]),
        spread_known=bool(liq_profile["spread_known"]),
        spread_filter_ok=bool(liq_profile["spread_filter_ok"]),
        daily_prev_close=prev_daily_close,
        daily_change_pct_live=daily_change_pct_live,
        daily_20d_high=daily_20d_high,
        daily_20d_low=daily_20d_low,
        daily_range_position_pct=daily_range_position_pct,
        daily_bias_label=daily_bias_label,
        reason=reason,
        chart_points=chart_points,
    )
