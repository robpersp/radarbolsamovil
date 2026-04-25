from __future__ import annotations

import html
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Dict, List

import streamlit as st
import streamlit.components.v1 as components

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
if CURRENT_DIR not in sys.path:
    sys.path.insert(0, CURRENT_DIR)

from desktop_engine import analyze_ticker_desktop
from stock_pullback_alert import now_madrid, parse_tickers, pick_signal_kind, resolve_tickers


st.set_page_config(
    page_title="Bolsa movil",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="collapsed",
)


@dataclass
class MobileSettings:
    market: str
    tickers_raw: str
    alert_mode: str
    interval_minutes: int
    max_price_eur: float
    min_pullback_pct: float
    max_pullback_pct: float
    min_ma_gap_pct: float
    min_price_above_sma50_pct: float
    min_sma50_slope_20d_pct: float
    min_daily_volume_ratio: float
    min_intraday_volume_ratio: float
    only_market_hours: bool
    send_telegram_alerts: bool
    show_only_actionable: bool
    preset_name: str
    lookback_bars: int = 180
    resistance_lookback_bars: int = 180
    min_breakout_buffer_pct: float = 0.10
    rebound_lookback_bars: int = 120
    rebound_recent_bars: int = 30
    min_rebound_pct: float = 0.50


def _default_settings(show_all_prices: bool) -> MobileSettings:
    return MobileSettings(
        market="madrid-continuo",
        tickers_raw="",
        alert_mode="all",
        interval_minutes=5,
        max_price_eur=9999.0 if show_all_prices else 10.0,
        min_pullback_pct=2.0,
        max_pullback_pct=8.0,
        min_ma_gap_pct=2.0,
        min_price_above_sma50_pct=1.0,
        min_sma50_slope_20d_pct=1.0,
        min_daily_volume_ratio=1.05,
        min_intraday_volume_ratio=1.10,
        only_market_hours=False,
        send_telegram_alerts=False,
        show_only_actionable=False,
        preset_name="Equilibrado",
    )


def _row_tone(row: Dict[str, object]) -> str:
    decision = str(row["decision"])
    trend = str(row["trend"])
    if decision == "ENTRAR":
        return "tone-enter"
    if decision == "VIGILAR":
        return "tone-watch"
    if trend == "Alcista fuerte":
        return "tone-strong"
    if trend == "Alcista agresiva":
        return "tone-aggressive"
    if trend == "Alcista escalonada":
        return "tone-stepped"
    if trend == "Alcista":
        return "tone-up"
    if trend == "Posible entrada":
        return "tone-possible"
    return "tone-neutral"


def _decision_profile(result, signal_kind: str | None, conviction: float, risk_pct: float) -> Dict[str, object]:
    blockers: List[str] = []
    if not result.uptrend:
        blockers.append("la tendencia aun no es suficientemente limpia")
    if signal_kind is None:
        blockers.append("falta un disparador tecnico claro")
    if not result.volume_filter_ok:
        blockers.append("el volumen diario no acompana")
    if signal_kind in {"breakout", "rebound"} and result.intraday_volume_ratio < 1.05:
        blockers.append("el volumen intradia sigue flojo")
    if result.relative_strength_pct < 0:
        blockers.append("no esta batiendo al mercado")
    if not result.liquidity_filter_ok:
        blockers.append("la liquidez es demasiado justa")
    if result.spread_known and not result.spread_filter_ok:
        blockers.append("el spread es amplio")
    if risk_pct > 6.0:
        blockers.append("el stop queda demasiado lejos")

    clear = (
        signal_kind in {"breakout", "rebound", "pullback"}
        and result.uptrend
        and result.volume_filter_ok
        and result.relative_strength_pct >= 0
        and result.liquidity_filter_ok
        and (not result.spread_known or result.spread_filter_ok)
        and risk_pct <= 6.0
        and conviction >= 72.0
    )
    watch = (
        not clear
        and signal_kind in {"breakout", "rebound", "pullback", None}
        and result.uptrend
        and result.liquidity_filter_ok
        and risk_pct <= 8.0
        and conviction >= 52.0
    )
    if clear:
        return {"label": "ENTRAR", "detail": "Tiene estructura, volumen y riesgo bastante razonables para estudiarla."}
    if watch:
        brief = ", ".join(blockers[:2]) if blockers else "aun le falta una confirmacion"
        return {"label": "VIGILAR", "detail": f"Interesante, pero conviene esperar porque {brief}."}
    brief = ", ".join(blockers[:2]) if blockers else "la lectura no es suficientemente solida"
    return {"label": "Esperando senal", "detail": f"Aun no es una entrada fiable porque {brief}."}


def _trade_plan_for_result(result, signal_kind: str | None) -> Dict[str, object]:
    if signal_kind == "breakout":
        entry = max(result.current_price, result.resistance_level * 1.001)
        stop = max(result.sma50 * 0.99, result.recent_low * 0.995 if result.recent_low > 0 else 0.0)
        if stop <= 0 or stop >= entry:
            stop = entry * 0.985
        base = 86.0
    elif signal_kind == "rebound":
        entry = result.current_price
        stop = result.recent_low * 0.995 if result.recent_low > 0 else result.current_price * 0.985
        if stop <= 0 or stop >= entry:
            stop = entry * 0.985
        base = 78.0
    elif signal_kind == "pullback":
        entry = result.current_price
        stop = min(result.sma50 * 0.99, result.current_price * 0.97) if result.sma50 > 0 else result.current_price * 0.97
        if stop <= 0 or stop >= entry:
            stop = entry * 0.975
        base = 74.0
    elif result.uptrend and result.price_filter_ok:
        entry = max(result.current_price, result.resistance_level * 1.001 if result.resistance_level > 0 else result.current_price)
        stop = result.sma50 * 0.99 if result.sma50 > 0 else result.current_price * 0.97
        if stop <= 0 or stop >= entry:
            stop = entry * 0.975
        base = 62.0
    else:
        entry = result.current_price
        stop = result.current_price * 0.97 if result.current_price > 0 else 0.0
        base = 42.0

    risk = entry - stop if entry > stop else max(entry * 0.02, 0.01)
    tp1 = entry + risk * 1.5
    tp2 = entry + risk * 2.5
    if signal_kind == "breakout":
        tp2 = max(tp2, result.current_price * 1.03)
    elif signal_kind == "rebound":
        tp1 = min(tp1, result.recent_high) if result.recent_high > 0 else tp1
        tp2 = max(tp2, result.recent_high)
    elif signal_kind == "pullback":
        tp1 = max(tp1, result.current_price * 1.02)
        if result.recent_high > 0:
            tp2 = max(tp2, result.recent_high * 0.995)

    if tp1 <= entry:
        tp1 = entry * 1.02
    if tp2 <= tp1:
        tp2 = tp1 * 1.3

    conviction = base
    if result.uptrend:
        conviction += 6.0
    if signal_kind == "pullback" and result.uptrend:
        conviction += 10.0
    elif signal_kind in {"breakout", "rebound"}:
        conviction += 6.0
    elif result.daily_bias_label in {"alcista fuerte", "alcista"}:
        conviction += 4.0
    if result.current_price <= 4.0:
        conviction += 8.0
    elif result.current_price <= 10.0:
        conviction += 2.0
    if result.volume_filter_ok:
        conviction += min(result.daily_volume_ratio, 2.0) * 2.5
    else:
        conviction -= 2.0
    if result.intraday_volume_ratio >= 1.1:
        conviction += 4.0
    elif result.intraday_volume_ratio < 1.0:
        conviction -= 4.0
    if result.relative_strength_pct >= 4.0:
        conviction += 8.0
    elif result.relative_strength_pct >= 1.5:
        conviction += 4.0
    elif result.relative_strength_pct < 0:
        conviction -= 4.0
    if result.liquidity_filter_ok:
        conviction += 3.0
    else:
        conviction -= 6.0
    if result.spread_known and not result.spread_filter_ok:
        conviction -= 6.0
    if signal_kind is None:
        conviction -= 6.0

    conviction = max(5.0, min(conviction, 95.0))
    risk_pct = (((entry - stop) / entry) * 100.0) if entry > 0 and stop > 0 and entry > stop else 0.0
    potential_pct = (((tp2 - entry) / entry) * 100.0) if entry > 0 and tp2 > entry else 0.0
    rr = ((tp2 - entry) / (entry - stop)) if entry > stop and tp2 > entry else 0.0
    tp1_probability = min(96.0, max(18.0, conviction * 0.78 + (8.0 if result.uptrend else -8.0) - max(risk_pct - 3.0, 0.0) * 2.2))
    tp2_probability = min(tp1_probability - 6.0, max(8.0, tp1_probability - 18.0 + min(max(rr - 2.0, 0.0) * 5.0, 8.0) - max(risk_pct - 4.0, 0.0) * 1.8))
    decision = _decision_profile(result, signal_kind, conviction, risk_pct)
    return {
        "entry": entry,
        "stop": stop,
        "tp1": tp1,
        "tp2": tp2,
        "risk_pct": risk_pct,
        "potential_pct": potential_pct,
        "tp1_probability": tp1_probability,
        "tp2_probability": tp2_probability,
        "conviction": conviction,
        "decision_label": decision["label"],
        "decision_detail": decision["detail"],
    }


def _trend_label(result, signal_kind: str | None) -> str:
    if result.uptrend and result.daily_bias_label == "alcista fuerte":
        return "Alcista fuerte"
    if result.uptrend and result.return_5d_pct >= 8.0 and result.daily_change_pct_live >= 1.2:
        return "Alcista agresiva"
    if result.uptrend and result.pullback and result.return_20d_pct >= 4.0:
        return "Alcista escalonada"
    if signal_kind == "pullback" or result.pullback:
        return "Posible entrada"
    if result.uptrend or result.daily_bias_label in {"alcista", "neutra-alcista"}:
        return "Alcista"
    return "Estancada"


def _trend_priority(trend_label: str) -> int:
    priorities = {
        "Alcista fuerte": 5,
        "Alcista agresiva": 4,
        "Alcista escalonada": 3,
        "Alcista": 2,
        "Posible entrada": 1,
        "Estancada": 0,
    }
    return priorities.get(trend_label, 0)


@st.cache_data(ttl=180, show_spinner=False)
def run_mobile_scan(show_all_prices: bool, refresh_token: int) -> Dict[str, object]:
    del refresh_token
    settings = _default_settings(show_all_prices)
    raw_tickers = parse_tickers(settings.tickers_raw)
    tickers = resolve_tickers(settings.market, raw_tickers)

    rows: List[Dict[str, object]] = []
    errors: List[str] = []
    signal_count = 0
    max_workers = max(4, min(8, len(tickers))) if tickers else 1

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                analyze_ticker_desktop,
                ticker=ticker,
                min_pullback_pct=settings.min_pullback_pct,
                max_pullback_pct=settings.max_pullback_pct,
                lookback_bars=settings.lookback_bars,
                resistance_lookback_bars=settings.resistance_lookback_bars,
                min_breakout_buffer_pct=settings.min_breakout_buffer_pct,
                rebound_lookback_bars=settings.rebound_lookback_bars,
                rebound_recent_bars=settings.rebound_recent_bars,
                min_rebound_pct=settings.min_rebound_pct,
                max_price_eur=settings.max_price_eur,
                min_ma_gap_pct=settings.min_ma_gap_pct,
                min_price_above_sma50_pct=settings.min_price_above_sma50_pct,
                min_sma50_slope_20d_pct=settings.min_sma50_slope_20d_pct,
                min_daily_volume_ratio=settings.min_daily_volume_ratio,
                min_intraday_volume_ratio=settings.min_intraday_volume_ratio,
            ): ticker
            for ticker in tickers
        }
        for future in as_completed(future_map):
            ticker = future_map[future]
            try:
                result = future.result()
            except Exception as exc:
                errors.append(f"{ticker}: {exc}")
                continue

            if not show_all_prices and float(result.current_price) > 10.0:
                continue

            signal_kind = pick_signal_kind(result, settings.alert_mode)
            if signal_kind is not None:
                signal_count += 1

            plan = _trade_plan_for_result(result, signal_kind)
            trend = _trend_label(result, signal_kind)
            trend_priority = _trend_priority(trend)
            rows.append(
                {
                    "decision": plan["decision_label"],
                    "empresa": result.company_name or ticker,
                    "ticker": ticker,
                    "trend": trend,
                    "trend_priority": trend_priority,
                    "price": float(result.current_price),
                    "entry": float(plan["entry"]),
                    "stop": float(plan["stop"]),
                    "tp1": float(plan["tp1"]),
                    "tp2": float(plan["tp2"]),
                    "tp1_probability": float(plan["tp1_probability"]),
                    "tp2_probability": float(plan["tp2_probability"]),
                    "conviction": float(plan["conviction"]),
                    "potential_pct": float(plan["potential_pct"]),
                }
            )

    rows.sort(
        key=lambda row: (
            row["trend_priority"],
            1 if row["decision"] == "ENTRAR" else 0,
            1 if row["decision"] == "VIGILAR" else 0,
            row["conviction"],
        ),
        reverse=True,
    )

    enter_count = sum(1 for row in rows if row["decision"] == "ENTRAR")
    watch_count = sum(1 for row in rows if row["decision"] == "VIGILAR")
    return {
        "rows": rows,
        "errors": errors,
        "checked": len(tickers),
        "signal_count": signal_count,
        "enter_count": enter_count,
        "watch_count": watch_count,
        "updated_at": now_madrid().strftime("%d/%m/%Y %H:%M:%S"),
    }


def render_table(rows: List[Dict[str, object]]) -> None:
    if not rows:
        st.warning("No hay filas para mostrar con el filtro actual.")
        return

    headers = [
        "DEC.",
        "EMP.",
        "TICK.",
        "TEND.",
        "PREC.",
        "ENT.",
        "STOP",
        "TP1",
        "TP2",
        "%TP1",
        "%TP2",
        "CONV.",
        "BEN.",
    ]
    table_rows = []
    for row in rows:
        table_rows.append(
            f"""
            <tr class="{_row_tone(row)}">
                <td>{html.escape(str(row["decision"]))}</td>
                <td>{html.escape(str(row["empresa"]))}</td>
                <td>{html.escape(str(row["ticker"]))}</td>
                <td>{html.escape(str(row["trend"]))}</td>
                <td>{row["price"]:.2f}</td>
                <td>{row["entry"]:.2f}</td>
                <td>{row["stop"]:.2f}</td>
                <td>{row["tp1"]:.2f}</td>
                <td>{row["tp2"]:.2f}</td>
                <td>{row["tp1_probability"]:.0f}%</td>
                <td>{row["tp2_probability"]:.0f}%</td>
                <td>{row["conviction"]:.0f}</td>
                <td>{row["potential_pct"]:.1f}%</td>
            </tr>
            """
        )

    st.markdown(
        """
        <style>
        .mobile-shell {
            padding-top: 0.2rem;
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(4, minmax(0, 1fr));
            gap: 0.5rem;
            margin: 0.3rem 0 0.8rem 0;
        }
        .summary-card {
            background: #0d1b2b;
            border: 1px solid #29445d;
            border-radius: 12px;
            padding: 0.65rem 0.7rem;
        }
        .summary-label {
            font-size: 0.75rem;
            color: #9fb6c8;
            margin-bottom: 0.15rem;
        }
        .summary-value {
            font-size: 1rem;
            font-weight: 700;
            color: #eef4fb;
        }
        .table-wrap {
            overflow-x: auto;
            border: 1px solid #29445d;
            border-radius: 14px;
            background: #0d1b2b;
        }
        table.mobile-table {
            width: 100%;
            border-collapse: collapse;
            min-width: 1040px;
            font-size: 0.82rem;
        }
        .mobile-table thead th {
            position: sticky;
            top: 0;
            background: #17324c;
            color: #eef4fb;
            text-align: left;
            padding: 0.65rem 0.55rem;
            white-space: nowrap;
        }
        .mobile-table tbody td {
            padding: 0.58rem 0.55rem;
            border-top: 1px solid #1c3247;
            white-space: nowrap;
            color: #f5f8fc;
        }
        .tone-enter td { color: #ff6b78 !important; font-weight: 700; }
        .tone-watch td { color: #ffb44c !important; font-weight: 700; }
        .tone-strong td { color: #39e06e !important; }
        .tone-aggressive td { color: #ffd85c !important; }
        .tone-stepped td { color: #8fd3ff !important; }
        .tone-up td { color: #67d97d !important; }
        .tone-possible td { color: #ff7c86 !important; }
        @media (max-width: 720px) {
            .summary-grid {
                grid-template-columns: repeat(2, minmax(0, 1fr));
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    header_html = "".join(f"<th>{h}</th>" for h in headers)
    body_html = "".join(table_rows)
    table_html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <style>
    html, body {{
        margin: 0;
        padding: 0;
        background: #0d1b2b;
        color: #f5f8fc;
        font-family: "Segoe UI", sans-serif;
    }}
    .table-wrap {{
        overflow-x: auto;
        border: 1px solid #29445d;
        border-radius: 14px;
        background: #0d1b2b;
    }}
    table.mobile-table {{
        width: 100%;
        border-collapse: collapse;
        min-width: 1040px;
        font-size: 0.82rem;
        background: #0d1b2b;
    }}
    .mobile-table thead th {{
        position: sticky;
        top: 0;
        background: #17324c;
        color: #eef4fb;
        text-align: left;
        padding: 0.65rem 0.55rem;
        white-space: nowrap;
        border-bottom: 1px solid #29445d;
    }}
    .mobile-table tbody td {{
        padding: 0.58rem 0.55rem;
        border-top: 1px solid #1c3247;
        white-space: nowrap;
        color: #f5f8fc;
        background: #0d1b2b;
    }}
    .tone-enter td {{ color: #ff6b78 !important; font-weight: 700; }}
    .tone-watch td {{ color: #ffb44c !important; font-weight: 700; }}
    .tone-strong td {{ color: #39e06e !important; }}
    .tone-aggressive td {{ color: #ffd85c !important; }}
    .tone-stepped td {{ color: #8fd3ff !important; }}
    .tone-up td {{ color: #67d97d !important; }}
    .tone-possible td {{ color: #ff7c86 !important; }}
    .tone-neutral td {{ color: #f5f8fc !important; }}
    </style>
    </head>
    <body>
    <div class="table-wrap">
        <table class="mobile-table">
            <thead><tr>{header_html}</tr></thead>
            <tbody>{body_html}</tbody>
        </table>
    </div>
    </body>
    </html>
    """
    table_height = min(max(420, 120 + len(rows) * 34), 1100)
    components.html(table_html, height=table_height, scrolling=True)


def main() -> None:
    st.markdown("<div class='mobile-shell'></div>", unsafe_allow_html=True)
    st.title("Bolsa movil")
    st.caption("Version web para movil centrada en la tabla del radar.")

    if "refresh_token" not in st.session_state:
        st.session_state.refresh_token = 0

    col1, col2 = st.columns([1.4, 1])
    with col1:
        scope_label = st.segmented_control(
            "Filtro",
            options=["Empresas < 10", "Todo mercado"],
            default="Empresas < 10",
        )
    with col2:
        if st.button("Actualizar tabla", use_container_width=True):
            st.session_state.refresh_token += 1

    show_all_prices = scope_label == "Todo mercado"

    with st.spinner("Cargando tabla del radar..."):
        payload = run_mobile_scan(show_all_prices, st.session_state.refresh_token)

    st.markdown(
        f"""
        <div class="summary-grid">
            <div class="summary-card"><div class="summary-label">Analizados</div><div class="summary-value">{payload['checked']}</div></div>
            <div class="summary-card"><div class="summary-label">Señales</div><div class="summary-value">{payload['signal_count']}</div></div>
            <div class="summary-card"><div class="summary-label">Entrar</div><div class="summary-value">{payload['enter_count']}</div></div>
            <div class="summary-card"><div class="summary-label">Vigilar</div><div class="summary-value">{payload['watch_count']}</div></div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.caption(f"Ultima actualizacion: {payload['updated_at']} | Desliza la tabla horizontalmente si hace falta.")
    render_table(payload["rows"])

    if payload["errors"]:
        with st.expander(f"Errores de carga ({len(payload['errors'])})"):
            for err in payload["errors"][:20]:
                st.write(err)


if __name__ == "__main__":
    main()
