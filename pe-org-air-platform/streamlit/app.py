
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import requests
import streamlit as st


# ============================================================
# Config
# ============================================================

DEFAULT_API_BASE = os.getenv("API_BASE_URL", "http://localhost:8000")
DEFAULT_API_PREFIX = os.getenv("API_PREFIX", "/api/v1")
DEFAULT_SCORING_PREFIX = os.getenv("SCORING_PREFIX", "/api/v1/scoring")

ASSESSMENT_TYPES = ["screening", "due_diligence", "quarterly", "exit_prep"]
ASSESSMENT_STATUSES = ["draft", "in_progress", "submitted", "approved", "superseded"]
DIMENSIONS = [
    "data_infrastructure",
    "ai_governance",
    "technology_stack",
    "talent_skills",
    "leadership_vision",
    "use_case_portfolio",
    "culture_change",
]

ROOT_DIR = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT_DIR / "scripts"


# ============================================================
# HTTP helpers
# ============================================================


def _join_url(base: str, path: str) -> str:
    return base.rstrip("/") + "/" + path.lstrip("/")


def _api_url(base: str, prefix: str, path: str, include_prefix: bool = True) -> str:
    if include_prefix:
        return _join_url(base, _join_url(prefix, path))
    return _join_url(base, path)


def _scoring_url(base: str, scoring_prefix: str, path: str) -> str:
    return _join_url(base, _join_url(scoring_prefix, path))


def _request(method: str, url: str, **kwargs: Any) -> requests.Response:
    timeout = kwargs.pop("timeout", 15)
    return requests.request(method, url, timeout=timeout, **kwargs)


def _request_json(method: str, url: str, **kwargs: Any) -> Any:
    resp = _request(method, url, **kwargs)
    if not resp.ok:
        raise requests.HTTPError(resp.text, response=resp)
    if resp.status_code == 204 or not resp.text.strip():
        return None
    return resp.json()


def _show_http_error(exc: requests.HTTPError) -> None:
    resp = exc.response
    if resp is None:
        st.error(f"Request failed: {exc}")
        return

    st.error(f"Request failed: {resp.status_code}")
    if not resp.text:
        return

    try:
        st.json(resp.json())
    except ValueError:
        st.code(resp.text)


def _show_payload(payload: Any) -> None:
    if payload is None:
        st.info("No content")
        return

    if isinstance(payload, dict):
        items = payload.get("items")
        if isinstance(items, list):
            meta = {k: v for k, v in payload.items() if k != "items"}
            if meta:
                st.json(meta)
            st.dataframe(items, use_container_width=True)
            return
        st.json(payload)
        return

    if isinstance(payload, list):
        if payload and all(isinstance(x, dict) for x in payload):
            st.dataframe(payload, use_container_width=True)
        else:
            st.json(payload)
        return

    st.write(payload)


def _parse_json_input(label: str, text: str, allow_empty: bool = True) -> tuple[bool, Any]:
    raw = text.strip()
    if not raw and allow_empty:
        return True, {}
    try:
        return True, json.loads(raw)
    except json.JSONDecodeError as exc:
        st.error(f"{label} has invalid JSON: {exc}")
        return False, None


def _build_headers(bearer_token: str, extra_headers_text: str) -> tuple[dict[str, str], str | None]:
    headers: dict[str, str] = {}

    token = bearer_token.strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"

    raw = extra_headers_text.strip()
    if not raw:
        return headers, None

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        return headers, f"Extra headers JSON is invalid: {exc}"

    if not isinstance(parsed, dict):
        return headers, "Extra headers JSON must be an object"

    for key, value in parsed.items():
        headers[str(key)] = str(value)

    return headers, None


def _list_repo_scripts() -> list[str]:
    if not SCRIPTS_DIR.exists():
        return []
    return sorted([p.name for p in SCRIPTS_DIR.glob("*.py")])


def _inject_ui_theme() -> None:
    st.markdown(
        """
<style>
@import url('https://fonts.googleapis.com/css2?family=Manrope:wght@400;600;700;800&display=swap');

:root {
    --ui-ink: #e8f1ff;
    --ui-muted: #9eb3cc;
    --ui-accent: #2c8bff;
    --ui-accent-soft: #31c6e6;
    --ui-border: #2a3d57;
    --ui-card: rgba(14, 24, 38, 0.88);
}

html, body, [class*="css"] {
    font-family: "Manrope", "Segoe UI", "Trebuchet MS", sans-serif;
    color: var(--ui-ink);
}

.stApp {
    background:
        radial-gradient(1000px 600px at 10% -15%, rgba(44, 139, 255, 0.22) 0%, transparent 60%),
        radial-gradient(1200px 680px at 96% -18%, rgba(49, 198, 230, 0.18) 0%, transparent 62%),
        linear-gradient(180deg, #070e18 0%, #0c1726 58%, #111b2c 100%);
}

[data-testid="stHeader"] {
    background: transparent;
}

.block-container {
    padding-top: 1rem;
}

section[data-testid="stSidebar"] {
    background: linear-gradient(180deg, #0a121f 0%, #0f2136 100%);
}

section[data-testid="stSidebar"] * {
    color: #dce8f8 !important;
}

div[data-baseweb="tab-list"] {
    gap: 0.35rem;
    padding: 0.35rem;
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    background: rgba(8, 16, 28, 0.8);
    backdrop-filter: blur(6px);
}

button[data-baseweb="tab"] {
    border-radius: 10px !important;
    padding: 0.4rem 0.9rem !important;
    font-weight: 700 !important;
    color: #9cb2cb !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease, color 0.2s ease;
}

button[data-baseweb="tab"]:hover {
    transform: translateY(-1px);
}

button[data-baseweb="tab"][aria-selected="true"] {
    color: #ffffff !important;
    background: linear-gradient(90deg, var(--ui-accent) 0%, #22a5ff 100%) !important;
    box-shadow: 0 7px 16px rgba(34, 165, 255, 0.32);
}

div.stButton > button,
div.stDownloadButton > button,
div[data-testid="stForm"] button[kind] {
    position: relative;
    overflow: hidden;
    border: 0;
    border-radius: 12px;
    font-weight: 700;
    letter-spacing: 0.2px;
    color: #ffffff;
    background: linear-gradient(135deg, var(--ui-accent) 0%, #249bff 58%, var(--ui-accent-soft) 100%);
    box-shadow: 0 8px 22px rgba(44, 139, 255, 0.34);
    transition: transform 0.18s ease, box-shadow 0.22s ease, filter 0.18s ease;
}

div.stButton > button:hover,
div.stDownloadButton > button:hover,
div[data-testid="stForm"] button[kind]:hover {
    transform: translateY(-2px) scale(1.01);
    box-shadow: 0 12px 28px rgba(44, 139, 255, 0.38);
}

div.stButton > button::after,
div.stDownloadButton > button::after,
div[data-testid="stForm"] button[kind]::after {
    content: "";
    position: absolute;
    inset: -55%;
    border-radius: 999px;
    background: radial-gradient(circle, rgba(255, 255, 255, 0.62) 0%, rgba(255, 255, 255, 0) 68%);
    opacity: 0;
    transform: scale(0.4);
    pointer-events: none;
}

div.stButton > button:active,
div.stDownloadButton > button:active,
div[data-testid="stForm"] button[kind]:active {
    transform: scale(0.97);
    animation: click-pop 0.33s ease, click-flash 0.5s ease;
}

div.stButton > button:active::after,
div.stDownloadButton > button:active::after,
div[data-testid="stForm"] button[kind]:active::after {
    animation: click-ripple 0.55s ease-out;
}

div[data-testid="stDataFrame"],
div[data-testid="stTable"] {
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 10px 24px rgba(1, 8, 16, 0.45);
}

div[data-testid="stMetric"] {
    background: var(--ui-card);
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    box-shadow: 0 8px 20px rgba(0, 8, 18, 0.4);
}

div.stTextInput > div > div > input,
div.stNumberInput > div > div > input,
div.stTextArea textarea {
    border-radius: 10px !important;
    border: 1px solid var(--ui-border) !important;
    background-color: rgba(11, 21, 35, 0.92) !important;
    color: var(--ui-ink) !important;
}

div.stTextInput > div > div > input::placeholder,
div.stNumberInput > div > div > input::placeholder,
div.stTextArea textarea::placeholder {
    color: var(--ui-muted) !important;
}

label, p, span, .stMarkdown, [data-testid="stMarkdownContainer"], .stCaption {
    color: var(--ui-ink) !important;
}

div[data-testid="stAlert"] {
    border-radius: 12px;
}

div[data-testid="stCodeBlock"] pre {
    border: 1px solid var(--ui-border);
    border-radius: 12px;
    background: rgba(6, 14, 24, 0.95) !important;
}

@keyframes click-pop {
    0% { transform: scale(1); }
    40% { transform: scale(0.93); }
    100% { transform: scale(1); }
}

@keyframes click-flash {
    0% { filter: brightness(1); }
    25% { filter: brightness(1.3); }
    100% { filter: brightness(1); }
}

@keyframes click-ripple {
    0% { opacity: 0.55; transform: scale(0.35); }
    100% { opacity: 0; transform: scale(1.75); }
}

@keyframes section-fade {
    from { opacity: 0; transform: translateY(6px); }
    to { opacity: 1; transform: translateY(0); }
}

[data-testid="stVerticalBlock"] > div {
    animation: section-fade 0.34s ease-out both;
}
</style>
        """,
        unsafe_allow_html=True,
    )


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_scoring_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict) and "composite_score" in item]
    if isinstance(payload, dict):
        if "composite_score" in payload:
            return [payload]
        items = payload.get("items")
        if isinstance(items, list):
            return [item for item in items if isinstance(item, dict) and "composite_score" in item]
    return []


def _vega_common_config() -> dict[str, Any]:
    return {
        "background": "transparent",
        "config": {
            "view": {"stroke": None},
            "axis": {
                "labelColor": "#dce8f8",
                "titleColor": "#dce8f8",
                "gridColor": "rgba(158, 179, 204, 0.22)",
                "domainColor": "rgba(158, 179, 204, 0.35)",
            },
            "legend": {
                "labelColor": "#dce8f8",
                "titleColor": "#dce8f8",
                "orient": "top",
            },
        },
    }


def _hydrate_company_names(
    company_ids: list[str],
    api_base: str,
    api_prefix: str,
    timeout: int,
    headers: dict[str, str],
    verify_tls: bool,
) -> dict[str, str]:
    cache = st.session_state.setdefault("company_name_cache", {})
    if not isinstance(cache, dict):
        cache = {}
        st.session_state["company_name_cache"] = cache

    for company_id in company_ids:
        cid = str(company_id).strip()
        if not cid or cid in cache:
            continue
        try:
            url = _api_url(api_base, api_prefix, f"/companies/{cid}")
            out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
            if isinstance(out, dict):
                company_name = str(out.get("name", "")).strip()
                cache[cid] = company_name or cid
            else:
                cache[cid] = cid
        except Exception:
            cache[cid] = cid
    return cache


def _render_scoring_visuals(records: list[dict[str, Any]], company_names: dict[str, str] | None = None) -> None:
    if not records:
        st.info("Load scoring results to render charts.")
        return

    names = company_names or {}
    ranked = sorted(records, key=lambda row: _to_float(row.get("composite_score")), reverse=True)
    preview = ranked[:15]
    score_min = min(_to_float(r.get("composite_score")) for r in ranked)
    score_max = max(_to_float(r.get("composite_score")) for r in ranked)
    avg_score = sum(_to_float(r.get("composite_score")) for r in ranked) / len(ranked)

    m1, m2, m3 = st.columns(3)
    m1.metric("Tracked Companies", len(ranked))
    m2.metric("Average Composite Score", f"{avg_score:.2f}")
    m3.metric("Score Range", f"{score_min:.2f} - {score_max:.2f}")

    leaderboard_data: list[dict[str, Any]] = []
    scatter_data: list[dict[str, Any]] = []
    for row in preview:
        company_id = str(row.get("company_id", "unknown"))
        company_label = names.get(company_id, company_id)
        leaderboard_data.append(
            {
                "company": company_label,
                "company_id": company_id,
                "composite_score": _to_float(row.get("composite_score")),
                "score_band": str(row.get("score_band", "unknown")).lower(),
            }
        )

    for row in ranked:
        company_id = str(row.get("company_id", "unknown"))
        company_label = names.get(company_id, company_id)
        scatter_data.append(
            {
                "company": company_label,
                "company_id": company_id,
                "vr_score": _to_float(row.get("vr_score")),
                "composite_score": _to_float(row.get("composite_score")),
                "synergy_bonus": _to_float(row.get("synergy_bonus")),
                "talent_penalty": _to_float(row.get("talent_penalty")),
                "score_band": str(row.get("score_band", "unknown")).lower(),
            }
        )

    chart_theme = _vega_common_config()

    col1, col2 = st.columns(2)
    with col1:
        st.caption("Top Composite Scores")
        st.vega_lite_chart(
            leaderboard_data,
            {
                **chart_theme,
                "mark": {"type": "bar", "cornerRadiusEnd": 8},
                "encoding": {
                    "y": {"field": "company", "type": "nominal", "sort": "-x", "title": None},
                    "x": {"field": "composite_score", "type": "quantitative", "title": "Composite"},
                    "color": {
                        "field": "score_band",
                        "type": "nominal",
                        "scale": {"range": ["#31c6e6", "#2c8bff", "#5c8eff", "#f6c85f", "#f08a5d"]},
                        "title": "Band",
                    },
                    "tooltip": [
                        {"field": "company", "type": "nominal"},
                        {"field": "company_id", "type": "nominal", "title": "Company ID"},
                        {"field": "composite_score", "type": "quantitative", "format": ".3f"},
                        {"field": "score_band", "type": "nominal"},
                    ],
                },
            },
            use_container_width=True,
        )

    with col2:
        st.caption("Composite Score Distribution")
        st.vega_lite_chart(
            ranked,
            {
                **chart_theme,
                "mark": {"type": "area", "line": {"color": "#2c8bff"}, "color": {"gradient": "linear", "stops": [{"offset": 0, "color": "rgba(49,198,230,0.6)"}, {"offset": 1, "color": "rgba(44,139,255,0.1)"}]}},
                "encoding": {
                    "x": {"field": "composite_score", "type": "quantitative", "bin": {"maxbins": 12}, "title": "Composite Score"},
                    "y": {"aggregate": "count", "type": "quantitative", "title": "Companies"},
                    "tooltip": [{"aggregate": "count", "type": "quantitative", "title": "Count"}],
                },
            },
            use_container_width=True,
        )

    st.caption("VR vs Composite (Bubble size = Synergy bonus, color = Score band)")
    st.vega_lite_chart(
        scatter_data,
        {
            **chart_theme,
            "mark": {"type": "circle", "opacity": 0.82, "stroke": "#0b1a2b", "strokeWidth": 1},
            "encoding": {
                "x": {"field": "vr_score", "type": "quantitative", "title": "VR Score"},
                "y": {"field": "composite_score", "type": "quantitative", "title": "Composite Score"},
                "size": {"field": "synergy_bonus", "type": "quantitative", "title": "Synergy Bonus"},
                "color": {
                    "field": "score_band",
                    "type": "nominal",
                    "scale": {"range": ["#31c6e6", "#2c8bff", "#5c8eff", "#f6c85f", "#f08a5d"]},
                    "title": "Score Band",
                },
                "tooltip": [
                    {"field": "company", "type": "nominal"},
                    {"field": "company_id", "type": "nominal", "title": "Company ID"},
                    {"field": "vr_score", "type": "quantitative", "format": ".3f"},
                    {"field": "composite_score", "type": "quantitative", "format": ".3f"},
                    {"field": "synergy_bonus", "type": "quantitative", "format": ".3f"},
                    {"field": "talent_penalty", "type": "quantitative", "format": ".3f"},
                ],
            },
        },
        use_container_width=True,
    )


def _render_company_breakdown(record: dict[str, Any], company_names: dict[str, str] | None = None) -> None:
    dimensions = record.get("dimension_breakdown")
    if not isinstance(dimensions, list) or not dimensions:
        st.info("No dimension breakdown found for this company.")
        return

    chart_data: list[dict[str, Any]] = []
    for item in dimensions:
        if not isinstance(item, dict):
            continue
        chart_data.append(
            {
                "dimension": str(item.get("dimension", "unknown")).replace("_", " ").title(),
                "weighted_score": _to_float(item.get("weighted_score")),
                "raw_score": _to_float(item.get("raw_score")),
                "confidence": _to_float(item.get("confidence")),
            }
        )

    if not chart_data:
        st.info("No dimension breakdown found for this company.")
        return

    company_id = str(record.get("company_id", "Unknown Company"))
    company_label = (company_names or {}).get(company_id, company_id)
    st.caption(f"Dimension Breakdown: {company_label}")
    st.vega_lite_chart(
        chart_data,
        {
            **_vega_common_config(),
            "layer": [
                {
                    "mark": {"type": "bar", "cornerRadiusEnd": 8},
                    "encoding": {
                        "y": {"field": "dimension", "type": "nominal", "sort": "-x", "title": None},
                        "x": {"field": "weighted_score", "type": "quantitative", "title": "Weighted Score"},
                        "color": {
                            "field": "confidence",
                            "type": "quantitative",
                            "scale": {"range": ["#f08a5d", "#31c6e6"]},
                            "title": "Confidence",
                        },
                        "tooltip": [
                            {"field": "dimension", "type": "nominal"},
                            {"field": "weighted_score", "type": "quantitative", "format": ".3f"},
                            {"field": "raw_score", "type": "quantitative", "format": ".3f"},
                            {"field": "confidence", "type": "quantitative", "format": ".3f"},
                        ],
                    },
                }
            ],
        },
        use_container_width=True,
    )


# ============================================================
# UI setup
# ============================================================

st.set_page_config(page_title="PE OrgAIR Platform", layout="wide")
_inject_ui_theme()
st.title("PE OrgAIR Platform")
st.caption("Comprehensive Streamlit console for all current API routers and local scripts.")

with st.sidebar:
    st.header("Connection")
    api_base = st.text_input("API Base URL", value=DEFAULT_API_BASE)
    api_prefix = st.text_input("API Prefix", value=DEFAULT_API_PREFIX)
    scoring_prefix = st.text_input("Scoring Prefix", value=DEFAULT_SCORING_PREFIX)
    timeout = st.number_input("HTTP Timeout (seconds)", min_value=1, max_value=300, value=20)
    verify_tls = st.checkbox("Verify TLS certificates", value=True)

    st.divider()
    st.header("Optional Auth")
    bearer_token = st.text_input("Bearer Token", value="", type="password")
    extra_headers_text = st.text_area("Extra Headers JSON", value="{}", height=100)

    headers, headers_error = _build_headers(bearer_token, extra_headers_text)
    if headers_error:
        st.error(headers_error)

    st.divider()
    st.caption("Routing notes")
    st.write("- Health endpoints do not use API prefix")
    st.write("- Scoring routes use the dedicated scoring prefix")


# ============================================================
# Tabs
# ============================================================

main_tabs = st.tabs(
    [
        "Health",
        "Companies",
        "Assessments",
        "Collection",
        "Documents & Chunks",
        "Signals",
        "Signal Summaries",
        "Evidence",
        "Scoring",
        "Scripts",
        "Raw API",
    ]
)


# ============================================================
# Health
# ============================================================

with main_tabs[0]:
    col1, col2 = st.columns(2)

    with col1:
        if st.button("GET /health", key="health_simple"):
            try:
                url = _api_url(api_base, api_prefix, "/health", include_prefix=False)
                payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Health check failed: {exc}")

    with col2:
        if st.button("GET /health/detailed", key="health_detailed"):
            try:
                url = _api_url(api_base, api_prefix, "/health/detailed", include_prefix=False)
                payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Detailed health check failed: {exc}")

# ============================================================
# Companies
# ============================================================

with main_tabs[1]:
    tabs = st.tabs(["List", "Industries", "Get", "Create", "Update", "Delete"])

    with tabs[0]:
        page = st.number_input("Page", min_value=1, value=1, key="companies_page")
        page_size = st.number_input("Page Size", min_value=1, max_value=100, value=20, key="companies_page_size")
        if st.button("GET /companies", key="companies_list_btn"):
            try:
                url = _api_url(api_base, api_prefix, "/companies")
                payload = _request_json(
                    "GET",
                    url,
                    params={"page": int(page), "page_size": int(page_size)},
                    timeout=timeout,
                    headers=headers,
                    verify=verify_tls,
                )
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List companies failed: {exc}")

    with tabs[1]:
        if st.button("GET /companies/industries", key="companies_industries_btn"):
            try:
                url = _api_url(api_base, api_prefix, "/companies/industries")
                payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List industries failed: {exc}")

    with tabs[2]:
        company_id = st.text_input("Company ID", key="companies_get_id")
        if st.button("GET /companies/{company_id}", key="companies_get_btn"):
            if not company_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{company_id.strip()}")
                    payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(payload)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get company failed: {exc}")

    with tabs[3]:
        with st.form("companies_create_form"):
            name = st.text_input("Name")
            ticker = st.text_input("Ticker (uppercase, optional)")
            industry_id = st.text_input("Industry ID (optional)")
            position_factor = st.number_input("Position Factor", min_value=-1.0, max_value=1.0, value=0.0, step=0.01)
            submitted = st.form_submit_button("POST /companies")

        if submitted:
            if not name.strip():
                st.error("Name is required")
            else:
                payload: dict[str, Any] = {
                    "name": name.strip(),
                    "position_factor": float(position_factor),
                }
                if ticker.strip():
                    payload["ticker"] = ticker.strip().upper()
                if industry_id.strip():
                    payload["industry_id"] = industry_id.strip()

                try:
                    url = _api_url(api_base, api_prefix, "/companies")
                    out = _request_json("POST", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Create company failed: {exc}")

    with tabs[4]:
        with st.form("companies_update_form"):
            update_id = st.text_input("Company ID")
            update_name = st.text_input("Name (optional)")
            update_ticker = st.text_input("Ticker (optional)")
            update_industry = st.text_input("Industry ID (optional)")
            include_position = st.checkbox("Include position_factor", value=False)
            update_position = st.number_input("Position Factor", min_value=-1.0, max_value=1.0, value=0.0, step=0.01)
            submitted_update = st.form_submit_button("PUT /companies/{company_id}")

        if submitted_update:
            if not update_id.strip():
                st.error("Company ID is required")
            else:
                payload: dict[str, Any] = {}
                if update_name.strip():
                    payload["name"] = update_name.strip()
                if update_ticker.strip():
                    payload["ticker"] = update_ticker.strip().upper()
                if update_industry.strip():
                    payload["industry_id"] = update_industry.strip()
                if include_position:
                    payload["position_factor"] = float(update_position)

                if not payload:
                    st.error("Provide at least one field to update")
                else:
                    try:
                        url = _api_url(api_base, api_prefix, f"/companies/{update_id.strip()}")
                        out = _request_json("PUT", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                        _show_payload(out)
                    except requests.HTTPError as exc:
                        _show_http_error(exc)
                    except Exception as exc:
                        st.error(f"Update company failed: {exc}")

    with tabs[5]:
        delete_id = st.text_input("Company ID", key="companies_delete_id")
        if st.button("DELETE /companies/{company_id}", key="companies_delete_btn"):
            if not delete_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/companies/{delete_id.strip()}")
                    resp = _request("DELETE", url, timeout=timeout, headers=headers, verify=verify_tls)
                    if not resp.ok:
                        raise requests.HTTPError(resp.text, response=resp)
                    st.success(f"Deleted ({resp.status_code})")
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Delete company failed: {exc}")


# ============================================================
# Assessments
# ============================================================

with main_tabs[2]:
    tabs = st.tabs(["List", "Get", "Create", "Update Status", "List Scores", "Upsert Score"])

    with tabs[0]:
        page = st.number_input("Page", min_value=1, value=1, key="assessments_page")
        page_size = st.number_input("Page Size", min_value=1, max_value=100, value=20, key="assessments_page_size")
        company_id = st.text_input("Company ID filter (optional)", key="assessments_filter_company")
        if st.button("GET /assessments", key="assessments_list_btn"):
            try:
                params: dict[str, Any] = {"page": int(page), "page_size": int(page_size)}
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                url = _api_url(api_base, api_prefix, "/assessments")
                payload = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(payload)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List assessments failed: {exc}")

    with tabs[1]:
        assessment_id = st.text_input("Assessment ID", key="assessments_get_id")
        if st.button("GET /assessments/{id}", key="assessments_get_btn"):
            if not assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{assessment_id.strip()}")
                    payload = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(payload)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get assessment failed: {exc}")

    with tabs[2]:
        with st.form("assessments_create_form"):
            create_company_id = st.text_input("Company ID")
            assessment_type = st.selectbox("Assessment Type", ASSESSMENT_TYPES)
            assessment_date = st.date_input("Assessment Date", value=date.today())
            primary_assessor = st.text_input("Primary Assessor (optional)")
            secondary_assessor = st.text_input("Secondary Assessor (optional)")
            include_vr = st.checkbox("Include VR score", value=False)
            vr_score = st.number_input("VR Score", min_value=0.0, max_value=100.0, value=50.0)
            include_bounds = st.checkbox("Include confidence bounds", value=False)
            conf_lower = st.number_input("Confidence Lower", min_value=0.0, max_value=100.0, value=40.0)
            conf_upper = st.number_input("Confidence Upper", min_value=0.0, max_value=100.0, value=60.0)
            submitted = st.form_submit_button("POST /assessments")

        if submitted:
            if not create_company_id.strip():
                st.error("Company ID is required")
            else:
                payload: dict[str, Any] = {
                    "company_id": create_company_id.strip(),
                    "assessment_type": assessment_type,
                    "assessment_date": assessment_date.isoformat(),
                }
                if primary_assessor.strip():
                    payload["primary_assessor"] = primary_assessor.strip()
                if secondary_assessor.strip():
                    payload["secondary_assessor"] = secondary_assessor.strip()
                if include_vr:
                    payload["vr_score"] = float(vr_score)
                if include_bounds:
                    payload["confidence_lower"] = float(conf_lower)
                    payload["confidence_upper"] = float(conf_upper)

                try:
                    url = _api_url(api_base, api_prefix, "/assessments")
                    out = _request_json("POST", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Create assessment failed: {exc}")

    with tabs[3]:
        with st.form("assessments_status_form"):
            update_assessment_id = st.text_input("Assessment ID")
            status_value = st.selectbox("New Status", ASSESSMENT_STATUSES)
            submitted = st.form_submit_button("PATCH /assessments/{id}/status")

        if submitted:
            if not update_assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{update_assessment_id.strip()}/status")
                    out = _request_json(
                        "PATCH",
                        url,
                        json={"status": status_value},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Update status failed: {exc}")

    with tabs[4]:
        assessment_id = st.text_input("Assessment ID", key="assessments_scores_id")
        page = st.number_input("Page", min_value=1, value=1, key="assessments_scores_page")
        page_size = st.number_input("Page Size", min_value=1, max_value=100, value=20, key="assessments_scores_page_size")
        if st.button("GET /assessments/{id}/scores", key="assessments_scores_btn"):
            if not assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{assessment_id.strip()}/scores")
                    out = _request_json(
                        "GET",
                        url,
                        params={"page": int(page), "page_size": int(page_size)},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"List dimension scores failed: {exc}")

    with tabs[5]:
        with st.form("assessments_upsert_score_form"):
            score_assessment_id = st.text_input("Assessment ID")
            score_dimension = st.selectbox("Dimension", DIMENSIONS)
            score_value = st.number_input("Score", min_value=0.0, max_value=100.0, value=50.0)
            include_weight = st.checkbox("Include weight", value=False)
            weight_value = st.number_input("Weight", min_value=0.0, max_value=1.0, value=0.15)
            confidence_value = st.slider("Confidence", min_value=0.0, max_value=1.0, value=0.8)
            evidence_count = st.number_input("Evidence Count", min_value=0, value=0)
            submitted = st.form_submit_button("POST /assessments/{id}/scores")

        if submitted:
            if not score_assessment_id.strip():
                st.error("Assessment ID is required")
            else:
                payload: dict[str, Any] = {
                    "assessment_id": score_assessment_id.strip(),
                    "dimension": score_dimension,
                    "score": float(score_value),
                    "confidence": float(confidence_value),
                    "evidence_count": int(evidence_count),
                }
                if include_weight:
                    payload["weight"] = float(weight_value)

                try:
                    url = _api_url(api_base, api_prefix, f"/assessments/{score_assessment_id.strip()}/scores")
                    out = _request_json("POST", url, json=payload, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Upsert score failed: {exc}")

# ============================================================
# Collection
# ============================================================

with main_tabs[3]:
    tabs = st.tabs(["Collect Evidence", "Collect Signals", "Task Status"])

    with tabs[0]:
        with st.form("collection_evidence_form"):
            companies = st.text_input("Tickers (comma-separated) or 'all'", value="all")
            submitted = st.form_submit_button("POST /collection/evidence")

        if submitted:
            if not companies.strip():
                st.error("companies is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, "/collection/evidence")
                    out = _request_json(
                        "POST",
                        url,
                        params={"companies": companies.strip()},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    if isinstance(out, dict) and out.get("task_id"):
                        st.session_state["last_collection_task_id"] = out["task_id"]
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Collect evidence failed: {exc}")

    with tabs[1]:
        with st.form("collection_signals_form"):
            companies = st.text_input("Tickers (comma-separated) or 'all'", value="all", key="collection_signals_companies")
            submitted = st.form_submit_button("POST /collection/signals")

        if submitted:
            if not companies.strip():
                st.error("companies is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, "/collection/signals")
                    out = _request_json(
                        "POST",
                        url,
                        params={"companies": companies.strip()},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    if isinstance(out, dict) and out.get("task_id"):
                        st.session_state["last_collection_task_id"] = out["task_id"]
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Collect signals failed: {exc}")

    with tabs[2]:
        default_task = st.session_state.get("last_collection_task_id", "")
        task_id = st.text_input("Task ID", value=default_task)
        if st.button("GET /collection/tasks/{task_id}", key="collection_task_status_btn"):
            if not task_id.strip():
                st.error("Task ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/collection/tasks/{task_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get task status failed: {exc}")


# ============================================================
# Documents & Chunks
# ============================================================

with main_tabs[4]:
    tabs = st.tabs(["List Documents", "Get Document", "List Chunks", "Get Chunk"])

    with tabs[0]:
        ticker = st.text_input("Ticker filter (optional)", key="documents_list_ticker")
        company_id = st.text_input("Company ID filter (optional)", key="documents_list_company")
        limit = st.number_input("Limit", min_value=1, max_value=500, value=100, key="documents_list_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="documents_list_offset")
        if st.button("GET /documents", key="documents_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                url = _api_url(api_base, api_prefix, "/documents")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List documents failed: {exc}")

    with tabs[1]:
        document_id = st.text_input("Document ID", key="documents_get_id")
        if st.button("GET /documents/{document_id}", key="documents_get_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/documents/{document_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get document failed: {exc}")

    with tabs[2]:
        document_id = st.text_input("Document ID", key="chunks_list_document_id")
        limit = st.number_input("Limit", min_value=1, max_value=1000, value=200, key="chunks_list_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="chunks_list_offset")
        if st.button("GET /chunks/?document_id=...", key="chunks_list_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, "/chunks/")
                    out = _request_json(
                        "GET",
                        url,
                        params={"document_id": document_id.strip(), "limit": int(limit), "offset": int(offset)},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"List chunks failed: {exc}")

    with tabs[3]:
        chunk_id = st.text_input("Chunk ID", key="chunks_get_id")
        if st.button("GET /chunks/{chunk_id}", key="chunks_get_btn"):
            if not chunk_id.strip():
                st.error("Chunk ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/chunks/{chunk_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get chunk failed: {exc}")


# ============================================================
# Signals
# ============================================================

with main_tabs[5]:
    tabs = st.tabs(["List", "Get"])

    with tabs[0]:
        ticker = st.text_input("Ticker (optional)", key="signals_list_ticker")
        signal_type = st.text_input("Signal Type (optional)", key="signals_list_type")
        source = st.text_input("Source (optional)", key="signals_list_source")
        limit = st.number_input("Limit", min_value=1, max_value=500, value=100, key="signals_list_limit")
        if st.button("GET /signals", key="signals_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                if signal_type.strip():
                    params["signal_type"] = signal_type.strip().lower()
                if source.strip():
                    params["source"] = source.strip().lower()
                url = _api_url(api_base, api_prefix, "/signals")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List signals failed: {exc}")

    with tabs[1]:
        signal_id = st.text_input("Signal ID", key="signals_get_id")
        if st.button("GET /signals/{signal_id}", key="signals_get_btn"):
            if not signal_id.strip():
                st.error("Signal ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/signals/{signal_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get signal failed: {exc}")

# ============================================================
# Signal Summaries
# ============================================================

with main_tabs[6]:
    tabs = st.tabs(["List", "Compute"])

    with tabs[0]:
        ticker = st.text_input("Ticker (optional)", key="summaries_list_ticker")
        limit = st.number_input("Limit", min_value=1, max_value=200, value=50, key="summaries_list_limit")
        if st.button("GET /signal-summaries", key="summaries_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                url = _api_url(api_base, api_prefix, "/signal-summaries")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List signal summaries failed: {exc}")

    with tabs[1]:
        with st.form("summaries_compute_form"):
            ticker = st.text_input("Ticker", key="summaries_compute_ticker")
            include_as_of = st.checkbox("Include as_of date", value=False)
            as_of_date = st.date_input("as_of", value=date.today(), key="summaries_compute_as_of")
            submitted = st.form_submit_button("POST /signal-summaries/compute")

        if submitted:
            if not ticker.strip():
                st.error("Ticker is required")
            else:
                params: dict[str, Any] = {"ticker": ticker.strip().upper()}
                if include_as_of:
                    params["as_of"] = as_of_date.isoformat()
                try:
                    url = _api_url(api_base, api_prefix, "/signal-summaries/compute")
                    out = _request_json("POST", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Compute summary failed: {exc}")


# ============================================================
# Evidence
# ============================================================

with main_tabs[7]:
    tabs = st.tabs(["Stats", "List Documents", "Get Document", "Get Document Chunks"])

    with tabs[0]:
        if st.button("GET /evidence/stats", key="evidence_stats_btn"):
            try:
                url = _api_url(api_base, api_prefix, "/evidence/stats")
                out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Evidence stats failed: {exc}")

    with tabs[1]:
        ticker = st.text_input("Ticker (optional)", key="evidence_docs_ticker")
        company_id = st.text_input("Company ID (optional)", key="evidence_docs_company")
        limit = st.number_input("Limit", min_value=1, max_value=500, value=100, key="evidence_docs_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="evidence_docs_offset")
        if st.button("GET /evidence/documents", key="evidence_docs_list_btn"):
            try:
                params: dict[str, Any] = {"limit": int(limit), "offset": int(offset)}
                if ticker.strip():
                    params["ticker"] = ticker.strip().upper()
                if company_id.strip():
                    params["company_id"] = company_id.strip()
                url = _api_url(api_base, api_prefix, "/evidence/documents")
                out = _request_json("GET", url, params=params, timeout=timeout, headers=headers, verify=verify_tls)
                _show_payload(out)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"List evidence documents failed: {exc}")

    with tabs[2]:
        document_id = st.text_input("Document ID", key="evidence_doc_get_id")
        if st.button("GET /evidence/documents/{document_id}", key="evidence_doc_get_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/evidence/documents/{document_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get evidence document failed: {exc}")

    with tabs[3]:
        document_id = st.text_input("Document ID", key="evidence_chunks_doc_id")
        limit = st.number_input("Limit", min_value=1, max_value=1000, value=200, key="evidence_chunks_limit")
        offset = st.number_input("Offset", min_value=0, value=0, key="evidence_chunks_offset")
        if st.button("GET /evidence/documents/{document_id}/chunks", key="evidence_chunks_btn"):
            if not document_id.strip():
                st.error("Document ID is required")
            else:
                try:
                    url = _api_url(api_base, api_prefix, f"/evidence/documents/{document_id.strip()}/chunks")
                    out = _request_json(
                        "GET",
                        url,
                        params={"limit": int(limit), "offset": int(offset)},
                        timeout=timeout,
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get evidence chunks failed: {exc}")


# ============================================================
# Scoring
# ============================================================

with main_tabs[8]:
    tabs = st.tabs(["Compute", "Latest by Company", "Leaderboard", "Visuals"])

    with tabs[0]:
        with st.form("scoring_compute_form"):
            company_id = st.text_input("Company ID")
            version = st.text_input("Version", value="v1.0")
            submitted = st.form_submit_button("POST {scoring_prefix}/compute/{company_id}")

        if submitted:
            if not company_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _scoring_url(api_base, scoring_prefix, f"/compute/{company_id.strip()}")
                    out = _request_json(
                        "POST",
                        url,
                        params={"version": version.strip() or "v1.0"},
                        timeout=max(timeout, 60),
                        headers=headers,
                        verify=verify_tls,
                    )
                    _show_payload(out)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Compute scoring failed: {exc}")

    with tabs[1]:
        company_id = st.text_input("Company ID", key="scoring_results_company_id")
        if st.button("GET {scoring_prefix}/results/{company_id}", key="scoring_results_company_btn"):
            if not company_id.strip():
                st.error("Company ID is required")
            else:
                try:
                    url = _scoring_url(api_base, scoring_prefix, f"/results/{company_id.strip()}")
                    out = _request_json("GET", url, timeout=timeout, headers=headers, verify=verify_tls)
                    st.session_state["scoring_last_company"] = out
                    _show_payload(out)
                    records = _as_scoring_records(out)
                    if records:
                        name_map = _hydrate_company_names(
                            [str(records[0].get("company_id", ""))],
                            api_base,
                            api_prefix,
                            int(timeout),
                            headers,
                            verify_tls,
                        )
                        st.divider()
                        _render_company_breakdown(records[0], name_map)
                except requests.HTTPError as exc:
                    _show_http_error(exc)
                except Exception as exc:
                    st.error(f"Get latest company score failed: {exc}")

    with tabs[2]:
        limit = st.number_input("Limit", min_value=1, max_value=200, value=50, key="scoring_results_limit")
        if st.button("GET {scoring_prefix}/results", key="scoring_results_list_btn"):
            try:
                url = _scoring_url(api_base, scoring_prefix, "/results")
                out = _request_json(
                    "GET",
                    url,
                    params={"limit": int(limit)},
                    timeout=timeout,
                    headers=headers,
                    verify=verify_tls,
                )
                st.session_state["scoring_last_results"] = out
                _show_payload(out)
                records = _as_scoring_records(out)
                if records:
                    ids = [str(item.get("company_id", "")) for item in records]
                    name_map = _hydrate_company_names(
                        ids,
                        api_base,
                        api_prefix,
                        int(timeout),
                        headers,
                        verify_tls,
                    )
                    st.divider()
                    _render_scoring_visuals(records, name_map)
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Get score leaderboard failed: {exc}")

    with tabs[3]:
        st.caption("Interactive charts for leaderboard and company scoring outputs.")
        visual_limit = st.number_input(
            "Leaderboard Limit",
            min_value=1,
            max_value=200,
            value=50,
            key="scoring_visual_limit",
        )
        if st.button("Load Visual Dashboard", key="scoring_visual_load"):
            try:
                url = _scoring_url(api_base, scoring_prefix, "/results")
                out = _request_json(
                    "GET",
                    url,
                    params={"limit": int(visual_limit)},
                    timeout=timeout,
                    headers=headers,
                    verify=verify_tls,
                )
                st.session_state["scoring_last_results"] = out
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Load scoring visuals failed: {exc}")

        records = _as_scoring_records(st.session_state.get("scoring_last_results"))
        ids = [str(item.get("company_id", "")) for item in records]
        name_map = _hydrate_company_names(
            ids,
            api_base,
            api_prefix,
            int(timeout),
            headers,
            verify_tls,
        ) if ids else {}
        _render_scoring_visuals(records, name_map)

        company_record = _as_scoring_records(st.session_state.get("scoring_last_company"))
        if company_record:
            company_ids = [str(company_record[0].get("company_id", ""))]
            company_name_map = _hydrate_company_names(
                company_ids,
                api_base,
                api_prefix,
                int(timeout),
                headers,
                verify_tls,
            )
            st.divider()
            _render_company_breakdown(company_record[0], company_name_map)

# ============================================================
# Scripts
# ============================================================

with main_tabs[9]:
    scripts = _list_repo_scripts()

    st.caption("Run repository scripts from the Streamlit UI (working directory: repo root).")
    st.write(f"Scripts directory: `{SCRIPTS_DIR}`")

    if not scripts:
        st.warning("No scripts found")
    else:
        st.dataframe([{"script": s} for s in scripts], use_container_width=True)

        script_name = st.selectbox("Script", scripts, key="scripts_selected")
        script_args = st.text_input("Arguments", value="", key="scripts_args")
        script_timeout = st.number_input("Timeout (seconds)", min_value=1, max_value=7200, value=600, key="scripts_timeout")

        if st.button("Run Script", key="scripts_run_btn"):
            script_path = SCRIPTS_DIR / script_name
            if not script_path.exists():
                st.error(f"Script not found: {script_path}")
            else:
                cmd = [sys.executable, str(script_path)]
                if script_args.strip():
                    try:
                        cmd.extend(shlex.split(script_args.strip(), posix=(os.name != "nt")))
                    except ValueError as exc:
                        st.error(f"Invalid arguments: {exc}")
                        st.stop()

                st.code("$ " + " ".join(shlex.quote(x) for x in cmd), language="bash")
                try:
                    proc = subprocess.run(
                        cmd,
                        cwd=str(ROOT_DIR),
                        capture_output=True,
                        text=True,
                        timeout=int(script_timeout),
                    )
                    st.write(f"Exit code: {proc.returncode}")
                    if proc.stdout:
                        st.subheader("STDOUT")
                        st.code(proc.stdout)
                    if proc.stderr:
                        st.subheader("STDERR")
                        st.code(proc.stderr)
                except subprocess.TimeoutExpired as exc:
                    st.error(f"Script timed out after {script_timeout} seconds")
                    if exc.stdout:
                        st.subheader("Partial STDOUT")
                        st.code(exc.stdout)
                    if exc.stderr:
                        st.subheader("Partial STDERR")
                        st.code(exc.stderr)
                except Exception as exc:
                    st.error(f"Script execution failed: {exc}")

        st.divider()
        st.caption("Common examples")
        st.code(
            "python scripts/collect_evidence.py --companies all\n"
            "python scripts/collect_signals.py --companies NVDA,JPM\n"
            "python scripts/run_scoring_engine.py --batch --tickers NVDA,JPM --version v1.0"
        )


# ============================================================
# Raw API
# ============================================================

with main_tabs[10]:
    st.caption("Manual API console for any path/method.")

    method = st.selectbox("Method", ["GET", "POST", "PUT", "PATCH", "DELETE"], key="raw_method")
    path = st.text_input("Path", value="/companies", key="raw_path")
    include_prefix = st.checkbox("Include API Prefix", value=True, key="raw_include_prefix")
    params_text = st.text_area("Query Params JSON", value="{}", key="raw_params")
    body_text = st.text_area("Request Body JSON", value="{}", key="raw_body")

    if st.button("Send Request", key="raw_send_btn"):
        if not path.strip().startswith("/"):
            st.error("Path must start with '/'")
        else:
            ok_params, params_obj = _parse_json_input("Query Params", params_text)
            if not ok_params:
                st.stop()
            if not isinstance(params_obj, dict):
                st.error("Query Params JSON must be an object")
                st.stop()

            req_kwargs: dict[str, Any] = {
                "timeout": timeout,
                "headers": headers,
                "verify": verify_tls,
                "params": params_obj,
            }

            if method in {"POST", "PUT", "PATCH", "DELETE"}:
                ok_body, body_obj = _parse_json_input("Request Body", body_text)
                if not ok_body:
                    st.stop()
                if body_text.strip():
                    req_kwargs["json"] = body_obj

            try:
                url = _api_url(api_base, api_prefix, path.strip(), include_prefix=include_prefix)
                resp = _request(method, url, **req_kwargs)
                st.write(f"Status: {resp.status_code}")
                if not resp.ok:
                    raise requests.HTTPError(resp.text, response=resp)
                if resp.text.strip():
                    try:
                        _show_payload(resp.json())
                    except ValueError:
                        st.code(resp.text)
                else:
                    st.info("No content")
            except requests.HTTPError as exc:
                _show_http_error(exc)
            except Exception as exc:
                st.error(f"Raw request failed: {exc}")
