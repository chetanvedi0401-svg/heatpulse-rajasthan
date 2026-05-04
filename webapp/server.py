from __future__ import annotations

from datetime import datetime, timezone
from functools import lru_cache
from io import StringIO
import os
from pathlib import Path
import re
from typing import Any

import pandas as pd
from flask import Flask, jsonify, render_template, request, Response


ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "outputs"
DOCS = ROOT / "docs"

LIVE_FILE = OUTPUTS / "api_forecast_alerts_step9_guarded.csv"
TOP10_FILE = OUTPUTS / "api_latest_top10_guarded.csv"
COORD_FILE = DOCS / "step9_district_coordinates.csv"
LOG_FILE = ROOT / "logs" / "daily_run_log.csv"

ALERT_ORDER = ["RED", "ORANGE", "YELLOW", "GREEN"]

app = Flask(
    __name__,
    static_folder="static",
    template_folder="static",
)


def _safe_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


@lru_cache(maxsize=2)
def _cached_frames(live_mtime: float, top_mtime: float, coord_mtime: float):
    live = pd.read_csv(LIVE_FILE, parse_dates=["date"])
    live["district"] = (
        live["district"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.title()
    )
    live = live.sort_values(["date", "rank_within_date"]).reset_index(drop=True)

    top10 = pd.read_csv(TOP10_FILE, parse_dates=["date"]) if TOP10_FILE.exists() else pd.DataFrame()
    if not top10.empty:
        top10["district"] = (
            top10["district"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.title()
        )
        top10 = top10.sort_values(["date", "rank_within_date"]).reset_index(drop=True)

    coords = pd.read_csv(COORD_FILE) if COORD_FILE.exists() else pd.DataFrame(columns=["district", "latitude", "longitude"])
    if not coords.empty:
        coords["district"] = (
            coords["district"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.title()
        )

    return live, top10, coords


def get_frames():
    if not LIVE_FILE.exists():
        raise FileNotFoundError(f"Live forecast file missing: {LIVE_FILE}")
    live_mtime = LIVE_FILE.stat().st_mtime
    top_mtime = TOP10_FILE.stat().st_mtime if TOP10_FILE.exists() else 0.0
    coord_mtime = COORD_FILE.stat().st_mtime if COORD_FILE.exists() else 0.0
    return _cached_frames(live_mtime, top_mtime, coord_mtime)


def _extract_source_date(path_s: str) -> str:
    m = re.search(r"(\d{8})(?=\.csv$)", str(path_s or ""))
    if not m:
        return ""
    try:
        return str(datetime.strptime(m.group(1), "%Y%m%d").date())
    except Exception:
        return ""


def _runtime_status() -> dict[str, str]:
    out = {
        "pipeline_last_run": "",
        "pipeline_status": "",
        "api_status": "",
        "api_source_file": "",
        "api_source_date": "",
        "live_file_updated_at_utc": "",
        "pipeline_model_mode": "",
        "pipeline_model_name": "",
    }
    if LIVE_FILE.exists():
        out["live_file_updated_at_utc"] = datetime.fromtimestamp(
            LIVE_FILE.stat().st_mtime, tz=timezone.utc
        ).strftime("%Y-%m-%d %H:%M UTC")

    if not LOG_FILE.exists():
        return out

    try:
        logs = pd.read_csv(LOG_FILE)
        if logs.empty:
            return out
        row = logs.iloc[-1]
        out["pipeline_last_run"] = str(row.get("run_time", "") or "")
        out["pipeline_status"] = str(row.get("status", "") or "")
        out["api_status"] = str(row.get("api_status", "") or "")
        out["api_source_file"] = str(row.get("api_input_file", "") or "")
        out["api_source_date"] = _extract_source_date(out["api_source_file"])
        out["pipeline_model_mode"] = str(row.get("model_mode", "") or "")
        out["pipeline_model_name"] = str(row.get("model_name", "") or "")
    except Exception:
        return out

    return out


def alert_mix(df: pd.DataFrame) -> dict[str, int]:
    vc = df["alert_level_next_day"].value_counts()
    return {k: int(vc.get(k, 0)) for k in ALERT_ORDER}


@app.route("/")
def home():
    return render_template("index.html")


@app.after_request
def add_no_cache_headers(response):
    if request.path == "/" or request.path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
    return response


@app.route("/api/meta")
def api_meta():
    live, _, _ = get_frames()
    runtime = _runtime_status()
    dates = sorted(live["date"].dt.date.unique().tolist())
    districts = sorted(live["district"].unique().tolist())
    latest_date = dates[-1] if dates else None
    previous_date = dates[-2] if len(dates) > 1 else latest_date
    latest_df = live[live["date"].dt.date == latest_date] if latest_date else live.head(0)

    return jsonify(
        {
            "forecast_start": str(dates[0]) if dates else "",
            "forecast_end": str(dates[-1]) if dates else "",
            "dates": [str(d) for d in dates],
            "districts": districts,
            "latest_date": str(latest_date) if latest_date else "",
            "previous_date": str(previous_date) if previous_date else "",
            "alert_mix_latest": alert_mix(latest_df),
            "rows": int(len(live)),
            "live_file_updated_at_utc": runtime["live_file_updated_at_utc"],
            "pipeline_last_run": runtime["pipeline_last_run"],
            "pipeline_status": runtime["pipeline_status"],
            "pipeline_model_mode": runtime["pipeline_model_mode"],
            "pipeline_model_name": runtime["pipeline_model_name"],
            "api_status": runtime["api_status"],
            "api_source_file": runtime["api_source_file"],
            "api_source_date": runtime["api_source_date"],
        }
    )


@app.route("/api/day")
def api_day():
    live, _, coords = get_frames()
    date_s = request.args.get("date", "")
    if not date_s:
        date_s = str(live["date"].dt.date.max())

    day = live[live["date"].dt.date.astype(str) == date_s].copy()
    day = day.merge(coords, on="district", how="left")
    day = day.sort_values("rank_within_date")

    items = []
    for _, r in day.iterrows():
        items.append(
            {
                "date": str(pd.to_datetime(r["date"]).date()),
                "district": r["district"],
                "tmax_c": _safe_float(r["tmax_c"]),
                "tmin_c": _safe_float(r["tmin_c"]),
                "rain_mm": _safe_float(r["rain_mm"]),
                "risk_score_next_day": _safe_float(r["risk_score_next_day"]),
                "pred_prob_critical_t1": _safe_float(r["pred_prob_critical_t1"]),
                "alert_level_next_day": r["alert_level_next_day"],
                "rank_within_date": int(r["rank_within_date"]),
                "recommended_action": r["recommended_action"],
                "pipeline_mode": r.get("pipeline_mode", ""),
                "aqi_status": r.get("aqi_status", ""),
                "confidence_tag": r.get("confidence_tag", ""),
                "latitude": _safe_float(r.get("latitude", 0)),
                "longitude": _safe_float(r.get("longitude", 0)),
            }
        )

    return jsonify({"date": date_s, "count": len(items), "alert_mix": alert_mix(day), "items": items})


@app.route("/api/district")
def api_district():
    live, _, coords = get_frames()
    district = request.args.get("district", "").strip().title()
    date_s = request.args.get("date", "")
    if not district:
        return jsonify({"error": "district is required"}), 400
    if not date_s:
        date_s = str(live["date"].dt.date.max())

    row_df = live[(live["district"] == district) & (live["date"].dt.date.astype(str) == date_s)]
    if row_df.empty:
        return jsonify({"error": "no data"}), 404
    row = row_df.iloc[0]

    trend_df = live[live["district"] == district].sort_values("date")
    trend = [
        {
            "date": str(pd.to_datetime(r["date"]).date()),
            "risk_score_next_day": _safe_float(r["risk_score_next_day"]),
            "tmax_c": _safe_float(r["tmax_c"]),
            "tmin_c": _safe_float(r["tmin_c"]),
            "rain_mm": _safe_float(r["rain_mm"]),
            "alert_level_next_day": r["alert_level_next_day"],
        }
        for _, r in trend_df.iterrows()
    ]

    comparison = {
        "previous_date": "",
        "previous_alert_level": "",
        "previous_risk_score": None,
        "risk_delta": None,
    }
    prev_df = trend_df[trend_df["date"] < row["date"]]
    if not prev_df.empty:
        prev_row = prev_df.iloc[-1]
        prev_risk = _safe_float(prev_row["risk_score_next_day"])
        curr_risk = _safe_float(row["risk_score_next_day"])
        comparison = {
            "previous_date": str(pd.to_datetime(prev_row["date"]).date()),
            "previous_alert_level": prev_row["alert_level_next_day"],
            "previous_risk_score": prev_risk,
            "risk_delta": curr_risk - prev_risk,
        }

    lat = 0.0
    lon = 0.0
    if not coords.empty:
        c = coords[coords["district"] == district]
        if not c.empty:
            lat = _safe_float(c.iloc[0].get("latitude", 0))
            lon = _safe_float(c.iloc[0].get("longitude", 0))

    return jsonify(
        {
            "district": district,
            "date": date_s,
            "current": {
                "tmax_c": _safe_float(row["tmax_c"]),
                "tmin_c": _safe_float(row["tmin_c"]),
                "rain_mm": _safe_float(row["rain_mm"]),
                "risk_score_next_day": _safe_float(row["risk_score_next_day"]),
                "pred_prob_critical_t1": _safe_float(row["pred_prob_critical_t1"]),
                "alert_level_next_day": row["alert_level_next_day"],
                "recommended_action": row["recommended_action"],
                "confidence_tag": row.get("confidence_tag", ""),
                "heat_stress_score": _safe_float(row.get("heat_stress_score", 0)),
                "rank_within_date": int(row["rank_within_date"]),
                "latitude": lat,
                "longitude": lon,
            },
            "trend": trend,
            "comparison": comparison,
        }
    )


@app.route("/api/top10")
def api_top10():
    _, top10, _ = get_frames()
    if top10.empty:
        return jsonify({"rows": []})

    rows = []
    for _, r in top10.sort_values("rank_within_date").iterrows():
        rows.append(
            {
                "date": str(pd.to_datetime(r["date"]).date()),
                "district": r["district"],
                "tmax_c": _safe_float(r["tmax_c"]),
                "risk_score_next_day": _safe_float(r["risk_score_next_day"]),
                "alert_level_next_day": r["alert_level_next_day"],
                "recommended_action": r["recommended_action"],
                "rank_within_date": int(r["rank_within_date"]),
            }
        )
    return jsonify({"rows": rows})


@app.route("/api/risers")
def api_risers():
    live, _, _ = get_frames()
    dates = sorted(live["date"].dt.date.unique().tolist())
    if len(dates) < 2:
        return jsonify({"rows": []})

    prev_s = request.args.get("from_date", str(dates[-2]))
    curr_s = request.args.get("to_date", str(dates[-1]))

    prev = live[live["date"].dt.date.astype(str) == prev_s][["district", "risk_score_next_day"]].copy()
    prev = prev.rename(columns={"risk_score_next_day": "risk_prev"})
    curr = live[live["date"].dt.date.astype(str) == curr_s][
        ["district", "risk_score_next_day", "alert_level_next_day", "tmax_c", "recommended_action"]
    ].copy()
    curr = curr.rename(columns={"risk_score_next_day": "risk_curr"})

    x = curr.merge(prev, on="district", how="left")
    x["risk_prev"] = x["risk_prev"].fillna(0.0)
    x["risk_delta"] = x["risk_curr"] - x["risk_prev"]
    x = x.sort_values("risk_delta", ascending=False).reset_index(drop=True)

    rows = []
    for _, r in x.head(10).iterrows():
        rows.append(
            {
                "district": r["district"],
                "alert_level_next_day": r["alert_level_next_day"],
                "risk_prev": _safe_float(r["risk_prev"]),
                "risk_curr": _safe_float(r["risk_curr"]),
                "risk_delta": _safe_float(r["risk_delta"]),
                "tmax_c": _safe_float(r["tmax_c"]),
                "recommended_action": r["recommended_action"],
            }
        )
    return jsonify({"from_date": prev_s, "to_date": curr_s, "rows": rows})


@app.route("/download/day.csv")
def download_day_csv():
    live, _, _ = get_frames()
    date_s = request.args.get("date", str(live["date"].dt.date.max()))
    day = live[live["date"].dt.date.astype(str) == date_s].copy().sort_values("rank_within_date")
    day["date"] = pd.to_datetime(day["date"]).dt.date.astype(str)
    buffer = StringIO()
    day.to_csv(buffer, index=False)
    csv_text = buffer.getvalue()
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="heat_day_{date_s}.csv"'},
    )


@app.route("/download/top10.csv")
def download_top10_csv():
    _, top10, _ = get_frames()
    if top10.empty:
        return Response("", mimetype="text/csv")
    out = top10.copy().sort_values("rank_within_date")
    out["date"] = pd.to_datetime(out["date"]).dt.date.astype(str)
    buffer = StringIO()
    out.to_csv(buffer, index=False)
    csv_text = buffer.getvalue()
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={"Content-Disposition": 'attachment; filename="heat_top10_latest.csv"'},
    )


if __name__ == "__main__":
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "5050"))
    app.run(host=host, port=port, debug=False)
