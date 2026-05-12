
import os
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Resolve project root dynamically so the same script works on local machine and cloud runners.
ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
DOCS = ROOT / "docs"
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

cfg = pd.read_csv(DOCS / "step9_api_config_template.csv")
cfg_map = dict(zip(cfg["key"], cfg["value"]))

coords = pd.read_csv(DOCS / "step9_district_coordinates.csv")

base_url = cfg_map.get("base_url", "https://api.open-meteo.com/v1/forecast")
timezone = cfg_map.get("timezone", "Asia/Kolkata")
retry_count = int(cfg_map.get("retry_count", 3))
retry_backoff_sec = int(cfg_map.get("retry_backoff_sec", 2))

all_rows = []
today_tag = datetime.now().strftime("%Y%m%d")


def _build_session():
    session = requests.Session()
    retries = Retry(
        total=max(3, retry_count),
        connect=max(3, retry_count),
        read=max(3, retry_count),
        backoff_factor=max(1, retry_backoff_sec),
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=20, pool_maxsize=20)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": "heatwatch-rajasthan/1.0"})
    return session


def _rows_from_daily_payload(district, data):
    rows = []
    daily = data.get("daily", {}) if isinstance(data, dict) else {}
    dates = daily.get("time", [])
    tmax = daily.get("temperature_2m_max", [])
    tmin = daily.get("temperature_2m_min", [])
    rain = daily.get("precipitation_sum", [])
    sunrise = daily.get("sunrise", [])
    sunset = daily.get("sunset", [])
    for d, tx, tn, rn, sr, ss in zip(dates, tmax, tmin, rain, sunrise, sunset):
        sr_label = ""
        ss_label = ""
        try:
            sr_label = pd.to_datetime(sr).strftime("%I:%M %p")
        except Exception:
            sr_label = ""
        try:
            ss_label = pd.to_datetime(ss).strftime("%I:%M %p")
        except Exception:
            ss_label = ""
        rows.append(
            {
                "date": d,
                "district": district,
                "rain_mm": rn,
                "tmax_c": tx,
                "tmin_c": tn,
                "sunrise": sr,
                "sunset": ss,
                "sunrise_local": sr_label,
                "sunset_local": ss_label,
            }
        )
    return rows


def _fetch_batch(session):
    # Open-Meteo supports multiple coordinates in one request.
    districts = coords["district"].astype(str).tolist()
    lats = ",".join([str(float(x)) for x in coords["latitude"]])
    lons = ",".join([str(float(x)) for x in coords["longitude"]])

    params = {
        "latitude": lats,
        "longitude": lons,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,sunrise,sunset",
        "timezone": timezone,
        "forecast_days": 2,
    }

    r = session.get(base_url, params=params, timeout=(10, 40))
    r.raise_for_status()
    payload = r.json()

    rows = []
    if isinstance(payload, list):
        for i, item in enumerate(payload):
            if i >= len(districts):
                continue
            rows.extend(_rows_from_daily_payload(districts[i], item))
    elif isinstance(payload, dict):
        # Some runtimes may return a single-structure response.
        rows.extend(_rows_from_daily_payload(districts[0], payload))
    return rows


def _fetch_per_district(session):
    rows = []
    for _, row in coords.iterrows():
        district = row["district"]
        lat = float(row["latitude"])
        lon = float(row["longitude"])

        params = {
            "latitude": lat,
            "longitude": lon,
            "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,sunrise,sunset",
            "timezone": timezone,
            "forecast_days": 2,
        }

        ok = False
        last_err = None
        for _try in range(max(3, retry_count)):
            try:
                r = session.get(base_url, params=params, timeout=(10, 30))
                r.raise_for_status()
                rows.extend(_rows_from_daily_payload(district, r.json()))
                ok = True
                break
            except Exception as e:
                last_err = str(e)
                if _try < max(3, retry_count) - 1 and retry_backoff_sec > 0:
                    time.sleep(retry_backoff_sec * (_try + 1))
        if not ok:
            print(f"Failed district: {district} | error: {last_err}")
        time.sleep(0.15)
    return rows


session = _build_session()
batch_err = None
try:
    all_rows = _fetch_batch(session)
    if len(all_rows) < 20:
        raise RuntimeError(f"batch_rows_too_low={len(all_rows)}")
    print(f"Batch API fetch rows: {len(all_rows)}")
except Exception as e:
    batch_err = str(e)
    print(f"Batch fetch failed, falling back to per-district mode: {batch_err}")
    all_rows = _fetch_per_district(session)

if not all_rows:
    raise RuntimeError("No API rows fetched. Check connectivity or config.")

api_df = pd.DataFrame(all_rows)
api_df["tavg_c"] = (api_df["tmax_c"] + api_df["tmin_c"]) / 2.0

out_file = RAW / f"api_daily_weather_{today_tag}.csv"
api_df.to_csv(out_file, index=False)

print("Saved API daily weather file:", out_file)
print("Shape:", api_df.shape)
print(api_df.head())
