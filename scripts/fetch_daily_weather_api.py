
import pandas as pd
from pathlib import Path
import requests
from datetime import datetime
import os
import time

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

for _, row in coords.iterrows():
    district = row["district"]
    lat = float(row["latitude"])
    lon = float(row["longitude"])

    params = {
        "latitude": lat,
        "longitude": lon,
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": timezone,
        "forecast_days": 2
    }

    ok = False
    last_err = None
    for _try in range(retry_count):
        try:
            r = requests.get(base_url, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()

            daily = data.get("daily", {})
            dates = daily.get("time", [])
            tmax = daily.get("temperature_2m_max", [])
            tmin = daily.get("temperature_2m_min", [])
            rain = daily.get("precipitation_sum", [])

            for d, tx, tn, rn in zip(dates, tmax, tmin, rain):
                all_rows.append({
                    "date": d,
                    "district": district,
                    "rain_mm": rn,
                    "tmax_c": tx,
                    "tmin_c": tn
                })
            ok = True
            break
        except Exception as e:
            last_err = str(e)
            if _try < retry_count - 1 and retry_backoff_sec > 0:
                time.sleep(retry_backoff_sec)

    if not ok:
        print(f"Failed district: {district} | error: {last_err}")

if not all_rows:
    raise RuntimeError("No API rows fetched. Check connectivity or config.")

api_df = pd.DataFrame(all_rows)
api_df["tavg_c"] = (api_df["tmax_c"] + api_df["tmin_c"]) / 2.0

out_file = RAW / f"api_daily_weather_{today_tag}.csv"
api_df.to_csv(out_file, index=False)

print("Saved API daily weather file:", out_file)
print("Shape:", api_df.shape)
print(api_df.head())
