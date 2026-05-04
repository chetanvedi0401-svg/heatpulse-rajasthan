
import glob
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

# Resolve project root dynamically so the same script works on local machine and cloud runners.
ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
RAW = ROOT / "data" / "raw"
PROCESSED = ROOT / "data" / "processed"
OUTPUTS = ROOT / "outputs"
DOCS = ROOT / "docs"
MODELS = ROOT / "models"
LOGS = ROOT / "logs"
SCRIPTS = ROOT / "scripts"

for p in [RAW, OUTPUTS, LOGS]:
    p.mkdir(parents=True, exist_ok=True)

ACTION_MAP = {
    "GREEN": "Routine monitoring",
    "YELLOW": "Targeted surveillance + advisories",
    "ORANGE": "Heat-health preparedness activation",
    "RED": "Emergency heat-health action",
}

API_REQUIRED_COLS = ["date", "district", "rain_mm", "tmax_c", "tmin_c", "tavg_c"]


def write_csv_resilient(df, target_file, index=False, retries=5, base_wait_sec=1.0):
    """
    Try writing to target CSV with retries.
    If target stays locked, write a timestamped fallback copy so pipeline does not fail.
    Returns: (written_path, used_fallback, warning_message)
    """
    target = Path(target_file)
    target.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, retries + 1):
        try:
            df.to_csv(target, index=index)
            return str(target), False, ""
        except PermissionError as err:
            if attempt < retries:
                time.sleep(base_wait_sec * attempt)
                continue

            fallback_dir = target.parent / "_write_fallback"
            fallback_dir.mkdir(parents=True, exist_ok=True)
            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            fallback_file = fallback_dir / f"{target.stem}_{stamp}{target.suffix}"
            try:
                df.to_csv(fallback_file, index=index)
            except Exception:
                raise err

            warning = (
                f"locked_target={target}; wrote_fallback={fallback_file}; "
                "close any open CSV/Excel handle to resume normal writes"
            )
            return str(fallback_file), True, warning


def normalize_district(s):
    return s.astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.title()


def load_expected_districts():
    df = pd.read_csv(DOCS / "step9_district_coordinates.csv")
    return sorted(normalize_district(df["district"]).unique().tolist())


def evaluate_api_quality(api_file, expected_districts):
    out = {
        "api_file": str(api_file),
        "is_valid": False,
        "reason": "",
        "rows": 0,
        "unique_dates": 0,
        "expected_districts": len(expected_districts),
        "observed_districts": 0,
        "min_districts_per_date": 0,
        "max_districts_per_date": 0,
        "duplicate_date_district": 0,
        "null_core_fields": 0,
        "invalid_temp_rows": 0,
        "invalid_rain_rows": 0,
    }
    try:
        df = pd.read_csv(api_file)
    except Exception as e:
        out["reason"] = f"read_error: {e}"
        return out

    missing = [c for c in API_REQUIRED_COLS if c not in df.columns]
    if missing:
        out["reason"] = f"missing_columns: {missing}"
        return out

    df = df[API_REQUIRED_COLS].copy()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["district"] = normalize_district(df["district"])
    for c in ["rain_mm", "tmax_c", "tmin_c", "tavg_c"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    out["rows"] = int(len(df))
    out["unique_dates"] = int(df["date"].nunique())
    out["duplicate_date_district"] = int(df.duplicated(subset=["date", "district"]).sum())

    core_null = df[["date", "district", "rain_mm", "tmax_c", "tmin_c"]].isna().any(axis=1)
    out["null_core_fields"] = int(core_null.sum())

    invalid_temp = (df["tmax_c"] < -10) | (df["tmax_c"] > 60) | (df["tmin_c"] < -15) | (df["tmin_c"] > 45)
    out["invalid_temp_rows"] = int(invalid_temp.sum())

    invalid_rain = (df["rain_mm"] < 0) | (df["rain_mm"] > 1000)
    out["invalid_rain_rows"] = int(invalid_rain.sum())

    observed = set(df["district"].dropna().unique().tolist())
    out["observed_districts"] = int(len(observed))
    by_date = df.groupby(df["date"].dt.normalize())["district"].nunique() if len(df) else pd.Series(dtype=int)
    out["min_districts_per_date"] = int(by_date.min()) if len(by_date) else 0
    out["max_districts_per_date"] = int(by_date.max()) if len(by_date) else 0

    expected_count = len(expected_districts)
    min_required = max(20, int(np.floor(expected_count * 0.90)))

    checks = [
        out["rows"] > 0,
        out["unique_dates"] >= 1,
        out["duplicate_date_district"] == 0,
        out["null_core_fields"] == 0,
        out["invalid_temp_rows"] == 0,
        out["invalid_rain_rows"] == 0,
        out["observed_districts"] >= min_required,
        out["min_districts_per_date"] >= min_required,
    ]
    out["is_valid"] = bool(all(checks))
    if out["is_valid"]:
        out["reason"] = "ok"
    else:
        out["reason"] = (
            f"quality_fail(rows={out['rows']}, dates={out['unique_dates']}, "
            f"obs_dist={out['observed_districts']}, min_dist_date={out['min_districts_per_date']}, "
            f"dup={out['duplicate_date_district']}, null={out['null_core_fields']}, "
            f"bad_temp={out['invalid_temp_rows']}, bad_rain={out['invalid_rain_rows']})"
        )
    return out


def list_valid_api_files(expected_districts):
    files = sorted(glob.glob(str(RAW / "api_daily_weather_*.csv")))
    valid_files = []
    quality_rows = []
    for f in files:
        q = evaluate_api_quality(f, expected_districts)
        quality_rows.append(q)
        if q["is_valid"]:
            valid_files.append(Path(f))
    return valid_files, quality_rows


def append_api_quality_gate_log(row):
    qfile = DOCS / "step10_api_quality_gate_report.csv"
    frame = pd.DataFrame([row])
    if qfile.exists():
        old = pd.read_csv(qfile)
        frame = pd.concat([old, frame], ignore_index=True)
    write_csv_resilient(frame, qfile, index=False)
    return qfile


def patch_sklearn_model_compat(clf):
    """
    Make old pickled sklearn estimators compatible with newer runtime versions.
    Some LogisticRegression pickles created in older versions may miss attributes
    that newer sklearn accesses during predict_proba.
    """
    try:
        candidates = []
        if hasattr(clf, "steps"):
            candidates.extend([step_model for _, step_model in clf.steps])
        candidates.append(clf)

        for obj in candidates:
            if obj.__class__.__name__ == "LogisticRegression":
                if not hasattr(obj, "multi_class"):
                    obj.multi_class = "auto"
    except Exception:
        # Non-blocking safety patch; never fail pipeline for compatibility patch.
        pass
    return clf


def load_model_artifacts():
    stronger_model_file = MODELS / "heatwave_model_step14_stronger.joblib"
    strong_table_file = PROCESSED / "model_table_step12_heatwave_v2.csv"

    if stronger_model_file.exists() and strong_table_file.exists():
        bundle = joblib.load(stronger_model_file)
        model_table = pd.read_csv(strong_table_file, parse_dates=["date"])
        model_features = [str(x).strip() for x in bundle.get("features", []) if str(x).strip()]
        best_thr = float(bundle.get("median_threshold", 0.5))
        model_obj = bundle.get("artifact", bundle)
        model_name = str(bundle.get("model_name", "stronger_model"))
        return model_table, model_obj, model_features, best_thr, "strong_v4", model_name

    strong_model_file = MODELS / "heatwave_model_step13_strong.joblib"

    if strong_model_file.exists() and strong_table_file.exists():
        bundle = joblib.load(strong_model_file)
        model_table = pd.read_csv(strong_table_file, parse_dates=["date"])
        model_features = [str(x).strip() for x in bundle.get("features", []) if str(x).strip()]
        best_thr = float(bundle.get("median_threshold", 0.5))
        model_obj = bundle.get("artifact", bundle)
        model_name = str(bundle.get("model_name", "strong_model"))
        return model_table, model_obj, model_features, best_thr, "strong_v3", model_name

    model_table_file = PROCESSED / "model_table_step3.csv"
    model_file = MODELS / "baseline_logistic_step4.joblib"
    model_table = pd.read_csv(model_table_file, parse_dates=["date"])
    clf = joblib.load(model_file)
    clf = patch_sklearn_model_compat(clf)

    feature_file = DOCS / "step4_model_features.txt"
    with open(feature_file, "r", encoding="utf-8") as f:
        model_features = [line.strip() for line in f if line.strip()]

    thr_df = pd.read_csv(DOCS / "step4_threshold_tuning.csv")
    best_thr = float(thr_df.sort_values("val_f1", ascending=False).iloc[0]["threshold"])
    return model_table, clf, model_features, best_thr, "legacy_v1", "baseline_logistic_step4"


def predict_prob(model_obj, x_df):
    if isinstance(model_obj, dict) and model_obj.get("type") == "ensemble":
        weights = model_obj.get("weights", [0.25, 0.35, 0.40])
        models = model_obj.get("models", {})
        lg = models.get("lg")
        rf = models.get("rf")
        gb = models.get("hgb")
        probs = []
        if lg is not None:
            probs.append(float(weights[0]) * lg.predict_proba(x_df)[:, 1])
        if rf is not None:
            probs.append(float(weights[1]) * rf.predict_proba(x_df)[:, 1])
        if gb is not None:
            probs.append(float(weights[2]) * gb.predict_proba(x_df)[:, 1])
        if not probs:
            raise RuntimeError("Ensemble model has no usable sub-models.")
        return np.sum(probs, axis=0)
    if isinstance(model_obj, dict) and model_obj.get("type") == "single":
        inner = model_obj.get("model")
        if inner is None:
            raise RuntimeError("Single-model bundle missing model object.")
        return inner.predict_proba(x_df)[:, 1]
    return model_obj.predict_proba(x_df)[:, 1]


def wet_bulb_stull_c(t_c, rh_pct):
    rh = rh_pct.clip(lower=5.0, upper=99.0)
    return (
        t_c * np.arctan(0.151977 * np.sqrt(rh + 8.313659))
        + np.arctan(t_c + rh)
        - np.arctan(rh - 1.676331)
        + 0.00391838 * np.power(rh, 1.5) * np.arctan(0.023101 * rh)
        - 4.686035
    )


def heat_index_noaa_c(t_c, rh_pct):
    t_f = (t_c * 9.0 / 5.0) + 32.0
    rh = rh_pct.clip(lower=1.0, upper=100.0)
    hi_simple = 0.5 * (t_f + 61.0 + ((t_f - 68.0) * 1.2) + (rh * 0.094))
    hi_simple = (hi_simple + t_f) / 2.0
    hi_reg = (
        -42.379
        + 2.04901523 * t_f
        + 10.14333127 * rh
        - 0.22475541 * t_f * rh
        - 0.00683783 * (t_f ** 2)
        - 0.05481717 * (rh ** 2)
        + 0.00122874 * (t_f ** 2) * rh
        + 0.00085282 * t_f * (rh ** 2)
        - 0.00000199 * (t_f ** 2) * (rh ** 2)
    )
    use_reg = hi_simple >= 80.0
    hi_f = np.where(use_reg, hi_reg, hi_simple)
    low_sqrt_term = ((17.0 - np.abs(t_f - 95.0)) / 17.0).clip(lower=0)
    adj_low = ((13.0 - rh) / 4.0) * np.sqrt(low_sqrt_term)
    low_mask = (rh < 13.0) & (t_f >= 80.0) & (t_f <= 112.0)
    hi_f = np.where(low_mask, hi_f - adj_low, hi_f)
    adj_high = ((rh - 85.0) / 10.0) * ((87.0 - t_f) / 5.0)
    high_mask = (rh > 85.0) & (t_f >= 80.0) & (t_f <= 87.0)
    hi_f = np.where(high_mask, hi_f + adj_high, hi_f)
    return (hi_f - 32.0) * 5.0 / 9.0


def add_v3_features(df):
    out = df.copy()
    out["diurnal_range_c"] = (out["tmax_c"] - out["tmin_c"]).clip(lower=0.1)
    out["humidity_proxy_pct"] = (
        76.0
        - 2.4 * (out["diurnal_range_c"] - 10.0)
        + 1.1 * out["rain_roll7"].clip(lower=0, upper=25)
        + 0.9 * out["tmin_anom"].clip(lower=-3, upper=8)
    ).clip(lower=12, upper=98)
    out["dewpoint_proxy_c"] = out["tmin_c"] - ((100.0 - out["humidity_proxy_pct"]) / 5.0)
    out["wetbulb_proxy_c"] = wet_bulb_stull_c(out["tavg_c"], out["humidity_proxy_pct"])
    out["heat_index_proxy_c"] = heat_index_noaa_c(out["tmax_c"], out["humidity_proxy_pct"])
    out["night_heat_index"] = (
        0.6 * out["tmin_anom"].clip(lower=0) * 8.0
        + 0.4 * (out["humidity_proxy_pct"] - 40.0).clip(lower=0)
    ).clip(lower=0, upper=100)
    out["moisture_stress_index"] = (
        0.45 * (out["humidity_proxy_pct"] - 30.0).clip(lower=0)
        + 0.35 * out["wetbulb_proxy_c"].clip(lower=18, upper=35)
        + 0.20 * out["heat_index_proxy_c"].clip(lower=25, upper=55)
    ).clip(lower=0, upper=100)
    out["wetbulb_roll3"] = out.groupby("district")["wetbulb_proxy_c"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    out["humidity_roll3"] = out.groupby("district")["humidity_proxy_pct"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    return out


def map_alert(prob, score, thr, model_mode="legacy_v1"):
    if model_mode in {"strong_v3", "strong_v4"}:
        red_cut = max(min(thr + 0.55, 0.90), 0.75)
        orange_cut = max(min(thr + 0.35, 0.80), 0.55)
        yellow_cut = max(min(thr + 0.20, 0.65), 0.40)
        if (prob >= red_cut) or (score >= 80):
            return "RED"
        if (prob >= orange_cut) or (score >= 60):
            return "ORANGE"
        if (prob >= yellow_cut) or (score >= 40):
            return "YELLOW"
        return "GREEN"

    if (prob >= thr) or (score >= 80):
        return "RED"
    if (prob >= 0.65) or (score >= 60):
        return "ORANGE"
    if (prob >= 0.40) or (score >= 40):
        return "YELLOW"
    return "GREEN"


def upsert_history_by_date_district(existing_file, incoming_df):
    """
    Keep full historical rows forever and upsert latest records by (date, district).
    This guarantees no old date is deleted on the next pipeline run.
    """
    key_cols = ["date", "district"]
    incoming = incoming_df.copy()
    incoming["date"] = pd.to_datetime(incoming["date"])

    if existing_file.exists():
        old = pd.read_csv(existing_file, parse_dates=["date"])
        combined = pd.concat([old, incoming], ignore_index=True, sort=False)
    else:
        combined = incoming

    # Upsert policy: latest incoming value wins for same (date, district)
    combined = combined.drop_duplicates(subset=key_cols, keep="last")

    sort_cols = [c for c in ["date", "rank_within_date", "district"] if c in combined.columns]
    if sort_cols:
        combined = combined.sort_values(sort_cols).reset_index(drop=True)
    else:
        combined = combined.sort_values(["date", "district"]).reset_index(drop=True)
    return combined


def build_historical_auto_alerts(model_table, model_obj, model_features, best_thr, model_mode):
    x_all = model_table[model_features].copy()
    all_prob = predict_prob(model_obj, x_all)
    all_pred = (all_prob >= best_thr).astype(int)

    score_col = "target_heatwave_score_t1" if "target_heatwave_score_t1" in model_table.columns else "target_heat_score_t1"
    alerts_df = model_table[["date", "district", score_col]].copy()
    alerts_df = alerts_df.rename(columns={score_col: "target_score_t1"})
    alerts_df["pred_prob_critical_t1"] = all_prob
    alerts_df["pred_prob_heatwave_t1"] = all_prob
    alerts_df["pred_critical_t1"] = all_pred
    alerts_df["pred_heatwave_t1"] = all_pred
    alerts_df["risk_score_next_day"] = (
        0.65 * alerts_df["target_score_t1"].clip(0, 100)
        + 0.35 * (alerts_df["pred_prob_critical_t1"] * 100)
    ).clip(0, 100)

    alerts_df["alert_level_next_day"] = [
        map_alert(p, s, best_thr, model_mode=model_mode)
        for p, s in zip(alerts_df["pred_prob_critical_t1"], alerts_df["risk_score_next_day"])
    ]

    alerts_df["rank_within_date"] = (
        alerts_df.groupby("date")["risk_score_next_day"].rank(method="first", ascending=False).astype(int)
    )
    if model_mode == "strong_v4":
        alerts_df["pipeline_mode"] = "Heatwave-v4"
    elif model_mode == "strong_v3":
        alerts_df["pipeline_mode"] = "Heatwave-v3"
    else:
        alerts_df["pipeline_mode"] = "Heat-only"
    alerts_df["aqi_status"] = "excluded"
    alerts_df["confidence_tag"] = np.select(
        [alerts_df["pred_prob_critical_t1"] >= 0.85, alerts_df["pred_prob_critical_t1"] >= 0.65],
        ["High", "Medium"],
        default="Low",
    )
    alerts_df["recommended_action"] = alerts_df["alert_level_next_day"].map(ACTION_MAP)
    alerts_df = alerts_df.sort_values(["date", "rank_within_date"]).reset_index(drop=True)

    full_file = OUTPUTS / "daily_alerts_auto.csv"
    top10_file = OUTPUTS / "daily_top10_auto.csv"
    latest_top10_file = OUTPUTS / "latest_day_top10_auto.csv"
    write_warnings = []

    _, fb1, w1 = write_csv_resilient(alerts_df, full_file, index=False)
    if fb1 and w1:
        write_warnings.append(w1)

    top10_df = alerts_df[alerts_df["rank_within_date"] <= 10].copy()
    _, fb2, w2 = write_csv_resilient(top10_df, top10_file, index=False)
    if fb2 and w2:
        write_warnings.append(w2)

    latest_date = alerts_df["date"].max()
    latest_top10_df = alerts_df[
        (alerts_df["date"] == latest_date) & (alerts_df["rank_within_date"] <= 10)
    ].copy()
    _, fb3, w3 = write_csv_resilient(latest_top10_df, latest_top10_file, index=False)
    if fb3 and w3:
        write_warnings.append(w3)

    return {
        "rows": int(len(alerts_df)),
        "latest_date": str(pd.to_datetime(latest_date).date()),
        "full_file": str(full_file),
        "top10_file": str(top10_file),
        "latest_top10_file": str(latest_top10_file),
        "write_warnings": " | ".join(write_warnings),
    }


def fetch_latest_api_file():
    fetch_script = SCRIPTS / "fetch_daily_weather_api.py"
    proc = subprocess.run([sys.executable, str(fetch_script)], capture_output=True, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"API fetch failed.\nSTDOUT:\n{proc.stdout}\nSTDERR:\n{proc.stderr}")

    files = sorted(glob.glob(str(RAW / "api_daily_weather_*.csv")))
    if not files:
        raise FileNotFoundError("API fetch script ran but no api_daily_weather_*.csv file found.")
    return Path(files[-1]), proc.stdout.strip()


def latest_existing_api_file():
    files = sorted(glob.glob(str(RAW / "api_daily_weather_*.csv")))
    if not files:
        return None
    return Path(files[-1])


def build_api_live_guarded_alerts(api_file, model_obj, model_features, best_thr, model_mode, api_files_for_archive=None):
    # Load full raw API archive so historical forecast dates are always retained.
    if api_files_for_archive is None:
        api_files = sorted(glob.glob(str(RAW / "api_daily_weather_*.csv")))
    else:
        api_files = [str(p) for p in api_files_for_archive]
    if not api_files:
        raise FileNotFoundError("No api_daily_weather_*.csv files found in data/raw.")
    api_parts = [pd.read_csv(f, parse_dates=["date"]) for f in api_files]
    api_df = pd.concat(api_parts, ignore_index=True)
    api_df = (
        api_df.sort_values(["date", "district"])
        .drop_duplicates(subset=["date", "district"], keep="last")
        .reset_index(drop=True)
    )
    hist_df = pd.read_csv(PROCESSED / "district_daily_climate_2017_2025_clean.csv", parse_dates=["date"])

    for df_ in [api_df, hist_df]:
        df_["district"] = normalize_district(df_["district"])

    api_df = api_df[["date", "district", "rain_mm", "tmax_c", "tmin_c", "tavg_c"]].copy()
    hist_df = hist_df[["date", "district", "rain_mm", "tmax_c", "tmin_c", "tavg_c"]].copy()

    hist_tmp = hist_df.copy()
    hist_tmp["month"] = hist_tmp["date"].dt.month
    clim = (
        hist_tmp.groupby(["district", "month"], as_index=False)
        .agg(
            tmax_month_mean=("tmax_c", "mean"),
            tmin_month_mean=("tmin_c", "mean"),
            tavg_month_mean=("tavg_c", "mean"),
            rain_month_mean=("rain_mm", "mean"),
        )
    )

    combo = pd.concat([hist_df, api_df], ignore_index=True).sort_values(["district", "date"]).reset_index(drop=True)
    combo["year"] = combo["date"].dt.year
    combo["month"] = combo["date"].dt.month
    combo["day"] = combo["date"].dt.day
    combo["doy"] = combo["date"].dt.dayofyear
    combo["weekofyear"] = combo["date"].dt.isocalendar().week.astype(int)
    combo = combo.merge(clim, on=["district", "month"], how="left")

    combo["tmax_anom"] = combo["tmax_c"] - combo["tmax_month_mean"]
    combo["tmin_anom"] = combo["tmin_c"] - combo["tmin_month_mean"]
    combo["tavg_anom"] = combo["tavg_c"] - combo["tavg_month_mean"]
    combo["rain_anom"] = combo["rain_mm"] - combo["rain_month_mean"]
    combo["tmax_roll3"] = combo.groupby("district")["tmax_c"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    combo["tmin_roll3"] = combo.groupby("district")["tmin_c"].transform(lambda s: s.rolling(3, min_periods=1).mean())
    combo["rain_roll7"] = combo.groupby("district")["rain_mm"].transform(lambda s: s.rolling(7, min_periods=1).sum())
    combo["tmax_lag1"] = combo.groupby("district")["tmax_c"].shift(1)
    combo["tmin_lag1"] = combo.groupby("district")["tmin_c"].shift(1)
    combo["rain_lag1"] = combo.groupby("district")["rain_mm"].shift(1)
    combo["dry_day"] = (combo["rain_mm"] == 0).astype(int)
    combo["dry_spell_3d"] = combo.groupby("district")["dry_day"].transform(lambda s: s.rolling(3, min_periods=1).sum())
    combo["dry_spell_7d"] = combo.groupby("district")["dry_day"].transform(lambda s: s.rolling(7, min_periods=1).sum())
    combo["heat_stress_score"] = (
        combo["tmax_anom"].clip(lower=0) * 6.0
        + combo["tmin_anom"].clip(lower=0) * 3.0
        + combo["dry_spell_7d"] * 1.5
    ).clip(lower=0, upper=100)
    combo = add_v3_features(combo)

    api_dates = set(api_df["date"].dt.normalize())
    live_df = combo[combo["date"].dt.normalize().isin(api_dates)].copy()

    x_live = live_df[model_features].copy()
    live_prob = predict_prob(model_obj, x_live)
    live_pred = (live_prob >= best_thr).astype(int)

    live_out = live_df[
        ["date", "district", "rain_mm", "tmax_c", "tmin_c", "tavg_c", "heat_stress_score"]
    ].copy()
    live_out["pred_prob_critical_t1"] = live_prob
    live_out["pred_prob_heatwave_t1"] = live_prob
    live_out["pred_critical_t1"] = live_pred
    live_out["pred_heatwave_t1"] = live_pred
    live_out["risk_score_next_day"] = (
        0.65 * live_out["heat_stress_score"] + 0.35 * (live_out["pred_prob_critical_t1"] * 100)
    ).clip(0, 100)

    live_out["alert_level_next_day"] = [
        map_alert(p, s, best_thr, model_mode=model_mode)
        for p, s in zip(live_out["pred_prob_critical_t1"], live_out["risk_score_next_day"])
    ]
    live_out["rank_within_date"] = (
        live_out.groupby("date")["risk_score_next_day"].rank(method="first", ascending=False).astype(int)
    )
    if model_mode == "strong_v4":
        live_out["pipeline_mode"] = "Heatwave-v4"
    elif model_mode == "strong_v3":
        live_out["pipeline_mode"] = "Heatwave-v3"
    else:
        live_out["pipeline_mode"] = "Heat-only"
    live_out["aqi_status"] = "excluded"
    live_out["confidence_tag"] = np.select(
        [live_out["pred_prob_critical_t1"] >= 0.85, live_out["pred_prob_critical_t1"] >= 0.65],
        ["High", "Medium"],
        default="Low",
    )
    live_out["recommended_action"] = live_out["alert_level_next_day"].map(ACTION_MAP)
    live_out = live_out.sort_values(["date", "rank_within_date"]).reset_index(drop=True)

    raw_live_file = OUTPUTS / "api_forecast_alerts_step9.csv"
    # Keep raw API forecast as full historical table (no date deletion).
    live_out_full = upsert_history_by_date_district(raw_live_file, live_out)
    write_warnings = []
    _, fb_raw, w_raw = write_csv_resilient(live_out_full, raw_live_file, index=False)
    if fb_raw and w_raw:
        write_warnings.append(w_raw)

    # Guardrail to avoid over-alerting
    mix = live_out["alert_level_next_day"].value_counts(normalize=True).mul(100)
    red_pct = float(mix.get("RED", 0.0))
    guarded = live_out.copy()

    if red_pct > 40:
        # Rebalance per forecast date so one hot day does not force all other days to GREEN.
        # Absolute score floors keep moderate-risk days visible in YELLOW/ORANGE buckets.
        def rebalance_alert(prob, score, q_y, q_o, q_r):
            if (prob >= 0.92 and score >= max(q_r, 58.0)) or (score >= 78.0):
                return "RED"
            if (prob >= 0.78 and score >= max(q_o, 48.0)) or (score >= 65.0):
                return "ORANGE"
            if (prob >= 0.50 and score >= max(q_y, 36.0)) or (score >= 45.0):
                return "YELLOW"
            return "GREEN"

        guarded_parts = []
        for _, day_df in guarded.groupby("date", sort=False):
            day_df = day_df.copy()
            q_y = float(day_df["risk_score_next_day"].quantile(0.65))
            q_o = float(day_df["risk_score_next_day"].quantile(0.85))
            q_r = float(day_df["risk_score_next_day"].quantile(0.95))
            day_df["alert_level_next_day"] = [
                rebalance_alert(p, s, q_y, q_o, q_r)
                for p, s in zip(day_df["pred_prob_critical_t1"], day_df["risk_score_next_day"])
            ]
            guarded_parts.append(day_df)

        guarded = pd.concat(guarded_parts, ignore_index=True).sort_values(["date", "rank_within_date"]).reset_index(
            drop=True
        )
        guarded["recommended_action"] = guarded["alert_level_next_day"].map(ACTION_MAP)
        guarded["confidence_tag"] = np.select(
            [guarded["pred_prob_critical_t1"] >= 0.90, guarded["pred_prob_critical_t1"] >= 0.75],
            ["High", "Medium"],
            default="Low",
        )

    guarded_file = OUTPUTS / "api_forecast_alerts_step9_guarded.csv"
    guarded_full = upsert_history_by_date_district(guarded_file, guarded)
    _, fb_guarded, w_guarded = write_csv_resilient(guarded_full, guarded_file, index=False)
    if fb_guarded and w_guarded:
        write_warnings.append(w_guarded)

    # Historical top10 (all days) + latest day top10
    historical_top10_file = OUTPUTS / "api_daily_top10_guarded.csv"
    hist_top10_df = guarded_full[guarded_full["rank_within_date"] <= 10].copy()
    _, fb_hist_top10, w_hist_top10 = write_csv_resilient(hist_top10_df, historical_top10_file, index=False)
    if fb_hist_top10 and w_hist_top10:
        write_warnings.append(w_hist_top10)

    latest_date = guarded_full["date"].max()
    latest_top10_file = OUTPUTS / "api_latest_top10_guarded.csv"
    latest_top10_df = guarded_full[
        (guarded_full["date"] == latest_date) & (guarded_full["rank_within_date"] <= 10)
    ].copy()
    _, fb_latest_top10, w_latest_top10 = write_csv_resilient(latest_top10_df, latest_top10_file, index=False)
    if fb_latest_top10 and w_latest_top10:
        write_warnings.append(w_latest_top10)

    # Reliability snapshot: confirms historical retention and zero key duplication.
    rel_file = DOCS / "step10_data_reliability_report.csv"
    raw_dup = int(live_out_full.duplicated(subset=["date", "district"]).sum())
    guarded_dup = int(guarded_full.duplicated(subset=["date", "district"]).sum())
    rel_row = pd.DataFrame(
        [
            {
                "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "raw_rows_total": int(len(live_out_full)),
                "raw_unique_dates": int(pd.to_datetime(live_out_full["date"]).nunique()),
                "raw_start_date": str(pd.to_datetime(live_out_full["date"]).min().date()),
                "raw_end_date": str(pd.to_datetime(live_out_full["date"]).max().date()),
                "raw_duplicate_date_district": raw_dup,
                "guarded_rows_total": int(len(guarded_full)),
                "guarded_unique_dates": int(pd.to_datetime(guarded_full["date"]).nunique()),
                "guarded_start_date": str(pd.to_datetime(guarded_full["date"]).min().date()),
                "guarded_end_date": str(pd.to_datetime(guarded_full["date"]).max().date()),
                "guarded_duplicate_date_district": guarded_dup,
                "historical_top10_rows": int(len(guarded_full[guarded_full["rank_within_date"] <= 10])),
                "status": "ok" if raw_dup == 0 and guarded_dup == 0 else "check",
            }
        ]
    )
    if rel_file.exists():
        old_rel = pd.read_csv(rel_file)
        rel_row = pd.concat([old_rel, rel_row], ignore_index=True)
    _, fb_rel, w_rel = write_csv_resilient(rel_row, rel_file, index=False)
    if fb_rel and w_rel:
        write_warnings.append(w_rel)

    return {
        "api_input_file": str(api_file),
        "api_rows": int(len(guarded_full)),
        "api_latest_date": str(pd.to_datetime(latest_date).date()),
        "api_raw_file": str(raw_live_file),
        "api_guarded_file": str(guarded_file),
        "api_historical_top10_file": str(historical_top10_file),
        "api_latest_top10_file": str(latest_top10_file),
        "api_reliability_report_file": str(rel_file),
        "write_warnings": " | ".join(write_warnings),
    }


def append_run_log(entry):
    run_log_file = LOGS / "daily_run_log.csv"
    log_df = pd.DataFrame([entry])
    if run_log_file.exists():
        old = pd.read_csv(run_log_file)
        log_df = pd.concat([old, log_df], ignore_index=True)
    write_csv_resilient(log_df, run_log_file, index=False)
    return run_log_file


def update_api_failsafe(run_log_file, run_time, window=2):
    """
    Raise an alert when live API has not succeeded for N consecutive runs.
    This is a lightweight fail-safe note for ops monitoring.
    """
    alert_file = DOCS / "step11_api_failsafe_alert.txt"
    event_file = DOCS / "step11_api_failsafe_events.csv"
    fail_like = {"stale_fallback", "failed"}

    logs = pd.read_csv(run_log_file)
    statuses = logs["api_status"].astype(str).str.strip().str.lower().tolist() if "api_status" in logs.columns else []
    recent = statuses[-window:] if len(statuses) >= window else statuses
    consecutive_fail = len(recent) == window and all(s in fail_like for s in recent)
    current_status = statuses[-1] if statuses else "unknown"

    if consecutive_fail:
        state = "ALERT"
        note = (
            f"API FAILSAFE ALERT ({run_time})\n"
            f"Condition: Last {window} runs are non-live ({', '.join(recent)}).\n"
            "Impact: Pipeline is running on fallback/no-live API.\n"
            "Action: Check API source availability and quality gate logs (docs/step10_api_quality_gate_report.csv).\n"
        )
    elif current_status == "success":
        state = "OK"
        note = (
            f"API FAILSAFE STATUS OK ({run_time})\n"
            "Condition: Latest run has live API success.\n"
            "Action: No intervention needed.\n"
        )
    else:
        state = "WATCH"
        note = (
            f"API FAILSAFE WATCH ({run_time})\n"
            f"Condition: Latest status is {current_status}. Waiting for confirmation over {window} runs.\n"
            "Action: Monitor next scheduled run.\n"
        )

    alert_file.write_text(note, encoding="utf-8")

    event_row = pd.DataFrame(
        [
            {
                "run_time": run_time,
                "failsafe_state": state,
                "latest_api_status": current_status,
                "recent_api_statuses": "|".join(recent),
                "window_runs": int(window),
                "consecutive_non_live_detected": bool(consecutive_fail),
            }
        ]
    )
    if event_file.exists():
        old = pd.read_csv(event_file)
        if not old.empty and str(old.iloc[-1].get("failsafe_state", "")).strip().upper() == state:
            return {"failsafe_state": state, "alert_file": str(alert_file), "event_file": str(event_file)}
        event_row = pd.concat([old, event_row], ignore_index=True)
    write_csv_resilient(event_row, event_file, index=False)
    return {"failsafe_state": state, "alert_file": str(alert_file), "event_file": str(event_file)}


def main():
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        model_table, model_obj, model_features, best_thr, model_mode, model_name = load_model_artifacts()
        expected_districts = load_expected_districts()
        hist_out = build_historical_auto_alerts(model_table, model_obj, model_features, best_thr, model_mode)

        api_status = "success"
        api_message = ""
        api_quality_message = ""
        api_out = {}
        try:
            api_file, api_stdout = fetch_latest_api_file()
            valid_files, quality_rows = list_valid_api_files(expected_districts)
            fetched_quality = evaluate_api_quality(api_file, expected_districts)

            if fetched_quality["is_valid"]:
                selected_file = api_file
                selected_mode = "live_valid"
                api_status = "success"
            else:
                # Latest valid fallback from archive
                selected_file = valid_files[-1] if valid_files else None
                selected_mode = "fallback_valid_archive" if selected_file is not None else "no_valid_api_file"
                api_status = "stale_fallback" if selected_file is not None else "failed"

            append_api_quality_gate_log(
                {
                    "run_time": run_time,
                    "fetched_file": str(api_file),
                    "fetched_valid": fetched_quality["is_valid"],
                    "selected_mode": selected_mode,
                    "selected_file": str(selected_file) if selected_file is not None else "",
                    "valid_archive_files": len(valid_files),
                    "invalid_archive_files": len(quality_rows) - len(valid_files),
                    "fetched_reason": fetched_quality["reason"],
                }
            )

            if selected_file is None:
                raise RuntimeError("No quality-valid API file available in archive.")

            api_out = build_api_live_guarded_alerts(
                selected_file,
                model_obj,
                model_features,
                best_thr,
                model_mode,
                api_files_for_archive=valid_files,
            )
            api_message = api_stdout
            api_quality_message = f"quality_mode={selected_mode}; selected_file={selected_file}"
        except Exception as api_err:
            valid_files, quality_rows = list_valid_api_files(expected_districts)
            fallback_file = valid_files[-1] if valid_files else None
            if fallback_file is not None:
                api_status = "stale_fallback"
                api_out = build_api_live_guarded_alerts(
                    fallback_file,
                    model_obj,
                    model_features,
                    best_thr,
                    model_mode,
                    api_files_for_archive=valid_files,
                )
                api_message = f"Live API fetch failed; used fallback file: {fallback_file}\nError: {api_err}"
                api_quality_message = (
                    f"quality_mode=fallback_after_fetch_error; valid_archive_files={len(valid_files)}; "
                    f"invalid_archive_files={len(quality_rows) - len(valid_files)}"
                )
                append_api_quality_gate_log(
                    {
                        "run_time": run_time,
                        "fetched_file": "",
                        "fetched_valid": False,
                        "selected_mode": "fallback_after_fetch_error",
                        "selected_file": str(fallback_file),
                        "valid_archive_files": len(valid_files),
                        "invalid_archive_files": len(quality_rows) - len(valid_files),
                        "fetched_reason": str(api_err),
                    }
                )
            else:
                api_status = "failed"
                api_message = str(api_err)
                api_quality_message = "quality_mode=failed_no_valid_archive"
                append_api_quality_gate_log(
                    {
                        "run_time": run_time,
                        "fetched_file": "",
                        "fetched_valid": False,
                        "selected_mode": "failed_no_valid_archive",
                        "selected_file": "",
                        "valid_archive_files": 0,
                        "invalid_archive_files": 0,
                        "fetched_reason": str(api_err),
                    }
                )

        status = "success" if api_status in ["success", "stale_fallback"] else "partial_success"
        write_warning_parts = []
        if hist_out.get("write_warnings"):
            write_warning_parts.append(f"historical={hist_out['write_warnings']}")
        if api_out.get("write_warnings"):
            write_warning_parts.append(f"api={api_out['write_warnings']}")
        write_warning_text = " | ".join(write_warning_parts)

        entry = {
            "run_time": run_time,
            "status": status,
            "api_status": api_status,
            "rows": hist_out["rows"],
            "latest_date": hist_out["latest_date"],
            "best_threshold": best_thr,
            "model_mode": model_mode,
            "model_name": model_name,
            "full_file": hist_out["full_file"],
            "top10_file": hist_out["top10_file"],
            "latest_top10_file": hist_out["latest_top10_file"],
            "api_input_file": api_out.get("api_input_file", ""),
            "api_guarded_file": api_out.get("api_guarded_file", ""),
            "api_historical_top10_file": api_out.get("api_historical_top10_file", ""),
            "api_latest_top10_file": api_out.get("api_latest_top10_file", ""),
            "api_reliability_report_file": api_out.get("api_reliability_report_file", ""),
            "message": f"{api_quality_message}\n{api_message}\n{write_warning_text}",
        }
        run_log_file = append_run_log(entry)
        failsafe = update_api_failsafe(run_log_file, run_time, window=2)

        print("Automation run complete.")
        print("Status:", status)
        print("Model mode:", model_mode)
        print("Model name:", model_name)
        print("Model threshold:", best_thr)
        print("Historical full alerts:", hist_out["full_file"])
        print("Historical top10:", hist_out["top10_file"])
        print("Historical latest top10:", hist_out["latest_top10_file"])
        if api_out:
            print("API guarded alerts:", api_out["api_guarded_file"])
            print("API historical top10:", api_out["api_historical_top10_file"])
            print("API latest top10:", api_out["api_latest_top10_file"])
            print("API reliability report:", api_out["api_reliability_report_file"])
            if api_out.get("write_warnings"):
                print("Write warnings:", api_out["write_warnings"])
            if api_status == "stale_fallback":
                print("API mode: stale fallback (last available API file used).")
        else:
            print("API output unavailable in this run.")
        if hist_out.get("write_warnings"):
            print("Historical write warnings:", hist_out["write_warnings"])
        print("API fail-safe state:", failsafe["failsafe_state"])
        print("API fail-safe note:", failsafe["alert_file"])
        print("Log:", run_log_file)
    except Exception:
        err = traceback.format_exc()
        run_log_file = append_run_log(
            {
                "run_time": run_time,
                "status": "failed",
                "api_status": "not_started",
                "rows": 0,
                "latest_date": "",
                "best_threshold": "",
                "full_file": "",
                "top10_file": "",
                "latest_top10_file": "",
                "api_input_file": "",
                "api_guarded_file": "",
                "api_historical_top10_file": "",
                "api_latest_top10_file": "",
                "api_reliability_report_file": "",
                "message": err,
            }
        )
        failsafe = update_api_failsafe(run_log_file, run_time, window=2)
        print("Automation run failed.")
        print(err)
        print("API fail-safe state:", failsafe["failsafe_state"])
        print("API fail-safe note:", failsafe["alert_file"])
        print("Log:", run_log_file)
        raise


if __name__ == "__main__":
    main()
