
import glob
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

ROOT = Path(r"d:/Rajasthan Climate Risk Command System")
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


def load_model_artifacts():
    model_table_file = PROCESSED / "model_table_step3.csv"
    model_file = MODELS / "baseline_logistic_step4.joblib"

    model_table = pd.read_csv(model_table_file, parse_dates=["date"])
    clf = joblib.load(model_file)

    feature_file = DOCS / "step4_model_features.txt"
    with open(feature_file, "r", encoding="utf-8") as f:
        model_features = [line.strip() for line in f if line.strip()]

    thr_df = pd.read_csv(DOCS / "step4_threshold_tuning.csv")
    best_thr = float(thr_df.sort_values("val_f1", ascending=False).iloc[0]["threshold"])
    return model_table, clf, model_features, best_thr


def map_alert(prob, score, thr):
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


def build_historical_auto_alerts(model_table, clf, model_features, best_thr):
    x_all = model_table[model_features].copy()
    all_prob = clf.predict_proba(x_all)[:, 1]
    all_pred = (all_prob >= best_thr).astype(int)

    alerts_df = model_table[["date", "district", "target_heat_score_t1"]].copy()
    alerts_df["pred_prob_critical_t1"] = all_prob
    alerts_df["pred_critical_t1"] = all_pred
    alerts_df["risk_score_next_day"] = (
        0.65 * alerts_df["target_heat_score_t1"].clip(0, 100)
        + 0.35 * (alerts_df["pred_prob_critical_t1"] * 100)
    ).clip(0, 100)

    alerts_df["alert_level_next_day"] = [
        map_alert(p, s, best_thr)
        for p, s in zip(alerts_df["pred_prob_critical_t1"], alerts_df["risk_score_next_day"])
    ]

    alerts_df["rank_within_date"] = (
        alerts_df.groupby("date")["risk_score_next_day"].rank(method="first", ascending=False).astype(int)
    )
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

    alerts_df.to_csv(full_file, index=False)
    alerts_df[alerts_df["rank_within_date"] <= 10].to_csv(top10_file, index=False)
    latest_date = alerts_df["date"].max()
    alerts_df[(alerts_df["date"] == latest_date) & (alerts_df["rank_within_date"] <= 10)].to_csv(
        latest_top10_file, index=False
    )

    return {
        "rows": int(len(alerts_df)),
        "latest_date": str(pd.to_datetime(latest_date).date()),
        "full_file": str(full_file),
        "top10_file": str(top10_file),
        "latest_top10_file": str(latest_top10_file),
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


def build_api_live_guarded_alerts(api_file, clf, model_features, best_thr):
    # Load full raw API archive so historical forecast dates are always retained.
    api_files = sorted(glob.glob(str(RAW / "api_daily_weather_*.csv")))
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
        df_["district"] = df_["district"].astype(str).str.strip().str.replace(r"\s+", " ", regex=True).str.title()

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

    api_dates = set(api_df["date"].dt.normalize())
    live_df = combo[combo["date"].dt.normalize().isin(api_dates)].copy()

    x_live = live_df[model_features].copy()
    live_prob = clf.predict_proba(x_live)[:, 1]
    live_pred = (live_prob >= best_thr).astype(int)

    live_out = live_df[
        ["date", "district", "rain_mm", "tmax_c", "tmin_c", "tavg_c", "heat_stress_score"]
    ].copy()
    live_out["pred_prob_critical_t1"] = live_prob
    live_out["pred_critical_t1"] = live_pred
    live_out["risk_score_next_day"] = (
        0.65 * live_out["heat_stress_score"] + 0.35 * (live_out["pred_prob_critical_t1"] * 100)
    ).clip(0, 100)

    live_out["alert_level_next_day"] = [
        map_alert(p, s, best_thr) for p, s in zip(live_out["pred_prob_critical_t1"], live_out["risk_score_next_day"])
    ]
    live_out["rank_within_date"] = (
        live_out.groupby("date")["risk_score_next_day"].rank(method="first", ascending=False).astype(int)
    )
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
    live_out_full.to_csv(raw_live_file, index=False)

    # Guardrail to avoid over-alerting
    mix = live_out["alert_level_next_day"].value_counts(normalize=True).mul(100)
    red_pct = float(mix.get("RED", 0.0))
    guarded = live_out.copy()

    if red_pct > 40:
        q_y = float(guarded["risk_score_next_day"].quantile(0.65))
        q_o = float(guarded["risk_score_next_day"].quantile(0.85))
        q_r = float(guarded["risk_score_next_day"].quantile(0.95))

        def rebalance_alert(prob, score):
            if prob >= 0.95 and score >= q_r:
                return "RED"
            if prob >= 0.80 and score >= q_o:
                return "ORANGE"
            if prob >= 0.55 and score >= q_y:
                return "YELLOW"
            return "GREEN"

        guarded["alert_level_next_day"] = [
            rebalance_alert(p, s) for p, s in zip(guarded["pred_prob_critical_t1"], guarded["risk_score_next_day"])
        ]
        guarded["recommended_action"] = guarded["alert_level_next_day"].map(ACTION_MAP)
        guarded["confidence_tag"] = np.select(
            [guarded["pred_prob_critical_t1"] >= 0.90, guarded["pred_prob_critical_t1"] >= 0.75],
            ["High", "Medium"],
            default="Low",
        )

    guarded_file = OUTPUTS / "api_forecast_alerts_step9_guarded.csv"
    guarded_full = upsert_history_by_date_district(guarded_file, guarded)
    guarded_full.to_csv(guarded_file, index=False)

    # Historical top10 (all days) + latest day top10
    historical_top10_file = OUTPUTS / "api_daily_top10_guarded.csv"
    guarded_full[guarded_full["rank_within_date"] <= 10].to_csv(historical_top10_file, index=False)

    latest_date = guarded_full["date"].max()
    latest_top10_file = OUTPUTS / "api_latest_top10_guarded.csv"
    guarded_full[
        (guarded_full["date"] == latest_date) & (guarded_full["rank_within_date"] <= 10)
    ].to_csv(
        latest_top10_file, index=False
    )

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
    rel_row.to_csv(rel_file, index=False)

    return {
        "api_input_file": str(api_file),
        "api_rows": int(len(guarded_full)),
        "api_latest_date": str(pd.to_datetime(latest_date).date()),
        "api_raw_file": str(raw_live_file),
        "api_guarded_file": str(guarded_file),
        "api_historical_top10_file": str(historical_top10_file),
        "api_latest_top10_file": str(latest_top10_file),
        "api_reliability_report_file": str(rel_file),
    }


def append_run_log(entry):
    run_log_file = LOGS / "daily_run_log.csv"
    log_df = pd.DataFrame([entry])
    if run_log_file.exists():
        old = pd.read_csv(run_log_file)
        log_df = pd.concat([old, log_df], ignore_index=True)
    log_df.to_csv(run_log_file, index=False)
    return run_log_file


def main():
    run_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        model_table, clf, model_features, best_thr = load_model_artifacts()
        hist_out = build_historical_auto_alerts(model_table, clf, model_features, best_thr)

        api_status = "success"
        api_message = ""
        api_out = {}
        try:
            api_file, api_stdout = fetch_latest_api_file()
            api_out = build_api_live_guarded_alerts(api_file, clf, model_features, best_thr)
            api_message = api_stdout
        except Exception as api_err:
            fallback_file = latest_existing_api_file()
            if fallback_file is not None:
                api_status = "stale_fallback"
                api_out = build_api_live_guarded_alerts(fallback_file, clf, model_features, best_thr)
                api_message = f"Live API fetch failed; used fallback file: {fallback_file}\nError: {api_err}"
            else:
                api_status = "failed"
                api_message = str(api_err)

        status = "success" if api_status in ["success", "stale_fallback"] else "partial_success"
        entry = {
            "run_time": run_time,
            "status": status,
            "api_status": api_status,
            "rows": hist_out["rows"],
            "latest_date": hist_out["latest_date"],
            "best_threshold": best_thr,
            "full_file": hist_out["full_file"],
            "top10_file": hist_out["top10_file"],
            "latest_top10_file": hist_out["latest_top10_file"],
            "api_input_file": api_out.get("api_input_file", ""),
            "api_guarded_file": api_out.get("api_guarded_file", ""),
            "api_historical_top10_file": api_out.get("api_historical_top10_file", ""),
            "api_latest_top10_file": api_out.get("api_latest_top10_file", ""),
            "api_reliability_report_file": api_out.get("api_reliability_report_file", ""),
            "message": api_message,
        }
        run_log_file = append_run_log(entry)

        print("Automation run complete.")
        print("Status:", status)
        print("Historical full alerts:", hist_out["full_file"])
        print("Historical top10:", hist_out["top10_file"])
        print("Historical latest top10:", hist_out["latest_top10_file"])
        if api_out:
            print("API guarded alerts:", api_out["api_guarded_file"])
            print("API historical top10:", api_out["api_historical_top10_file"])
            print("API latest top10:", api_out["api_latest_top10_file"])
            print("API reliability report:", api_out["api_reliability_report_file"])
            if api_status == "stale_fallback":
                print("API mode: stale fallback (last available API file used).")
        else:
            print("API output unavailable in this run.")
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
        print("Automation run failed.")
        print(err)
        print("Log:", run_log_file)
        raise


if __name__ == "__main__":
    main()
