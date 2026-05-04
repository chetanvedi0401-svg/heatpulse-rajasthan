from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler


ROOT = Path(r"d:/Rajasthan Climate Risk Command System")
PROCESSED = ROOT / "data" / "processed"
DOCS = ROOT / "docs"
MODELS = ROOT / "models"
FIRST_TEST_YEAR = 2021
LAST_TEST_YEAR = 2025


@dataclass
class ModelSpec:
    name: str
    builder: Callable[[], object]
    needs_scaling: bool = False


def metric_pack(y_true: pd.Series, prob: np.ndarray, thr: float) -> dict[str, float]:
    pred = (prob >= thr).astype(int)
    return {
        "accuracy": float(accuracy_score(y_true, pred)),
        "precision": float(precision_score(y_true, pred, zero_division=0)),
        "recall": float(recall_score(y_true, pred, zero_division=0)),
        "f1": float(f1_score(y_true, pred, zero_division=0)),
        "roc_auc": float(roc_auc_score(y_true, prob)) if y_true.nunique() > 1 else np.nan,
        "positive_rate": float(np.mean(pred)),
        "actual_event_rate": float(np.mean(y_true)),
    }


def build_model(spec: ModelSpec):
    base = spec.builder()
    if spec.needs_scaling:
        return Pipeline([("scaler", StandardScaler()), ("clf", base)])
    return base


def tune_threshold(y_true: pd.Series, prob: np.ndarray) -> tuple[float, pd.DataFrame]:
    # Recall-priority threshold tuning for safety use-case.
    rows = []
    for thr in np.round(np.arange(0.20, 0.91, 0.04), 2):
        pred = (prob >= thr).astype(int)
        p = float(precision_score(y_true, pred, zero_division=0))
        r = float(recall_score(y_true, pred, zero_division=0))
        f1 = float(f1_score(y_true, pred, zero_division=0))
        score = (0.65 * r) + (0.35 * p)  # weighted objective
        rows.append({"threshold": float(thr), "val_precision": p, "val_recall": r, "val_f1": f1, "val_score": score})
    grid = pd.DataFrame(rows).sort_values(["val_score", "val_recall", "val_f1"], ascending=False).reset_index(drop=True)
    return float(grid.iloc[0]["threshold"]), grid


def run_rolling_for_model(
    df: pd.DataFrame,
    features: list[str],
    model_name: str,
    model_spec: ModelSpec,
    first_test_year: int,
    last_test_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict] = []
    pred_rows: list[pd.DataFrame] = []

    for test_year in range(first_test_year, last_test_year + 1):
        test_start = pd.Timestamp(f"{test_year}-01-01")
        test_end = pd.Timestamp(f"{test_year}-12-31")
        val_start = pd.Timestamp(f"{test_year - 1}-01-01")
        val_end = pd.Timestamp(f"{test_year - 1}-12-31")

        fit_df = df[df["date"] < val_start].copy()
        val_df = df[(df["date"] >= val_start) & (df["date"] <= val_end)].copy()
        train_all_df = df[df["date"] < test_start].copy()
        test_df = df[(df["date"] >= test_start) & (df["date"] <= test_end)].copy()

        if fit_df.empty or val_df.empty or train_all_df.empty or test_df.empty:
            continue
        if fit_df["target_heatwave_t1"].nunique() < 2 or val_df["target_heatwave_t1"].nunique() < 2:
            continue
        if train_all_df["target_heatwave_t1"].nunique() < 2 or test_df["target_heatwave_t1"].nunique() < 2:
            continue

        x_fit = fit_df[features]
        y_fit = fit_df["target_heatwave_t1"]
        x_val = val_df[features]
        y_val = val_df["target_heatwave_t1"]
        x_train_all = train_all_df[features]
        y_train_all = train_all_df["target_heatwave_t1"]
        x_test = test_df[features]
        y_test = test_df["target_heatwave_t1"]

        thr_model = build_model(model_spec)
        thr_model.fit(x_fit, y_fit)
        val_prob = thr_model.predict_proba(x_val)[:, 1]
        best_thr, grid = tune_threshold(y_val, val_prob)

        deploy_model = build_model(model_spec)
        deploy_model.fit(x_train_all, y_train_all)
        test_prob = deploy_model.predict_proba(x_test)[:, 1]

        m = metric_pack(y_test, test_prob, best_thr)
        m.update(
            {
                "model": model_name,
                "test_year": int(test_year),
                "best_threshold": float(best_thr),
                "train_rows": int(len(train_all_df)),
                "val_rows": int(len(val_df)),
                "test_rows": int(len(test_df)),
            }
        )
        fold_rows.append(m)

        tmp = test_df[["date", "district", "target_heatwave_t1"]].copy()
        tmp["model"] = model_name
        tmp["test_year"] = int(test_year)
        tmp["best_threshold"] = float(best_thr)
        tmp["pred_prob"] = test_prob
        tmp["pred_label"] = (test_prob >= best_thr).astype(int)
        pred_rows.append(tmp)

        g = grid.copy()
        g["model"] = model_name
        g["test_year"] = int(test_year)
        g["selected"] = g["threshold"] == best_thr
        DOCS.mkdir(parents=True, exist_ok=True)
        g.to_csv(DOCS / f"_tmp_grid_{model_name}_{test_year}.csv", index=False)

    fold_df = pd.DataFrame(fold_rows)
    pred_df = pd.concat(pred_rows, ignore_index=True) if pred_rows else pd.DataFrame()
    return fold_df, pred_df


def run_rolling_ensemble(
    df: pd.DataFrame,
    features: list[str],
    first_test_year: int,
    last_test_year: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict] = []
    pred_rows: list[pd.DataFrame] = []

    for test_year in range(first_test_year, last_test_year + 1):
        test_start = pd.Timestamp(f"{test_year}-01-01")
        test_end = pd.Timestamp(f"{test_year}-12-31")
        val_start = pd.Timestamp(f"{test_year - 1}-01-01")
        val_end = pd.Timestamp(f"{test_year - 1}-12-31")

        fit_df = df[df["date"] < val_start].copy()
        val_df = df[(df["date"] >= val_start) & (df["date"] <= val_end)].copy()
        train_all_df = df[df["date"] < test_start].copy()
        test_df = df[(df["date"] >= test_start) & (df["date"] <= test_end)].copy()

        if fit_df.empty or val_df.empty or train_all_df.empty or test_df.empty:
            continue
        if fit_df["target_heatwave_t1"].nunique() < 2 or val_df["target_heatwave_t1"].nunique() < 2:
            continue
        if train_all_df["target_heatwave_t1"].nunique() < 2 or test_df["target_heatwave_t1"].nunique() < 2:
            continue

        x_fit = fit_df[features]
        y_fit = fit_df["target_heatwave_t1"]
        x_val = val_df[features]
        y_val = val_df["target_heatwave_t1"]
        x_train_all = train_all_df[features]
        y_train_all = train_all_df["target_heatwave_t1"]
        x_test = test_df[features]
        y_test = test_df["target_heatwave_t1"]

        lg_val = Pipeline(
            [("scaler", StandardScaler()), ("clf", LogisticRegression(max_iter=500, class_weight="balanced", random_state=42))]
        )
        rf_val = RandomForestClassifier(
            n_estimators=180, max_depth=10, min_samples_leaf=2, class_weight="balanced_subsample", random_state=42, n_jobs=1
        )
        hgb_val = GradientBoostingClassifier(
            n_estimators=180, learning_rate=0.06, max_depth=3, subsample=0.9, random_state=42
        )
        for m in [lg_val, rf_val, hgb_val]:
            m.fit(x_fit, y_fit)
        val_prob = (0.25 * lg_val.predict_proba(x_val)[:, 1]) + (0.35 * rf_val.predict_proba(x_val)[:, 1]) + (0.40 * hgb_val.predict_proba(x_val)[:, 1])
        best_thr, _ = tune_threshold(y_val, val_prob)

        lg = Pipeline(
            [("scaler", StandardScaler()), ("clf", LogisticRegression(max_iter=500, class_weight="balanced", random_state=42))]
        )
        rf = RandomForestClassifier(
            n_estimators=180, max_depth=10, min_samples_leaf=2, class_weight="balanced_subsample", random_state=42, n_jobs=1
        )
        hgb = GradientBoostingClassifier(
            n_estimators=180, learning_rate=0.06, max_depth=3, subsample=0.9, random_state=42
        )
        for m in [lg, rf, hgb]:
            m.fit(x_train_all, y_train_all)
        test_prob = (0.25 * lg.predict_proba(x_test)[:, 1]) + (0.35 * rf.predict_proba(x_test)[:, 1]) + (0.40 * hgb.predict_proba(x_test)[:, 1])

        m = metric_pack(y_test, test_prob, best_thr)
        m.update(
            {
                "model": "ensemble_lgrfgb",
                "test_year": int(test_year),
                "best_threshold": float(best_thr),
                "train_rows": int(len(train_all_df)),
                "val_rows": int(len(val_df)),
                "test_rows": int(len(test_df)),
            }
        )
        fold_rows.append(m)

        tmp = test_df[["date", "district", "target_heatwave_t1"]].copy()
        tmp["model"] = "ensemble_lgrfgb"
        tmp["test_year"] = int(test_year)
        tmp["best_threshold"] = float(best_thr)
        tmp["pred_prob"] = test_prob
        tmp["pred_label"] = (test_prob >= best_thr).astype(int)
        pred_rows.append(tmp)

    fold_df = pd.DataFrame(fold_rows)
    pred_df = pd.concat(pred_rows, ignore_index=True) if pred_rows else pd.DataFrame()
    return fold_df, pred_df


def build_district_metrics(pred_df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for district, g in pred_df.groupby("district"):
        y = g["target_heatwave_t1"].astype(int)
        p = g["pred_label"].astype(int)
        rows.append(
            {
                "district": district,
                "rows": int(len(g)),
                "event_rate": float(y.mean()),
                "pred_rate": float(p.mean()),
                "precision": float(precision_score(y, p, zero_division=0)),
                "recall": float(recall_score(y, p, zero_division=0)),
                "f1": float(f1_score(y, p, zero_division=0)),
            }
        )
    out = pd.DataFrame(rows).sort_values(["f1", "recall"], ascending=[False, False]).reset_index(drop=True)
    return out


def finalize_best_model(df: pd.DataFrame, features: list[str], model_name: str):
    x_all = df[features]
    y_all = df["target_heatwave_t1"]

    if model_name == "logistic":
        model = Pipeline([("scaler", StandardScaler()), ("clf", LogisticRegression(max_iter=500, class_weight="balanced", random_state=42))])
    elif model_name == "random_forest":
        model = RandomForestClassifier(
            n_estimators=180, max_depth=10, min_samples_leaf=2, class_weight="balanced_subsample", random_state=42, n_jobs=1
        )
    elif model_name == "grad_boost":
        model = GradientBoostingClassifier(
            n_estimators=180, learning_rate=0.06, max_depth=3, subsample=0.9, random_state=42
        )
    else:
        # Save three models separately for ensemble inference.
        lg = Pipeline([("scaler", StandardScaler()), ("clf", LogisticRegression(max_iter=500, class_weight="balanced", random_state=42))])
        rf = RandomForestClassifier(
            n_estimators=180, max_depth=10, min_samples_leaf=2, class_weight="balanced_subsample", random_state=42, n_jobs=1
        )
        hgb = GradientBoostingClassifier(
            n_estimators=180, learning_rate=0.06, max_depth=3, subsample=0.9, random_state=42
        )
        lg.fit(x_all, y_all)
        rf.fit(x_all, y_all)
        hgb.fit(x_all, y_all)
        return {"type": "ensemble", "weights": [0.25, 0.35, 0.40], "models": {"lg": lg, "rf": rf, "hgb": hgb}}

    model.fit(x_all, y_all)
    return {"type": "single", "model": model}


def main() -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    MODELS.mkdir(parents=True, exist_ok=True)

    table_file = PROCESSED / "model_table_step12_heatwave_v2.csv"
    df = pd.read_csv(table_file, parse_dates=["date"]).sort_values(["date", "district"]).reset_index(drop=True)

    feature_file = DOCS / "step12_feature_list_v2.txt"
    with open(feature_file, "r", encoding="utf-8") as f:
        features = [x.strip() for x in f if x.strip()]

    needed = features + ["target_heatwave_t1", "district", "date"]
    df = df.dropna(subset=needed).copy().reset_index(drop=True)
    df["target_heatwave_t1"] = df["target_heatwave_t1"].astype(int)

    model_specs = [
        ModelSpec("logistic", lambda: LogisticRegression(max_iter=500, class_weight="balanced", random_state=42), needs_scaling=True),
        ModelSpec(
                "random_forest",
                lambda: RandomForestClassifier(
                n_estimators=180, max_depth=10, min_samples_leaf=2, class_weight="balanced_subsample", random_state=42, n_jobs=1
                ),
        ),
        ModelSpec(
            "grad_boost",
            lambda: GradientBoostingClassifier(
                n_estimators=180, learning_rate=0.06, max_depth=3, subsample=0.9, random_state=42
            ),
        ),
    ]

    fold_frames = []
    pred_frames = []

    def load_or_run_model(model_key: str, runner: Callable[[], tuple[pd.DataFrame, pd.DataFrame]]):
        fold_path = DOCS / f"step13_{model_key}_folds_v3.csv"
        pred_path = DOCS / f"step13_{model_key}_preds_v3.csv"

        if fold_path.exists() and pred_path.exists():
            print(f"[resume] Loading cached {model_key} folds/preds.")
            return pd.read_csv(fold_path), pd.read_csv(pred_path, parse_dates=["date"])

        print(f"[run] Training/evaluating {model_key} ...")
        fdf, pdf = runner()
        if fdf.empty or pdf.empty:
            raise RuntimeError(f"{model_key} produced empty outputs.")
        fdf.to_csv(fold_path, index=False)
        pdf.to_csv(pred_path, index=False)
        print(f"[saved] {model_key} -> {fold_path.name}, {pred_path.name}")
        return fdf, pdf
    for spec in model_specs:
        fdf, pdf = load_or_run_model(
            spec.name,
            lambda s=spec: run_rolling_for_model(
                df, features, s.name, s, first_test_year=FIRST_TEST_YEAR, last_test_year=LAST_TEST_YEAR
            ),
        )
        if not fdf.empty:
            fold_frames.append(fdf)
        if not pdf.empty:
            pred_frames.append(pdf)

    ens_fold, ens_pred = load_or_run_model(
        "ensemble_lgrfgb",
        lambda: run_rolling_ensemble(
            df, features, first_test_year=FIRST_TEST_YEAR, last_test_year=LAST_TEST_YEAR
        ),
    )
    if not ens_fold.empty:
        fold_frames.append(ens_fold)
    if not ens_pred.empty:
        pred_frames.append(ens_pred)

    if not fold_frames:
        raise RuntimeError("No valid rolling folds produced for any model.")

    folds = pd.concat(fold_frames, ignore_index=True)
    preds = pd.concat(pred_frames, ignore_index=True)

    leaderboard = (
        folds.groupby("model", as_index=False)
        .agg(
            avg_accuracy=("accuracy", "mean"),
            avg_precision=("precision", "mean"),
            avg_recall=("recall", "mean"),
            avg_f1=("f1", "mean"),
            avg_roc_auc=("roc_auc", "mean"),
            avg_threshold=("best_threshold", "mean"),
            folds=("test_year", "nunique"),
        )
        .sort_values(["avg_f1", "avg_recall", "avg_precision"], ascending=False)
        .reset_index(drop=True)
    )

    best_model_name = str(leaderboard.iloc[0]["model"])
    best_pred = preds[preds["model"] == best_model_name].copy()
    district_metrics = build_district_metrics(best_pred)

    model_obj = finalize_best_model(df, features, best_model_name)
    model_file = MODELS / "heatwave_model_step13_strong.joblib"
    joblib.dump(
        {
            "model_name": best_model_name,
            "features": features,
            "median_threshold": float(folds[folds["model"] == best_model_name]["best_threshold"].median()),
            "artifact": model_obj,
        },
        model_file,
    )

    folds_file = DOCS / "step13_backtest_by_year_v3.csv"
    leader_file = DOCS / "step13_model_leaderboard_v3.csv"
    district_file = DOCS / "step13_district_metrics_best_v3.csv"
    summary_file = DOCS / "step13_summary_v3.csv"

    folds.to_csv(folds_file, index=False)
    leaderboard.to_csv(leader_file, index=False)
    district_metrics.to_csv(district_file, index=False)

    summary = pd.DataFrame(
        [
            {
                "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "best_model": best_model_name,
                "best_avg_f1": float(leaderboard.iloc[0]["avg_f1"]),
                "best_avg_recall": float(leaderboard.iloc[0]["avg_recall"]),
                "best_avg_precision": float(leaderboard.iloc[0]["avg_precision"]),
                "best_avg_auc": float(leaderboard.iloc[0]["avg_roc_auc"]),
                "best_threshold_median": float(folds[folds["model"] == best_model_name]["best_threshold"].median()),
                "rows": int(len(df)),
                "districts": int(df["district"].nunique()),
                "model_file": str(model_file),
                "leaderboard_file": str(leader_file),
                "fold_file": str(folds_file),
                "district_file": str(district_file),
            }
        ]
    )
    summary.to_csv(summary_file, index=False)

    # Cleanup temporary threshold grids.
    for f in DOCS.glob("_tmp_grid_*.csv"):
        try:
            f.unlink()
        except Exception:
            pass

    print("Step 13 strong-model training complete.")
    print("Best model:", best_model_name)
    print("Model artifact:", model_file)
    print("Leaderboard:", leader_file)
    print("Backtest by year:", folds_file)
    print("District metrics:", district_file)
    print("Summary:", summary_file)
    print("\nLeaderboard:")
    print(leaderboard.to_string(index=False))


if __name__ == "__main__":
    main()
