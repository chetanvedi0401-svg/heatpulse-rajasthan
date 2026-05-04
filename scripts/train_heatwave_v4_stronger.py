from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import ExtraTreesClassifier, GradientBoostingClassifier, RandomForestClassifier
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
    supports_sample_weight: bool = False


def build_model(spec: ModelSpec):
    base = spec.builder()
    if spec.needs_scaling:
        return Pipeline([("scaler", StandardScaler()), ("clf", base)])
    return base


def fit_with_optional_weights(model, x: pd.DataFrame, y: pd.Series, supports_sample_weight: bool):
    if not supports_sample_weight:
        model.fit(x, y)
        return model

    pos = float((y == 1).sum())
    neg = float((y == 0).sum())
    pos_weight = (neg / pos) if pos > 0 else 1.0
    w = np.where(y.values == 1, pos_weight, 1.0)
    model.fit(x, y, sample_weight=w)
    return model


def threshold_grid_eval(y_true: pd.Series, prob: np.ndarray) -> pd.DataFrame:
    rows = []
    for thr in np.round(np.arange(0.18, 0.91, 0.02), 2):
        pred = (prob >= thr).astype(int)
        p = float(precision_score(y_true, pred, zero_division=0))
        r = float(recall_score(y_true, pred, zero_division=0))
        f1 = float(f1_score(y_true, pred, zero_division=0))
        rows.append({"threshold": float(thr), "precision": p, "recall": r, "f1": f1, "positive_rate": float(np.mean(pred))})
    return pd.DataFrame(rows)


def choose_threshold(y_true: pd.Series, prob: np.ndarray, min_recall: float) -> tuple[float, pd.DataFrame]:
    grid = threshold_grid_eval(y_true, prob)
    eligible = grid[grid["recall"] >= min_recall].copy()
    if eligible.empty:
        chosen = grid.sort_values(["f1", "recall", "precision"], ascending=False).iloc[0]
    else:
        chosen = eligible.sort_values(["f1", "precision", "recall"], ascending=False).iloc[0]
    return float(chosen["threshold"]), grid


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


def run_rolling_for_model(
    df: pd.DataFrame,
    features: list[str],
    spec: ModelSpec,
    first_test_year: int,
    last_test_year: int,
    min_recall: float,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fold_rows: list[dict] = []
    pred_rows: list[pd.DataFrame] = []
    grid_rows: list[pd.DataFrame] = []

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

        thr_model = build_model(spec)
        fit_with_optional_weights(thr_model, x_fit, y_fit, spec.supports_sample_weight)
        val_prob = thr_model.predict_proba(x_val)[:, 1]
        best_thr, grid = choose_threshold(y_val, val_prob, min_recall=min_recall)
        grid["model"] = spec.name
        grid["test_year"] = int(test_year)
        grid["selected"] = grid["threshold"] == best_thr
        grid_rows.append(grid)

        deploy_model = build_model(spec)
        fit_with_optional_weights(deploy_model, x_train_all, y_train_all, spec.supports_sample_weight)
        test_prob = deploy_model.predict_proba(x_test)[:, 1]

        m = metric_pack(y_test, test_prob, best_thr)
        m.update(
            {
                "model": spec.name,
                "test_year": int(test_year),
                "best_threshold": float(best_thr),
                "train_rows": int(len(train_all_df)),
                "val_rows": int(len(val_df)),
                "test_rows": int(len(test_df)),
            }
        )
        fold_rows.append(m)

        tmp = test_df[["date", "district", "target_heatwave_t1"]].copy()
        tmp["model"] = spec.name
        tmp["test_year"] = int(test_year)
        tmp["best_threshold"] = float(best_thr)
        tmp["pred_prob"] = test_prob
        tmp["pred_label"] = (test_prob >= best_thr).astype(int)
        pred_rows.append(tmp)

    fold_df = pd.DataFrame(fold_rows)
    pred_df = pd.concat(pred_rows, ignore_index=True) if pred_rows else pd.DataFrame()
    grid_df = pd.concat(grid_rows, ignore_index=True) if grid_rows else pd.DataFrame()
    return fold_df, pred_df, grid_df


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
    return pd.DataFrame(rows).sort_values(["f1", "recall"], ascending=[False, False]).reset_index(drop=True)


def finalize_model(df: pd.DataFrame, features: list[str], spec: ModelSpec):
    x_all = df[features]
    y_all = df["target_heatwave_t1"]
    model = build_model(spec)
    fit_with_optional_weights(model, x_all, y_all, spec.supports_sample_weight)
    return {"type": "single", "model": model}


def main() -> None:
    DOCS.mkdir(parents=True, exist_ok=True)
    MODELS.mkdir(parents=True, exist_ok=True)

    table_file = PROCESSED / "model_table_step12_heatwave_v2.csv"
    df = pd.read_csv(table_file, parse_dates=["date"]).sort_values(["date", "district"]).reset_index(drop=True)

    with open(DOCS / "step12_feature_list_v2.txt", "r", encoding="utf-8") as f:
        features = [x.strip() for x in f if x.strip()]

    needed = features + ["target_heatwave_t1", "district", "date"]
    df = df.dropna(subset=needed).copy().reset_index(drop=True)
    df["target_heatwave_t1"] = df["target_heatwave_t1"].astype(int)

    # Slightly stronger precision-leaning recall floor than v3.
    min_recall = 0.82

    model_specs = [
        ModelSpec("logistic_v4", lambda: LogisticRegression(max_iter=900, class_weight="balanced", random_state=42), needs_scaling=True),
        ModelSpec(
            "random_forest_v4",
            lambda: RandomForestClassifier(
                n_estimators=260,
                max_depth=12,
                min_samples_leaf=1,
                class_weight="balanced_subsample",
                random_state=42,
                n_jobs=1,
            ),
        ),
        ModelSpec(
            "grad_boost_v4",
            lambda: GradientBoostingClassifier(
                n_estimators=260,
                learning_rate=0.045,
                max_depth=3,
                subsample=0.92,
                random_state=42,
            ),
        ),
        ModelSpec(
            "extra_trees_v4",
            lambda: ExtraTreesClassifier(
                n_estimators=320,
                max_depth=14,
                min_samples_leaf=1,
                class_weight="balanced_subsample",
                random_state=42,
                n_jobs=1,
            ),
        ),
    ]

    fold_frames = []
    pred_frames = []
    grid_frames = []

    for spec in model_specs:
        print(f"[run] {spec.name}")
        fdf, pdf, gdf = run_rolling_for_model(
            df,
            features,
            spec,
            first_test_year=FIRST_TEST_YEAR,
            last_test_year=LAST_TEST_YEAR,
            min_recall=min_recall,
        )
        if fdf.empty or pdf.empty:
            raise RuntimeError(f"{spec.name} produced empty outputs.")
        fold_frames.append(fdf)
        pred_frames.append(pdf)
        grid_frames.append(gdf)

    folds = pd.concat(fold_frames, ignore_index=True)
    preds = pd.concat(pred_frames, ignore_index=True)
    grids = pd.concat(grid_frames, ignore_index=True)

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
        .sort_values(["avg_f1", "avg_precision", "avg_recall"], ascending=False)
        .reset_index(drop=True)
    )

    best_model_name = str(leaderboard.iloc[0]["model"])
    best_spec = next(s for s in model_specs if s.name == best_model_name)
    best_pred = preds[preds["model"] == best_model_name].copy()

    global_thr, global_grid = choose_threshold(
        best_pred["target_heatwave_t1"].astype(int),
        best_pred["pred_prob"].values,
        min_recall=min_recall,
    )

    best_pred["pred_label_global_thr"] = (best_pred["pred_prob"] >= global_thr).astype(int)
    district_metrics = build_district_metrics(
        best_pred[["district", "target_heatwave_t1", "pred_label_global_thr"]].rename(
            columns={"pred_label_global_thr": "pred_label"}
        )
    )

    model_obj = finalize_model(df, features, best_spec)
    model_file = MODELS / "heatwave_model_step14_stronger.joblib"
    joblib.dump(
        {
            "model_name": best_model_name,
            "features": features,
            "median_threshold": float(global_thr),
            "artifact": model_obj,
        },
        model_file,
    )

    folds_file = DOCS / "step14_backtest_by_year_v4.csv"
    leader_file = DOCS / "step14_model_leaderboard_v4.csv"
    district_file = DOCS / "step14_district_metrics_best_v4.csv"
    pred_file = DOCS / "step14_best_model_preds_v4.csv"
    grid_file = DOCS / "step14_threshold_grid_best_v4.csv"
    fold_grid_file = DOCS / "step14_threshold_grid_folds_v4.csv"
    summary_file = DOCS / "step14_summary_v4.csv"

    folds.to_csv(folds_file, index=False)
    leaderboard.to_csv(leader_file, index=False)
    district_metrics.to_csv(district_file, index=False)
    best_pred.to_csv(pred_file, index=False)
    global_grid.assign(selected=global_grid["threshold"] == global_thr).to_csv(grid_file, index=False)
    grids.to_csv(fold_grid_file, index=False)

    summary = pd.DataFrame(
        [
            {
                "run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "best_model": best_model_name,
                "best_avg_f1": float(leaderboard.iloc[0]["avg_f1"]),
                "best_avg_precision": float(leaderboard.iloc[0]["avg_precision"]),
                "best_avg_recall": float(leaderboard.iloc[0]["avg_recall"]),
                "best_avg_auc": float(leaderboard.iloc[0]["avg_roc_auc"]),
                "global_deploy_threshold": float(global_thr),
                "recall_floor_for_threshold": float(min_recall),
                "rows": int(len(df)),
                "districts": int(df["district"].nunique()),
                "model_file": str(model_file),
                "leaderboard_file": str(leader_file),
                "fold_file": str(folds_file),
                "district_file": str(district_file),
                "threshold_grid_file": str(grid_file),
            }
        ]
    )
    summary.to_csv(summary_file, index=False)

    print("Step 14 stronger-model training complete.")
    print("Best model:", best_model_name)
    print("Global deploy threshold:", global_thr)
    print("Model artifact:", model_file)
    print("Leaderboard:", leader_file)
    print("Summary:", summary_file)
    print("\nLeaderboard:")
    print(leaderboard.to_string(index=False))


if __name__ == "__main__":
    main()
