const els = {
  district: document.getElementById("districtSelect"),
  date: document.getElementById("dateSelect"),
  refresh: document.getElementById("refreshBtn"),
  downloadDayBtn: document.getElementById("downloadDayBtn"),
  forecastWindow: document.getElementById("forecastWindow"),
  mixChips: document.getElementById("mixChips"),
  mTmax: document.getElementById("mTmax"),
  mTmin: document.getElementById("mTmin"),
  mRain: document.getElementById("mRain"),
  mRisk: document.getElementById("mRisk"),
  alertBadge: document.getElementById("alertBadge"),
  actionText: document.getElementById("actionText"),
  probText: document.getElementById("probText"),
  confText: document.getElementById("confText"),
  rankText: document.getElementById("rankText"),
  topBody: document.querySelector("#topTable tbody"),
  riserBody: document.querySelector("#riserTable tbody"),
  langBtn: document.getElementById("langBtn"),
  fsBtn: document.getElementById("fsBtn"),
  weatherIcon: document.getElementById("weatherIcon"),
  weatherTitle: document.getElementById("weatherTitle"),
  weatherTag: document.getElementById("weatherTag"),
  tipText: document.getElementById("tipText"),
  planHydration: document.getElementById("planHydration"),
  planOrs: document.getElementById("planOrs"),
  planWindow: document.getElementById("planWindow"),
  planRiskGroup: document.getElementById("planRiskGroup"),
  planSummary: document.getElementById("planSummary"),
  simpleAlertTitle: document.getElementById("simpleAlertTitle"),
  simpleAlertText: document.getElementById("simpleAlertText"),
  simpleWindow: document.getElementById("simpleWindow"),
  simpleHydration: document.getElementById("simpleHydration"),
  simpleOrs: document.getElementById("simpleOrs"),
  riskMeterFill: document.getElementById("riskMeterFill"),
  riskMeterLabel: document.getElementById("riskMeterLabel"),
  whyReason1: document.getElementById("whyReason1"),
  whyReason2: document.getElementById("whyReason2"),
  whyReason3: document.getElementById("whyReason3"),
  whyPrevDate: document.getElementById("whyPrevDate"),
  whyPrevRisk: document.getElementById("whyPrevRisk"),
  whyDeltaRisk: document.getElementById("whyDeltaRisk"),
  whyPrevAlert: document.getElementById("whyPrevAlert"),
  whyConfidence: document.getElementById("whyConfidence"),
  formulaLine: document.getElementById("formulaLine"),
  probBandText: document.getElementById("probBandText"),
  riskBandText: document.getElementById("riskBandText"),
  policySummary: document.getElementById("policySummary"),
  tempOnlyNote: document.getElementById("tempOnlyNote"),
};

const labels = {
  en: {
    heroKicker: "Live Heat Intelligence",
    heroTitle: "District Heat Risk Command Center",
    refresh: "Refresh Data",
    alertPanelTitle: "Alert Panel",
    mapTitle: "Risk Map Snapshot",
    trendTitle: "District Trend",
    topTitle: "Top-10 Hotspots",
    mixTitle: "State Alert Mix",
    riserTitle: "Fastest Rising Risk Districts",
    planTitle: "Personal Safety Planner",
    action: "Recommended Action:",
    prob: "Critical Probability:",
    conf: "Confidence:",
    rank: "Rank Within Date:",
    pill1: "Public Dashboard",
    pill2: "Next-Day Risk",
    pill3: "Decision Support",
    thRank: "Rank",
    thDistrict: "District",
    thAlert: "Alert",
    thRisk: "Risk",
    thTmax: "Tmax",
    weatherTitle: "Heat Outlook",
    fsOn: "Exit Fullscreen",
    fsOff: "Fullscreen",
    dayCsv: "Download Day CSV",
    windowPrefix: "Safe Window:",
    hydrationPrefix: "Water:",
    orsPrefix: "ORS:",
  },
  hi: {
    heroKicker: "Live Heat Intelligence",
    heroTitle: "District Heat Risk Command Center",
    refresh: "Refresh Data",
    alertPanelTitle: "Alert Panel",
    mapTitle: "Risk Map Snapshot",
    trendTitle: "District Trend",
    topTitle: "Top-10 Hotspots",
    mixTitle: "State Alert Mix",
    riserTitle: "Fastest Rising Risk Districts",
    planTitle: "Personal Safety Planner",
    action: "Recommended Action:",
    prob: "Critical Probability:",
    conf: "Confidence:",
    rank: "Rank Within Date:",
    pill1: "Public Dashboard",
    pill2: "Next-Day Risk",
    pill3: "Decision Support",
    thRank: "Rank",
    thDistrict: "District",
    thAlert: "Alert",
    thRisk: "Risk",
    thTmax: "Tmax",
    weatherTitle: "Heat Outlook",
    fsOn: "Exit Fullscreen",
    fsOff: "Fullscreen",
    dayCsv: "Download Day CSV",
    windowPrefix: "Safe Window:",
    hydrationPrefix: "Water:",
    orsPrefix: "ORS:",
  },
};

let currentLang = "en";
let trendChart = null;
let mixChart = null;
let map = null;
let markerLayer = null;

function alertClass(level) {
  const x = (level || "").toUpperCase();
  if (x === "RED") return "alert-red";
  if (x === "ORANGE") return "alert-orange";
  if (x === "YELLOW") return "alert-yellow";
  return "alert-green";
}

function alertColor(level) {
  const x = (level || "").toUpperCase();
  if (x === "RED") return "#e03131";
  if (x === "ORANGE") return "#e8590c";
  if (x === "YELLOW") return "#c99700";
  return "#2f9e44";
}

function weatherSymbol(tmax, rain, alert) {
  if (alert === "RED") return "HEAT";
  if (rain >= 2) return "RAIN";
  if (tmax >= 42) return "VERY HOT";
  if (tmax >= 38) return "HOT";
  return "MILD";
}

function weatherTag(tmax, rain, alert) {
  if (alert === "RED") return "Extreme heat risk. Reduce outdoor exposure.";
  if (alert === "ORANGE") return "High heat stress likely. Plan hydration breaks.";
  if (alert === "YELLOW") return "Moderate caution. Track symptoms and fluids.";
  if (rain > 0) return "Mild rain support with manageable heat.";
  return "Relatively stable conditions.";
}

function healthTip(alert) {
  if (alert === "RED") return "Emergency tip: ORS + water every 30-45 min, avoid 11am-4pm outdoor work.";
  if (alert === "ORANGE") return "Preparedness tip: carry water, cap, and schedule shaded rest breaks.";
  if (alert === "YELLOW") return "Advisory tip: increase fluids, especially for elderly and outdoor workers.";
  return "Routine tip: keep hydration steady and monitor local updates.";
}

function riskExplain(alert) {
  if (alert === "RED") return "Very high heat risk. Avoid outdoor exposure except essential tasks.";
  if (alert === "ORANGE") return "High risk. Limit outdoor exposure and take frequent breaks.";
  if (alert === "YELLOW") return "Moderate risk. Increase hydration and rest intervals.";
  return "Low risk. Routine precautions are sufficient.";
}

function confidenceExplain(tag) {
  const x = String(tag || "").toUpperCase();
  if (x === "HIGH") {
    return "Confidence: High. Similar weather patterns were seen in training data.";
  }
  if (x === "MEDIUM") {
    return "Confidence: Medium. Signal is strong but not extreme.";
  }
  return "Confidence: Low. Treat this as advisory and monitor updates.";
}

function probabilityBand(prob) {
  if (prob >= 0.90) return "Very High (>= 0.90)";
  if (prob >= 0.75) return "High (0.75 - 0.89)";
  if (prob >= 0.50) return "Moderate (0.50 - 0.74)";
  if (prob >= 0.25) return "Low (0.25 - 0.49)";
  return "Very Low (< 0.25)";
}

function riskBand(score) {
  if (score >= 60) return "High (>= 60)";
  if (score >= 45) return "Elevated (45 - 59)";
  if (score >= 30) return "Moderate (30 - 44)";
  return "Low (< 30)";
}

function buildWhyReasons(current, comparison) {
  const reasons = [];
  const probPct = (current.pred_prob_critical_t1 || 0) * 100;
  const heatStress = current.heat_stress_score || 0;
  const tmax = current.tmax_c || 0;
  const rain = current.rain_mm || 0;
  const delta = comparison?.risk_delta ?? null;

  if (probPct >= 90) {
    reasons.push(`Model critical probability is very high at ${probPct.toFixed(1)}%.`);
  } else if (probPct >= 75) {
    reasons.push(`Model critical probability is elevated at ${probPct.toFixed(1)}%.`);
  } else if (probPct >= 55) {
    reasons.push(`Model indicates moderate critical probability at ${probPct.toFixed(1)}%.`);
  }

  if (heatStress >= 50) {
    reasons.push(`Heat stress score is high (${heatStress.toFixed(1)}), increasing health risk.`);
  } else if (heatStress >= 35) {
    reasons.push(`Heat stress score is moderate (${heatStress.toFixed(1)}).`);
  }

  if (tmax >= 42) {
    reasons.push(`Maximum temperature is extreme at ${tmax.toFixed(1)} deg C.`);
  } else if (tmax >= 39) {
    reasons.push(`Maximum temperature is high at ${tmax.toFixed(1)} deg C.`);
  }

  if (rain <= 0.1) {
    reasons.push("No meaningful rainfall support is expected, so cooling relief is limited.");
  }

  if (delta !== null) {
    if (delta >= 5) {
      reasons.push(`Risk increased sharply by ${delta.toFixed(2)} vs previous date.`);
    } else if (delta <= -5) {
      reasons.push(`Risk dropped by ${Math.abs(delta).toFixed(2)} vs previous date.`);
    }
  }

  reasons.push(
    "Final alert color follows policy thresholds using risk score, model probability, and guardrail balancing."
  );

  return reasons.slice(0, 3);
}

function buildPolicySummary(current) {
  const prob = current.pred_prob_critical_t1 || 0;
  const score = current.risk_score_next_day || 0;
  const alert = String(current.alert_level_next_day || "GREEN").toUpperCase();
  return `Final alert: ${alert}. This is derived from Probability Band (${probabilityBand(prob)}) + Risk Band (${riskBand(score)}), then adjusted by guardrail policy if needed.`;
}

function safetyPlan(current) {
  const tmax = current.tmax_c || 0;
  if (tmax >= 43) {
    return {
      hydration: "4.5 - 5.5 L/day",
      ors: "Every 2-3 hours",
      window: "Before 10:00 AM / After 5:00 PM",
      riskGroup: "Elderly, children, outdoor workers",
      summary: "Severe heat expected. Use shaded breaks, light clothing, and strict hydration discipline.",
    };
  }
  if (tmax >= 40) {
    return {
      hydration: "3.5 - 4.5 L/day",
      ors: "2-3 times/day",
      window: "Before 11:00 AM / After 4:30 PM",
      riskGroup: "Outdoor workers, chronic patients",
      summary: "High heat expected. Avoid long direct sun exposure in peak afternoon.",
    };
  }
  if (tmax >= 36) {
    return {
      hydration: "3.0 - 3.5 L/day",
      ors: "1-2 times/day",
      window: "Before 12:00 PM / After 4:00 PM",
      riskGroup: "Elderly and children",
      summary: "Moderate heat. Maintain fluids and monitor fatigue signs.",
    };
  }
  return {
    hydration: "2.5 - 3.0 L/day",
    ors: "If needed",
    window: "Normal schedule",
    riskGroup: "Sensitive groups only",
    summary: "Low heat stress conditions. Routine precautions are enough.",
  };
}

function setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}

function applyLanguage() {
  const t = labels[currentLang];
  setText("heroKicker", t.heroKicker);
  setText("heroTitle", t.heroTitle);
  setText("refreshBtn", t.refresh);
  setText("alertPanelTitle", t.alertPanelTitle);
  setText("mapTitle", t.mapTitle);
  setText("trendTitle", t.trendTitle);
  setText("topTitle", t.topTitle);
  setText("mixTitle", t.mixTitle);
  setText("riserTitle", t.riserTitle);
  setText("planTitle", t.planTitle);
  setText("labAction", t.action);
  setText("labProb", t.prob);
  setText("labConf", t.conf);
  setText("labRank", t.rank);
  setText("pill1", t.pill1);
  setText("pill2", t.pill2);
  setText("pill3", t.pill3);
  setText("thRank", t.thRank);
  setText("thDistrict", t.thDistrict);
  setText("thAlert", t.thAlert);
  setText("thRisk", t.thRisk);
  setText("thTmax", t.thTmax);
  setText("weatherTitle", t.weatherTitle);
  setText("downloadDayBtn", t.dayCsv);
  setText("whyTitle", "Why This Alert?");
}

function setFsButtonText() {
  const t = labels[currentLang];
  if (document.fullscreenElement) {
    els.fsBtn.textContent = t.fsOn;
  } else {
    els.fsBtn.textContent = t.fsOff;
  }
}

async function jget(url) {
  const r = await fetch(url);
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${url}`);
  return r.json();
}

function setMixChips(mix) {
  const keys = ["RED", "ORANGE", "YELLOW", "GREEN"];
  els.mixChips.innerHTML = keys
    .map((k) => `<span class="chip">${k}: ${mix?.[k] ?? 0}</span>`)
    .join("");
}

function fillSelectors(meta) {
  els.district.innerHTML = meta.districts
    .map((d) => `<option value="${d}">${d}</option>`)
    .join("");
  els.date.innerHTML = meta.dates
    .map((d) => `<option value="${d}">${d}</option>`)
    .join("");
  els.date.value = meta.latest_date;
  els.forecastWindow.textContent = `Forecast window: ${meta.forecast_start} to ${meta.forecast_end}`;
  setMixChips(meta.alert_mix_latest);
}

async function loadTop10() {
  const top = await jget("/api/top10");
  els.topBody.innerHTML = "";
  (top.rows || []).forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.rank_within_date}</td>
      <td>${r.district}</td>
      <td style="color:${alertColor(r.alert_level_next_day)};font-weight:700">${r.alert_level_next_day}</td>
      <td>${r.risk_score_next_day.toFixed(2)}</td>
      <td>${r.tmax_c.toFixed(1)}</td>
    `;
    els.topBody.appendChild(tr);
  });
}

async function loadRisers() {
  const dates = Array.from(els.date.options).map((o) => o.value);
  if (dates.length < 2) return;
  const toDate = els.date.value;
  const idx = dates.indexOf(toDate);
  const fromDate = idx > 0 ? dates[idx - 1] : dates[0];
  const data = await jget(`/api/risers?from_date=${encodeURIComponent(fromDate)}&to_date=${encodeURIComponent(toDate)}`);
  els.riserBody.innerHTML = "";
  (data.rows || []).forEach((r) => {
    const tr = document.createElement("tr");
    tr.innerHTML = `
      <td>${r.district}</td>
      <td style="color:${alertColor(r.alert_level_next_day)};font-weight:700">${r.alert_level_next_day}</td>
      <td>${r.risk_prev.toFixed(2)}</td>
      <td>${r.risk_curr.toFixed(2)}</td>
      <td class="delta-up">+${r.risk_delta.toFixed(2)}</td>
    `;
    els.riserBody.appendChild(tr);
  });
}

function renderMixChart(mix) {
  const ctx = document.getElementById("mixChart");
  const data = [mix.RED || 0, mix.ORANGE || 0, mix.YELLOW || 0, mix.GREEN || 0];
  if (mixChart) mixChart.destroy();
  mixChart = new Chart(ctx, {
    type: "doughnut",
    data: {
      labels: ["RED", "ORANGE", "YELLOW", "GREEN"],
      datasets: [{ data, backgroundColor: ["#e03131", "#e8590c", "#c99700", "#2f9e44"] }],
    },
    options: {
      plugins: { legend: { labels: { color: "#dbeafe" } } },
    },
  });
}

function ensureMap() {
  if (map) return;
  map = L.map("map", { zoomControl: false }).setView([26.9, 75.8], 6);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap",
  }).addTo(map);
}

function renderMap(items) {
  ensureMap();
  if (markerLayer) markerLayer.clearLayers();
  markerLayer = L.layerGroup().addTo(map);
  items.forEach((r) => {
    if (!r.latitude || !r.longitude) return;
    const marker = L.circleMarker([r.latitude, r.longitude], {
      radius: 8,
      color: alertColor(r.alert_level_next_day),
      fillColor: alertColor(r.alert_level_next_day),
      fillOpacity: 0.7,
      weight: 2,
    });
    marker.bindPopup(
      `<b>${r.district}</b><br>Alert: ${r.alert_level_next_day}<br>Risk: ${r.risk_score_next_day.toFixed(2)}<br>Tmax: ${r.tmax_c.toFixed(1)}`
    );
    marker.addTo(markerLayer);
  });
}

function renderTrend(trend) {
  const ctx = document.getElementById("trendChart");
  const labelsX = trend.map((x) => x.date);
  const risks = trend.map((x) => x.risk_score_next_day);
  if (trendChart) trendChart.destroy();
  trendChart = new Chart(ctx, {
    type: "line",
    data: {
      labels: labelsX,
      datasets: [{
        label: "Risk Score",
        data: risks,
        tension: 0.3,
        borderWidth: 3,
        borderColor: "#3b82f6",
        backgroundColor: "rgba(59,130,246,.2)",
        pointRadius: 5,
      }],
    },
    options: {
      plugins: { legend: { labels: { color: "#dbeafe" } } },
      scales: {
        x: { ticks: { color: "#cbd5e1" }, grid: { color: "rgba(255,255,255,.08)" } },
        y: { ticks: { color: "#cbd5e1" }, grid: { color: "rgba(255,255,255,.08)" } },
      },
    },
  });
}

function renderPlanner(current) {
  const plan = safetyPlan(current);
  els.planHydration.textContent = plan.hydration;
  els.planOrs.textContent = plan.ors;
  els.planWindow.textContent = plan.window;
  els.planRiskGroup.textContent = plan.riskGroup;
  els.planSummary.textContent = plan.summary;

  const t = labels[currentLang];
  els.simpleWindow.textContent = `${t.windowPrefix} ${plan.window}`;
  els.simpleHydration.textContent = `${t.hydrationPrefix} ${plan.hydration}`;
  els.simpleOrs.textContent = `${t.orsPrefix} ${plan.ors}`;
}

function renderRiskMeter(riskScore) {
  const maxRef = 70;
  const pct = Math.max(0, Math.min(100, (riskScore / maxRef) * 100));
  els.riskMeterFill.style.width = `${pct.toFixed(0)}%`;
  els.riskMeterLabel.textContent = `${pct.toFixed(0)}%`;
}

function renderSimpleCards(current) {
  const alert = (current.alert_level_next_day || "GREEN").toUpperCase();
  els.simpleAlertTitle.textContent = alert;
  els.simpleAlertTitle.style.color = alertColor(alert);
  els.simpleAlertText.textContent = riskExplain(alert);
}

function renderWhyPanel(current, comparison) {
  const reasons = buildWhyReasons(current, comparison);
  els.whyReason1.textContent = reasons[0] || "-";
  els.whyReason2.textContent = reasons[1] || "-";
  els.whyReason3.textContent = reasons[2] || "-";

  els.whyPrevDate.textContent = comparison?.previous_date || "-";
  els.whyPrevRisk.textContent =
    comparison?.previous_risk_score === null || comparison?.previous_risk_score === undefined
      ? "-"
      : Number(comparison.previous_risk_score).toFixed(2);

  if (comparison?.risk_delta === null || comparison?.risk_delta === undefined) {
    els.whyDeltaRisk.textContent = "-";
  } else {
    const d = Number(comparison.risk_delta);
    const sign = d > 0 ? "+" : "";
    els.whyDeltaRisk.textContent = `${sign}${d.toFixed(2)}`;
  }

  els.whyPrevAlert.textContent = comparison?.previous_alert_level || "-";
  els.whyConfidence.textContent = confidenceExplain(current.confidence_tag);
  els.formulaLine.textContent = "Risk Score = 0.65 × Heat Stress Score + 0.35 × (Critical Probability × 100)";
  els.probBandText.textContent = probabilityBand(current.pred_prob_critical_t1 || 0);
  els.riskBandText.textContent = riskBand(current.risk_score_next_day || 0);
  els.policySummary.textContent = buildPolicySummary(current);
  els.tempOnlyNote.textContent = "Note: Higher temperature can increase risk, but final color also depends on model probability and policy thresholds.";
}

function renderCurrent(current, comparison) {
  els.mTmax.textContent = current.tmax_c.toFixed(1);
  els.mTmin.textContent = current.tmin_c.toFixed(1);
  els.mRain.textContent = current.rain_mm.toFixed(1);
  els.mRisk.textContent = current.risk_score_next_day.toFixed(2);
  els.alertBadge.className = `alert-badge ${alertClass(current.alert_level_next_day)}`;
  els.alertBadge.textContent = current.alert_level_next_day;
  els.actionText.textContent = current.recommended_action;
  els.probText.textContent = current.pred_prob_critical_t1.toFixed(4);
  els.confText.textContent = current.confidence_tag;
  els.rankText.textContent = String(current.rank_within_date);

  const symbol = weatherSymbol(current.tmax_c, current.rain_mm, current.alert_level_next_day);
  els.weatherIcon.textContent = symbol;
  els.weatherTag.textContent = weatherTag(current.tmax_c, current.rain_mm, current.alert_level_next_day);
  els.tipText.textContent = healthTip(current.alert_level_next_day);

  renderRiskMeter(current.risk_score_next_day);
  renderSimpleCards(current);
  renderPlanner(current);
  renderWhyPanel(current, comparison);
}

async function loadSelection() {
  const district = els.district.value;
  const date = els.date.value;
  const [districtRes, dayRes] = await Promise.all([
    jget(`/api/district?district=${encodeURIComponent(district)}&date=${encodeURIComponent(date)}`),
    jget(`/api/day?date=${encodeURIComponent(date)}`),
  ]);

  renderCurrent(districtRes.current, districtRes.comparison || null);
  renderTrend(districtRes.trend);
  renderMap(dayRes.items || []);
  renderMixChart(dayRes.alert_mix || {});
  await loadRisers();
  els.downloadDayBtn.href = `/download/day.csv?date=${encodeURIComponent(date)}`;
}

async function init() {
  const meta = await jget("/api/meta");
  fillSelectors(meta);
  await loadTop10();
  await loadSelection();
  applyLanguage();
  setFsButtonText();
}

els.district.addEventListener("change", loadSelection);
els.date.addEventListener("change", loadSelection);
els.refresh.addEventListener("click", async () => {
  await init();
});

els.langBtn.addEventListener("click", () => {
  currentLang = currentLang === "en" ? "hi" : "en";
  applyLanguage();
  setFsButtonText();
  renderPlanner({
    tmax_c: Number.parseFloat(els.mTmax.textContent) || 0,
  });
});

els.fsBtn.addEventListener("click", async () => {
  if (!document.fullscreenElement) {
    await document.documentElement.requestFullscreen();
  } else {
    await document.exitFullscreen();
  }
  setFsButtonText();
});

document.addEventListener("fullscreenchange", setFsButtonText);

init().catch((e) => {
  console.error(e);
  alert("Failed to load dashboard data. Check backend server and data files.");
});
