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
  simpleKickerAlert: document.getElementById("simpleKickerAlert"),
  simpleKickerWindow: document.getElementById("simpleKickerWindow"),
  simpleKickerWater: document.getElementById("simpleKickerWater"),
  simpleWindowNote: document.getElementById("simpleWindowNote"),
  tipLabel: document.getElementById("tipLabel"),
  formulaTitle: document.getElementById("formulaTitle"),
  probBandLabel: document.getElementById("probBandLabel"),
  riskBandLabel: document.getElementById("riskBandLabel"),
  faqTitle: document.getElementById("faqTitle"),
  faqQ1: document.getElementById("faqQ1"),
  faqA1: document.getElementById("faqA1"),
  faqQ2: document.getElementById("faqQ2"),
  faqA2: document.getElementById("faqA2"),
  faqQ3: document.getElementById("faqQ3"),
  faqA3: document.getElementById("faqA3"),
  faqQ4: document.getElementById("faqQ4"),
  faqA4: document.getElementById("faqA4"),
  districtLabel: document.getElementById("districtLabel"),
  dateLabel: document.getElementById("dateLabel"),
  whyLogic: document.getElementById("whyLogic"),
  statusUpdatedLabel: document.getElementById("statusUpdatedLabel"),
  statusApiLabel: document.getElementById("statusApiLabel"),
  statusSourceLabel: document.getElementById("statusSourceLabel"),
  statusUpdatedValue: document.getElementById("statusUpdatedValue"),
  statusApiValue: document.getElementById("statusApiValue"),
  statusSourceValue: document.getElementById("statusSourceValue"),
  mapShell: document.getElementById("mapShell"),
  mapEmpty: document.getElementById("mapEmpty"),
  trendShell: document.getElementById("trendShell"),
  topShell: document.getElementById("topShell"),
  riserShell: document.getElementById("riserShell"),
  solarKicker: document.getElementById("solarKicker"),
  solarSunriseLabel: document.getElementById("solarSunriseLabel"),
  solarSunsetLabel: document.getElementById("solarSunsetLabel"),
  solarSunrise: document.getElementById("solarSunrise"),
  solarSunset: document.getElementById("solarSunset"),
  solarDaylight: document.getElementById("solarDaylight"),
  solarArcFill: document.getElementById("solarArcFill"),
  tickerLabel: document.getElementById("tickerLabel"),
  tickerTrack: document.getElementById("tickerTrack"),
  compareTitle: document.getElementById("compareTitle"),
  compareLabel: document.getElementById("compareLabel"),
  compareDistrict: document.getElementById("compareDistrictSelect"),
  comparePairLabel: document.getElementById("comparePairLabel"),
  comparePairValue: document.getElementById("comparePairValue"),
  compareRiskLabel: document.getElementById("compareRiskLabel"),
  compareRiskValue: document.getElementById("compareRiskValue"),
  compareTmaxLabel: document.getElementById("compareTmaxLabel"),
  compareTmaxValue: document.getElementById("compareTmaxValue"),
  compareAlertLabel: document.getElementById("compareAlertLabel"),
  compareAlertValue: document.getElementById("compareAlertValue"),
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
    prob: "Heatwave Probability:",
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
    districtLabel: "District",
    dateLabel: "Forecast Date",
    simpleKickerAlert: "Today's Color Alert",
    simpleKickerWindow: "Safe Outdoor Time",
    simpleKickerWater: "Water + ORS Plan",
    simpleWindowNote: "Use extra caution during peak sun hours (11 AM - 4 PM).",
    tipLabel: "Health Tip",
    whyTitle: "Why This Alert?",
    formulaTitle: "How This Risk Score Is Calculated",
    probBandLabel: "Probability Band",
    riskBandLabel: "Risk Band",
    faqTitle: "Common User Help",
    faqQ1: "What does Green mean?",
    faqA1: "Routine monitoring. Normal daily activity can continue with regular hydration.",
    faqQ2: "What is the difference between Yellow, Orange, and Red?",
    faqA2: "Yellow: advisory. Orange: preparedness mode. Red: emergency heat-health action with minimal outdoor exposure.",
    faqQ3: "How is this prediction generated?",
    faqA3: "Next-day risk is generated from district weather inputs and model probability. Final color comes from policy logic that combines both risk score and critical probability.",
    faqQ4: "Why can two hot days have different colors?",
    faqA4: "Because alert color is not based only on temperature. The model also uses pattern-based probability, heat stress behavior, and policy guardrails.",
    whyLogic: "Alert color is policy-driven using probability, risk score, and guardrail rules.",
    forecastPrefix: "Forecast window:",
    formulaLine: "Risk Score = 0.65 × Heat Stress Score + 0.35 × (Heatwave Probability × 100)",
    tempOnlyNote: "Note: Higher temperature can increase risk, but final color also depends on model probability and policy thresholds.",
    statusUpdatedLabel: "Last Updated (IST)",
    statusApiLabel: "API Status",
    statusSourceLabel: "Source Date",
    solarKicker: "Sun Window",
    solarSunriseLabel: "Sunrise",
    solarSunsetLabel: "Sunset",
    solarDaylightPrefix: "Daylight",
    tickerLabel: "Live Heatwave Feed",
    compareTitle: "District Compare",
    compareLabel: "Compare With",
    comparePairLabel: "Pair",
    compareRiskLabel: "Risk Delta",
    compareTmaxLabel: "Tmax Delta",
    compareAlertLabel: "Alert Pair",
  },
  hi: {
    heroKicker: "Live Heat Intelligence",
    heroTitle: "Jila Heat Risk Command Center",
    refresh: "Data Refresh Karen",
    alertPanelTitle: "Alert Panel",
    mapTitle: "Risk Map Snapshot",
    trendTitle: "Jila Trend",
    topTitle: "Top-10 Hotspots",
    mixTitle: "Rajya Alert Mix",
    riserTitle: "Sabse Tez Badhte Risk Wale Jile",
    planTitle: "Personal Safety Planner",
    action: "Suggested Action:",
    prob: "Heatwave Probability:",
    conf: "Confidence:",
    rank: "Date ke andar Rank:",
    pill1: "Public Dashboard",
    pill2: "Next-Day Risk",
    pill3: "Decision Support",
    thRank: "Rank",
    thDistrict: "Jila",
    thAlert: "Alert",
    thRisk: "Risk",
    thTmax: "Tmax",
    weatherTitle: "Heat Outlook",
    fsOn: "Fullscreen Band Karein",
    fsOff: "Fullscreen",
    dayCsv: "Day CSV Download",
    windowPrefix: "Safe Time:",
    hydrationPrefix: "Paani:",
    orsPrefix: "ORS:",
    districtLabel: "Jila",
    dateLabel: "Forecast Date",
    simpleKickerAlert: "Aaj ka Color Alert",
    simpleKickerWindow: "Safe Outdoor Time",
    simpleKickerWater: "Paani + ORS Plan",
    simpleWindowNote: "Peak dhoop (11 AM - 4 PM) me extra savdhani rakhein.",
    tipLabel: "Health Tip",
    whyTitle: "Yeh Alert Kyun?",
    formulaTitle: "Risk Score Kaise Calculate Hota Hai",
    probBandLabel: "Probability Band",
    riskBandLabel: "Risk Band",
    faqTitle: "Common User Help",
    faqQ1: "Green ka matlab kya hai?",
    faqA1: "Routine monitoring. Regular hydration ke saath normal daily activity continue kar sakte hain.",
    faqQ2: "Yellow, Orange aur Red me kya fark hai?",
    faqA2: "Yellow: advisory. Orange: preparedness mode. Red: emergency heat-health action, outdoor exposure minimum rakhein.",
    faqQ3: "Yeh prediction kaise banta hai?",
    faqA3: "Next-day risk district weather inputs aur model probability se banta hai. Final color policy logic decide karti hai jo risk score + critical probability dono use karti hai.",
    faqQ4: "Do garam dino ka color alag kyun ho sakta hai?",
    faqA4: "Alert color sirf temperature se decide nahi hota. Model probability, heat stress pattern aur policy guardrails bhi count hote hain.",
    whyLogic: "Alert color policy rules par based hai jo probability, risk score aur guardrail logic use karte hain.",
    forecastPrefix: "Forecast window:",
    formulaLine: "Risk Score = 0.65 × Heat Stress Score + 0.35 × (Heatwave Probability × 100)",
    tempOnlyNote: "Note: Temperature badhne se risk badh sakta hai, lekin final color model probability aur policy thresholds par bhi depend karta hai.",
    statusUpdatedLabel: "Last Updated (IST)",
    statusApiLabel: "API Status",
    statusSourceLabel: "Source Date",
    solarKicker: "Surya Window",
    solarSunriseLabel: "Sunrise",
    solarSunsetLabel: "Sunset",
    solarDaylightPrefix: "Daylight",
    tickerLabel: "Live Heatwave Feed",
    compareTitle: "Jila Compare",
    compareLabel: "Compare With",
    comparePairLabel: "Pair",
    compareRiskLabel: "Risk Delta",
    compareTmaxLabel: "Tmax Delta",
    compareAlertLabel: "Alert Pair",
  },
};

let currentLang = "en";
let trendChart = null;
let mixChart = null;
let map = null;
let markerLayer = null;
let revealObserver = null;
let lastDayItems = [];
let lastSelectedDistrict = "";
const isHi = () => currentLang === "hi";

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

function setRefreshLoading(flag) {
  if (flag) {
    els.refresh.classList.add("is-loading");
  } else {
    els.refresh.classList.remove("is-loading");
  }
}

function setSkeletonLoading(flag) {
  const targets = [els.mapShell, els.trendShell, els.topShell, els.riserShell];
  targets.forEach((el) => {
    if (!el) return;
    if (flag) el.classList.add("is-loading");
    else el.classList.remove("is-loading");
  });
}

function animateNumber(el, target, decimals = 1) {
  const current = Number.parseFloat(String(el.textContent).replace(/[^\d.-]/g, ""));
  const start = Number.isFinite(current) ? current : target;
  const duration = 520;
  const startTs = performance.now();

  const tick = (ts) => {
    const t = Math.min(1, (ts - startTs) / duration);
    const eased = 1 - Math.pow(1 - t, 3);
    const value = start + (target - start) * eased;
    el.textContent = value.toFixed(decimals);
    if (t < 1) requestAnimationFrame(tick);
  };
  requestAnimationFrame(tick);
}

function popMetric(el) {
  el.classList.remove("value-pop");
  void el.offsetWidth;
  el.classList.add("value-pop");
}

function weatherSymbol(tmax, rain, alert) {
  if (alert === "RED") return "HEAT";
  if (rain >= 2) return "RAIN";
  if (tmax >= 42) return "VERY HOT";
  if (tmax >= 38) return "HOT";
  return "MILD";
}

function weatherIconDisplay(symbol) {
  if (symbol === "HEAT" || symbol === "VERY HOT") return "SUN";
  if (symbol === "RAIN") return "RAIN";
  if (symbol === "HOT") return "WARM";
  return "MILD";
}

function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

function formatHourMin(totalMinutes) {
  let m = Math.round(totalMinutes) % (24 * 60);
  if (m < 0) m += 24 * 60;
  const hh24 = Math.floor(m / 60);
  const mm = m % 60;
  const ampm = hh24 >= 12 ? "PM" : "AM";
  const hh12 = ((hh24 + 11) % 12) + 1;
  return `${hh12}:${String(mm).padStart(2, "0")} ${ampm}`;
}

function computeSunTimes(dateStr, lat, lon) {
  if (!Number.isFinite(lat) || !Number.isFinite(lon) || (lat === 0 && lon === 0)) return null;
  const dt = new Date(`${dateStr}T00:00:00`);
  if (Number.isNaN(dt.getTime())) return null;
  const yearStart = new Date(dt.getFullYear(), 0, 0);
  const dayOfYear = Math.floor((dt - yearStart) / 86400000);

  const gamma = (2 * Math.PI / 365) * (dayOfYear - 1);
  const eqTime = 229.18 * (
    0.000075
    + 0.001868 * Math.cos(gamma)
    - 0.032077 * Math.sin(gamma)
    - 0.014615 * Math.cos(2 * gamma)
    - 0.040849 * Math.sin(2 * gamma)
  );
  const decl = (
    0.006918
    - 0.399912 * Math.cos(gamma)
    + 0.070257 * Math.sin(gamma)
    - 0.006758 * Math.cos(2 * gamma)
    + 0.000907 * Math.sin(2 * gamma)
    - 0.002697 * Math.cos(3 * gamma)
    + 0.00148 * Math.sin(3 * gamma)
  );

  const latRad = lat * Math.PI / 180;
  const zenith = 90.833 * Math.PI / 180;
  const cosH = (Math.cos(zenith) / (Math.cos(latRad) * Math.cos(decl))) - Math.tan(latRad) * Math.tan(decl);
  if (cosH > 1 || cosH < -1) return null;

  const hourAngle = Math.acos(clamp(cosH, -1, 1));
  const hourDeg = hourAngle * 180 / Math.PI;

  // UTC minutes of solar events
  const solarNoonUTC = 720 - 4 * lon - eqTime;
  const sunriseUTC = solarNoonUTC - 4 * hourDeg;
  const sunsetUTC = solarNoonUTC + 4 * hourDeg;

  // IST (+330 min)
  const sunriseIST = sunriseUTC + 330;
  const sunsetIST = sunsetUTC + 330;
  const daylightMin = ((sunsetIST - sunriseIST) + 24 * 60) % (24 * 60);

  return {
    sunriseText: formatHourMin(sunriseIST),
    sunsetText: formatHourMin(sunsetIST),
    daylightHours: daylightMin / 60,
    sunriseMin: sunriseIST,
    sunsetMin: sunsetIST,
  };
}

function weatherTag(tmax, rain, alert) {
  if (isHi()) {
    if (alert === "RED") return "Bahut high heat risk. Outdoor exposure kam se kam rakhein.";
    if (alert === "ORANGE") return "High heat stress sambhav hai. Hydration breaks plan karein.";
    if (alert === "YELLOW") return "Moderate caution. Symptoms aur fluids par nazar rakhein.";
    if (rain > 0) return "Halka rain support mil raha hai, heat manageable ho sakti hai.";
    return "Conditions filhal relatively stable hain.";
  }
  if (alert === "RED") return "Extreme heat risk. Reduce outdoor exposure.";
  if (alert === "ORANGE") return "High heat stress likely. Plan hydration breaks.";
  if (alert === "YELLOW") return "Moderate caution. Track symptoms and fluids.";
  if (rain > 0) return "Mild rain support with manageable heat.";
  return "Relatively stable conditions.";
}

function healthTip(alert) {
  if (isHi()) {
    if (alert === "RED") return "Emergency tip: Har 30-45 min ORS + paani, 11am-4pm outdoor work avoid karein.";
    if (alert === "ORANGE") return "Preparedness tip: Paani, cap aur shaded rest breaks ready rakhein.";
    if (alert === "YELLOW") return "Advisory tip: Fluids badhayein, khas kar elderly aur outdoor workers ke liye.";
    return "Routine tip: Hydration steady rakhein aur local updates dekhte rahein.";
  }
  if (alert === "RED") return "Emergency tip: ORS + water every 30-45 min, avoid 11am-4pm outdoor work.";
  if (alert === "ORANGE") return "Preparedness tip: carry water, cap, and schedule shaded rest breaks.";
  if (alert === "YELLOW") return "Advisory tip: increase fluids, especially for elderly and outdoor workers.";
  return "Routine tip: keep hydration steady and monitor local updates.";
}

function riskExplain(alert) {
  if (isHi()) {
    if (alert === "RED") return "Very high risk. Sirf zaruri kaam ke liye hi bahar niklein.";
    if (alert === "ORANGE") return "High risk. Outdoor time limit karein aur frequent breaks lein.";
    if (alert === "YELLOW") return "Moderate risk. Hydration aur rest intervals badhayein.";
    return "Low risk. Routine precautions kafi hain.";
  }
  if (alert === "RED") return "Very high heat risk. Avoid outdoor exposure except essential tasks.";
  if (alert === "ORANGE") return "High risk. Limit outdoor exposure and take frequent breaks.";
  if (alert === "YELLOW") return "Moderate risk. Increase hydration and rest intervals.";
  return "Low risk. Routine precautions are sufficient.";
}

function confidenceExplain(tag) {
  const x = String(tag || "").toUpperCase();
  if (isHi()) {
    if (x === "HIGH") return "Confidence: High. Similar weather patterns training data me dekhe gaye hain.";
    if (x === "MEDIUM") return "Confidence: Medium. Signal strong hai, par extreme nahi.";
    return "Confidence: Low. Isse advisory samjhein aur updates monitor karein.";
  }
  if (x === "HIGH") {
    return "Confidence: High. Similar weather patterns were seen in training data.";
  }
  if (x === "MEDIUM") {
    return "Confidence: Medium. Signal is strong but not extreme.";
  }
  return "Confidence: Low. Treat this as advisory and monitor updates.";
}

function probabilityBand(prob) {
  if (isHi()) {
    if (prob >= 0.90) return "Bahut High (>= 0.90)";
    if (prob >= 0.75) return "High (0.75 - 0.89)";
    if (prob >= 0.50) return "Moderate (0.50 - 0.74)";
    if (prob >= 0.25) return "Low (0.25 - 0.49)";
    return "Bahut Low (< 0.25)";
  }
  if (prob >= 0.90) return "Very High (>= 0.90)";
  if (prob >= 0.75) return "High (0.75 - 0.89)";
  if (prob >= 0.50) return "Moderate (0.50 - 0.74)";
  if (prob >= 0.25) return "Low (0.25 - 0.49)";
  return "Very Low (< 0.25)";
}

function riskBand(score) {
  if (isHi()) {
    if (score >= 60) return "High (>= 60)";
    if (score >= 45) return "Elevated (45 - 59)";
    if (score >= 30) return "Moderate (30 - 44)";
    return "Low (< 30)";
  }
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
    reasons.push(isHi() ? `Model critical probability bahut high hai: ${probPct.toFixed(1)}%.` : `Model critical probability is very high at ${probPct.toFixed(1)}%.`);
  } else if (probPct >= 75) {
    reasons.push(isHi() ? `Model critical probability elevated hai: ${probPct.toFixed(1)}%.` : `Model critical probability is elevated at ${probPct.toFixed(1)}%.`);
  } else if (probPct >= 55) {
    reasons.push(isHi() ? `Model moderate critical probability dikhata hai: ${probPct.toFixed(1)}%.` : `Model indicates moderate critical probability at ${probPct.toFixed(1)}%.`);
  }

  if (heatStress >= 50) {
    reasons.push(isHi() ? `Heat stress score high hai (${heatStress.toFixed(1)}), health risk badh raha hai.` : `Heat stress score is high (${heatStress.toFixed(1)}), increasing health risk.`);
  } else if (heatStress >= 35) {
    reasons.push(isHi() ? `Heat stress score moderate hai (${heatStress.toFixed(1)}).` : `Heat stress score is moderate (${heatStress.toFixed(1)}).`);
  }

  if (tmax >= 42) {
    reasons.push(isHi() ? `Maximum temperature extreme hai: ${tmax.toFixed(1)} deg C.` : `Maximum temperature is extreme at ${tmax.toFixed(1)} deg C.`);
  } else if (tmax >= 39) {
    reasons.push(isHi() ? `Maximum temperature high hai: ${tmax.toFixed(1)} deg C.` : `Maximum temperature is high at ${tmax.toFixed(1)} deg C.`);
  }

  if (rain <= 0.1) {
    reasons.push(isHi() ? "Meaningful rainfall support expected nahi hai, isliye cooling relief limited hai." : "No meaningful rainfall support is expected, so cooling relief is limited.");
  }

  if (delta !== null) {
    if (delta >= 5) {
      reasons.push(isHi() ? `Previous date ke mukable risk ${delta.toFixed(2)} se tez bada hai.` : `Risk increased sharply by ${delta.toFixed(2)} vs previous date.`);
    } else if (delta <= -5) {
      reasons.push(isHi() ? `Previous date ke mukable risk ${Math.abs(delta).toFixed(2)} se gira hai.` : `Risk dropped by ${Math.abs(delta).toFixed(2)} vs previous date.`);
    }
  }

  reasons.push(
    isHi()
      ? "Final alert color policy thresholds follow karta hai jo risk score, model probability aur guardrail balancing use karte hain."
      : "Final alert color follows policy thresholds using risk score, model probability, and guardrail balancing."
  );

  return reasons.slice(0, 3);
}

function buildPolicySummary(current) {
  const prob = current.pred_prob_critical_t1 || 0;
  const score = current.risk_score_next_day || 0;
  const alert = String(current.alert_level_next_day || "GREEN").toUpperCase();
  if (isHi()) {
    return `Final alert: ${alert}. Yeh Probability Band (${probabilityBand(prob)}) + Risk Band (${riskBand(score)}) se nikalta hai, fir zarurat par guardrail policy se adjust hota hai.`;
  }
  return `Final alert: ${alert}. This is derived from Probability Band (${probabilityBand(prob)}) + Risk Band (${riskBand(score)}), then adjusted by guardrail policy if needed.`;
}

function safetyPlan(current) {
  const tmax = current.tmax_c || 0;
  if (tmax >= 43) {
    return {
      hydration: "4.5 - 5.5 L/day",
      ors: isHi() ? "Har 2-3 ghante" : "Every 2-3 hours",
      window: "Before 10:00 AM / After 5:00 PM",
      riskGroup: isHi() ? "Elderly, bachche, outdoor workers" : "Elderly, children, outdoor workers",
      summary: isHi() ? "Severe heat expected hai. Shade breaks, light clothing aur strict hydration follow karein." : "Severe heat expected. Use shaded breaks, light clothing, and strict hydration discipline.",
    };
  }
  if (tmax >= 40) {
    return {
      hydration: "3.5 - 4.5 L/day",
      ors: "2-3 times/day",
      window: "Before 11:00 AM / After 4:30 PM",
      riskGroup: isHi() ? "Outdoor workers, chronic patients" : "Outdoor workers, chronic patients",
      summary: isHi() ? "High heat expected hai. Peak afternoon me direct sun exposure avoid karein." : "High heat expected. Avoid long direct sun exposure in peak afternoon.",
    };
  }
  if (tmax >= 36) {
    return {
      hydration: "3.0 - 3.5 L/day",
      ors: "1-2 times/day",
      window: "Before 12:00 PM / After 4:00 PM",
      riskGroup: isHi() ? "Elderly aur bachche" : "Elderly and children",
      summary: isHi() ? "Moderate heat. Fluids maintain rakhein aur fatigue signs monitor karein." : "Moderate heat. Maintain fluids and monitor fatigue signs.",
    };
  }
  return {
    hydration: "2.5 - 3.0 L/day",
    ors: isHi() ? "Agar zarurat ho" : "If needed",
    window: isHi() ? "Normal schedule" : "Normal schedule",
    riskGroup: isHi() ? "Sensitive groups only" : "Sensitive groups only",
    summary: isHi() ? "Low heat stress conditions. Routine precautions kafi hain." : "Low heat stress conditions. Routine precautions are enough.",
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
  setText("whyTitle", t.whyTitle);
  setText("districtLabel", t.districtLabel);
  setText("dateLabel", t.dateLabel);
  setText("simpleKickerAlert", t.simpleKickerAlert);
  setText("simpleKickerWindow", t.simpleKickerWindow);
  setText("simpleKickerWater", t.simpleKickerWater);
  setText("simpleWindowNote", t.simpleWindowNote);
  setText("tipLabel", t.tipLabel);
  setText("formulaTitle", t.formulaTitle);
  setText("probBandLabel", t.probBandLabel);
  setText("riskBandLabel", t.riskBandLabel);
  setText("faqTitle", t.faqTitle);
  setText("faqQ1", t.faqQ1);
  setText("faqA1", t.faqA1);
  setText("faqQ2", t.faqQ2);
  setText("faqA2", t.faqA2);
  setText("faqQ3", t.faqQ3);
  setText("faqA3", t.faqA3);
  setText("faqQ4", t.faqQ4);
  setText("faqA4", t.faqA4);
  setText("whyLogic", t.whyLogic);
  setText("statusUpdatedLabel", t.statusUpdatedLabel);
  setText("statusApiLabel", t.statusApiLabel);
  setText("statusSourceLabel", t.statusSourceLabel);
  setText("solarKicker", t.solarKicker);
  setText("solarSunriseLabel", t.solarSunriseLabel);
  setText("solarSunsetLabel", t.solarSunsetLabel);
  setText("tickerLabel", t.tickerLabel);
  setText("compareTitle", t.compareTitle);
  setText("compareLabel", t.compareLabel);
  setText("comparePairLabel", t.comparePairLabel);
  setText("compareRiskLabel", t.compareRiskLabel);
  setText("compareTmaxLabel", t.compareTmaxLabel);
  setText("compareAlertLabel", t.compareAlertLabel);
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
  const r = await fetch(url, {
    cache: "no-store",
    headers: { "Cache-Control": "no-cache" },
  });
  if (!r.ok) throw new Error(`HTTP ${r.status}: ${url}`);
  return r.json();
}

function formatApiStatus(apiStatus, pipelineStatus) {
  const a = String(apiStatus || "").toLowerCase();
  const p = String(pipelineStatus || "").toLowerCase();
  if (a === "success") return "live";
  if (a === "stale_fallback") return "fallback";
  if (p === "success" || p === "partial_success") return "pipeline-ok";
  return a || p || "unknown";
}

function formatUtcToIst(utcText) {
  const s = String(utcText || "").trim();
  if (!s) return "-";
  const m = s.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}):(\d{2})(?::(\d{2}))?\s+UTC$/i);
  if (!m) return s;

  const sec = Number(m[4] || "0");
  const dt = new Date(`${m[1]}T${m[2]}:${m[3]}:${String(sec).padStart(2, "0")}Z`);
  if (Number.isNaN(dt.getTime())) return s;

  const fmt = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Asia/Kolkata",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  });
  const parts = Object.fromEntries(fmt.formatToParts(dt).map((p) => [p.type, p.value]));
  return `${parts.year}-${parts.month}-${parts.day} ${parts.hour}:${parts.minute} IST`;
}

function renderStatus(meta) {
  const updated = formatUtcToIst(meta.live_file_updated_at_utc || "-");
  const sourceDate = meta.api_source_date || meta.latest_date || "-";
  const apiStatus = formatApiStatus(meta.api_status, meta.pipeline_status);

  els.statusUpdatedValue.textContent = updated;
  els.statusApiValue.textContent = apiStatus;
  els.statusSourceValue.textContent = sourceDate;

  const apiItem = els.statusApiValue.closest(".status-item");
  if (apiItem) {
    apiItem.classList.remove("status-live", "status-fallback", "status-unknown");
    if (apiStatus === "live") {
      apiItem.classList.add("status-live");
    } else if (apiStatus === "fallback") {
      apiItem.classList.add("status-fallback");
    } else {
      apiItem.classList.add("status-unknown");
    }
  }
}

function setMixChips(mix) {
  const keys = ["RED", "ORANGE", "YELLOW", "GREEN"];
  els.mixChips.innerHTML = keys
    .map((k) => `<span class="chip">${k}: ${mix?.[k] ?? 0}</span>`)
    .join("");
}

function formatSigned(x, decimals = 2) {
  const n = Number(x || 0);
  const sign = n > 0 ? "+" : "";
  return `${sign}${n.toFixed(decimals)}`;
}

function renderTicker(items, date) {
  if (!els.tickerTrack) return;
  const sorted = [...items].sort((a, b) => (b.risk_score_next_day || 0) - (a.risk_score_next_day || 0));
  const top = sorted.slice(0, 5);
  const total = sorted.length;
  const red = sorted.filter((x) => String(x.alert_level_next_day).toUpperCase() === "RED").length;
  const orange = sorted.filter((x) => String(x.alert_level_next_day).toUpperCase() === "ORANGE").length;
  const yel = sorted.filter((x) => String(x.alert_level_next_day).toUpperCase() === "YELLOW").length;
  const snippets = top.map((r) => `${r.district} ${r.alert_level_next_day} (${(r.risk_score_next_day || 0).toFixed(1)})`);
  const base = `Date ${date} | Districts ${total} | RED ${red} | ORANGE ${orange} | YELLOW ${yel}`;
  const text = `${base} | Top heat districts: ${snippets.join(" | ")} | ${base} | Top heat districts: ${snippets.join(" | ")}`;
  els.tickerTrack.textContent = text;
}

function ensureCompareSelection(metaDistricts) {
  if (!els.compareDistrict) return;
  const current = els.district.value;
  const previous = els.compareDistrict.value;
  els.compareDistrict.innerHTML = metaDistricts.map((d) => `<option value="${d}">${d}</option>`).join("");
  if (previous && previous !== current && metaDistricts.includes(previous)) {
    els.compareDistrict.value = previous;
    return;
  }
  const fallback = metaDistricts.find((d) => d !== current) || current;
  els.compareDistrict.value = fallback;
}

function renderComparePanel(currentDistrict, dayItems) {
  if (!els.compareDistrict || !dayItems?.length) return;
  if (els.compareDistrict.value === currentDistrict) {
    const alt = Array.from(els.compareDistrict.options).map((o) => o.value).find((d) => d !== currentDistrict);
    if (alt) els.compareDistrict.value = alt;
  }
  const cmpDistrict = els.compareDistrict.value;
  const a = dayItems.find((x) => x.district === currentDistrict);
  const b = dayItems.find((x) => x.district === cmpDistrict);
  if (!a || !b) return;

  const riskDelta = (a.risk_score_next_day || 0) - (b.risk_score_next_day || 0);
  const tmaxDelta = (a.tmax_c || 0) - (b.tmax_c || 0);
  const pair = `${currentDistrict} vs ${cmpDistrict}`;
  const alertPair = `${a.alert_level_next_day} vs ${b.alert_level_next_day}`;

  els.comparePairValue.textContent = pair;
  els.compareRiskValue.textContent = formatSigned(riskDelta, 2);
  els.compareTmaxValue.textContent = `${formatSigned(tmaxDelta, 1)} deg C`;
  els.compareAlertValue.textContent = alertPair;

  els.compareRiskValue.classList.remove("compare-pos", "compare-neg");
  els.compareTmaxValue.classList.remove("compare-pos", "compare-neg");
  els.compareRiskValue.classList.add(riskDelta >= 0 ? "compare-pos" : "compare-neg");
  els.compareTmaxValue.classList.add(tmaxDelta >= 0 ? "compare-pos" : "compare-neg");
}

function fillSelectors(meta) {
  els.district.innerHTML = meta.districts
    .map((d) => `<option value="${d}">${d}</option>`)
    .join("");
  els.date.innerHTML = meta.dates
    .map((d) => `<option value="${d}">${d}</option>`)
    .join("");
  els.date.value = meta.latest_date;
  ensureCompareSelection(meta.districts || []);
  const t = labels[currentLang];
  els.forecastWindow.textContent = `${t.forecastPrefix} ${meta.forecast_start} to ${meta.forecast_end}`;
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
      animation: { duration: 700, easing: "easeOutQuart" },
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
  try {
    ensureMap();
  } catch (_) {
    if (els.mapEmpty) {
      els.mapEmpty.textContent = "Map failed to initialize. Please refresh.";
      els.mapEmpty.classList.add("show");
    }
    return;
  }
  if (markerLayer) markerLayer.clearLayers();
  markerLayer = L.layerGroup().addTo(map);
  const districtLabel = isHi() ? "Jila" : "District";
  const alertLabel = "Alert";
  const riskLabel = "Risk";
  const tmaxLabel = "Tmax";
  const bounds = [];
  let markerCount = 0;
  items.forEach((r) => {
    const lat = Number(r.latitude);
    const lon = Number(r.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || (lat === 0 && lon === 0)) return;
    const marker = L.circleMarker([lat, lon], {
      radius: 8,
      color: alertColor(r.alert_level_next_day),
      fillColor: alertColor(r.alert_level_next_day),
      fillOpacity: 0.7,
      weight: 2,
    });
    marker.bindPopup(
      `<b>${districtLabel}: ${r.district}</b><br>${alertLabel}: ${r.alert_level_next_day}<br>${riskLabel}: ${r.risk_score_next_day.toFixed(2)}<br>${tmaxLabel}: ${r.tmax_c.toFixed(1)}`
    );
    marker.addTo(markerLayer);
    bounds.push([lat, lon]);
    markerCount += 1;
  });

  if (markerCount > 0) {
    if (els.mapEmpty) els.mapEmpty.classList.remove("show");
    map.fitBounds(bounds, { padding: [16, 16], maxZoom: 7 });
  } else if (els.mapEmpty) {
    els.mapEmpty.textContent = "Map data unavailable for this selection.";
    els.mapEmpty.classList.add("show");
  }
  setTimeout(() => map.invalidateSize(), 120);
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
      animation: { duration: 760, easing: "easeOutQuart" },
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

function renderSolarCard(current, date) {
  const t = labels[currentLang];
  const lat = Number(current.latitude || 0);
  const lon = Number(current.longitude || 0);
  const sun = computeSunTimes(date, lat, lon);

  if (!sun) {
    els.solarSunrise.textContent = "--";
    els.solarSunset.textContent = "--";
    els.solarDaylight.textContent = `${t.solarDaylightPrefix}: --`;
    els.solarArcFill.style.width = "50%";
    document.body.classList.remove("theme-day", "theme-dusk", "theme-night");
    document.body.classList.add("theme-night");
    return;
  }

  els.solarSunrise.textContent = sun.sunriseText;
  els.solarSunset.textContent = sun.sunsetText;
  els.solarDaylight.textContent = `${t.solarDaylightPrefix}: ${sun.daylightHours.toFixed(1)}h`;
  const daylightPct = clamp((sun.daylightHours - 9) / 5, 0, 1) * 100;
  els.solarArcFill.style.width = `${daylightPct.toFixed(0)}%`;

  const nowIST = new Date();
  const hh = Number(new Intl.DateTimeFormat("en-US", { hour: "2-digit", hour12: false, timeZone: "Asia/Kolkata" }).format(nowIST));
  const mm = Number(new Intl.DateTimeFormat("en-US", { minute: "2-digit", hour12: false, timeZone: "Asia/Kolkata" }).format(nowIST));
  const nowMin = hh * 60 + mm;

  const sunriseMin = ((sun.sunriseMin % (24 * 60)) + (24 * 60)) % (24 * 60);
  const sunsetMin = ((sun.sunsetMin % (24 * 60)) + (24 * 60)) % (24 * 60);

  document.body.classList.remove("theme-day", "theme-dusk", "theme-night");
  if (nowMin >= sunriseMin + 45 && nowMin <= sunsetMin - 60) {
    document.body.classList.add("theme-day");
  } else if ((nowMin >= sunriseMin - 30 && nowMin < sunriseMin + 45) || (nowMin > sunsetMin - 60 && nowMin <= sunsetMin + 50)) {
    document.body.classList.add("theme-dusk");
  } else {
    document.body.classList.add("theme-night");
  }
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
  const t = labels[currentLang];
  const reasons = buildWhyReasons(current, comparison);
  const reasonEls = [els.whyReason1, els.whyReason2, els.whyReason3];
  reasonEls.forEach((el, idx) => {
    const txt = String(reasons[idx] || "").trim();
    if (txt) {
      el.textContent = txt;
      el.classList.remove("is-empty");
    } else {
      el.textContent = "";
      el.classList.add("is-empty");
    }
  });

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
  els.formulaLine.textContent = t.formulaLine;
  els.probBandText.textContent = probabilityBand(current.pred_prob_critical_t1 || 0);
  els.riskBandText.textContent = riskBand(current.risk_score_next_day || 0);
  els.policySummary.textContent = buildPolicySummary(current);
  els.tempOnlyNote.textContent = t.tempOnlyNote;
}

function renderCurrent(current, comparison) {
  animateNumber(els.mTmax, current.tmax_c, 1);
  animateNumber(els.mTmin, current.tmin_c, 1);
  animateNumber(els.mRain, current.rain_mm, 1);
  animateNumber(els.mRisk, current.risk_score_next_day, 2);
  popMetric(els.mTmax);
  popMetric(els.mTmin);
  popMetric(els.mRain);
  popMetric(els.mRisk);

  els.alertBadge.className = `alert-badge ${alertClass(current.alert_level_next_day)}`;
  els.alertBadge.textContent = current.alert_level_next_day;
  const alertUpper = String(current.alert_level_next_day || "GREEN").toUpperCase();
  if (alertUpper === "RED") els.alertBadge.classList.add("hot-motion");
  else if (alertUpper === "ORANGE" || alertUpper === "YELLOW") els.alertBadge.classList.add("warn-motion");
  else els.alertBadge.classList.add("calm-motion");
  els.actionText.textContent = current.recommended_action;
  els.probText.textContent = current.pred_prob_critical_t1.toFixed(4);
  els.confText.textContent = current.confidence_tag;
  els.rankText.textContent = String(current.rank_within_date);

  const symbol = weatherSymbol(current.tmax_c, current.rain_mm, current.alert_level_next_day);
  els.weatherIcon.classList.remove("weather-hot", "weather-rain", "weather-mild", "weather-cloud");
  if (symbol === "HEAT" || symbol === "VERY HOT") els.weatherIcon.classList.add("weather-hot");
  else if (symbol === "RAIN") els.weatherIcon.classList.add("weather-rain");
  else if (symbol === "HOT") els.weatherIcon.classList.add("weather-cloud");
  else els.weatherIcon.classList.add("weather-mild");
  els.weatherIcon.textContent = weatherIconDisplay(symbol);
  els.weatherTag.textContent = weatherTag(current.tmax_c, current.rain_mm, current.alert_level_next_day);
  els.tipText.textContent = healthTip(current.alert_level_next_day);

  renderRiskMeter(current.risk_score_next_day);
  renderSimpleCards(current);
  renderPlanner(current);
  renderWhyPanel(current, comparison);
}

function setupRevealAnimations() {
  const candidates = document.querySelectorAll(
    ".ticker, .hero, .controls, .compare, .metric-card, .simple-card, .grid-two .card, .faq-card, .status-item"
  );
  candidates.forEach((el, idx) => {
    el.classList.add("reveal-up");
    el.style.transitionDelay = `${Math.min(idx * 20, 220)}ms`;
  });

  if (!("IntersectionObserver" in window)) {
    candidates.forEach((el) => el.classList.add("is-visible"));
    return;
  }

  revealObserver = new IntersectionObserver(
    (entries) => {
      entries.forEach((entry) => {
        if (entry.isIntersecting) {
          entry.target.classList.add("is-visible");
          revealObserver.unobserve(entry.target);
        }
      });
    },
    { threshold: 0.08 }
  );

  candidates.forEach((el) => revealObserver.observe(el));
}

async function loadSelection() {
  setSkeletonLoading(true);
  const district = els.district.value;
  const date = els.date.value;
  try {
    const [districtRes, dayRes] = await Promise.all([
      jget(`/api/district?district=${encodeURIComponent(district)}&date=${encodeURIComponent(date)}`),
      jget(`/api/day?date=${encodeURIComponent(date)}`),
    ]);

    renderCurrent(districtRes.current, districtRes.comparison || null);
    renderSolarCard(districtRes.current, date);
    renderTrend(districtRes.trend);
    renderMap(dayRes.items || []);
    renderMixChart(dayRes.alert_mix || {});
    lastDayItems = dayRes.items || [];
    lastSelectedDistrict = district;
    renderTicker(lastDayItems, date);
    renderComparePanel(district, lastDayItems);
    await loadRisers();
    els.downloadDayBtn.href = `/download/day.csv?date=${encodeURIComponent(date)}`;
  } finally {
    setSkeletonLoading(false);
  }
}

async function init() {
  setRefreshLoading(true);
  setSkeletonLoading(true);
  const meta = await jget("/api/meta");
  renderStatus(meta);
  fillSelectors(meta);
  await loadTop10();
  await loadSelection();
  applyLanguage();
  setFsButtonText();
  setRefreshLoading(false);
}

els.district.addEventListener("change", loadSelection);
els.date.addEventListener("change", loadSelection);
els.compareDistrict.addEventListener("change", () => {
  if (!lastDayItems.length || !lastSelectedDistrict) return;
  renderComparePanel(lastSelectedDistrict, lastDayItems);
});
els.refresh.addEventListener("click", async () => {
  try {
    await init();
  } finally {
    setRefreshLoading(false);
  }
});

els.langBtn.addEventListener("click", async () => {
  currentLang = currentLang === "en" ? "hi" : "en";
  await loadSelection();
  applyLanguage();
  setFsButtonText();
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

setupRevealAnimations();

init().catch((e) => {
  console.error(e);
  setRefreshLoading(false);
  setSkeletonLoading(false);
  alert("Failed to load dashboard data. Check backend server and data files.");
});
