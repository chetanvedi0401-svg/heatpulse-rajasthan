const el = {
  district: document.getElementById("miniDistrict"),
  date: document.getElementById("miniDate"),
  refresh: document.getElementById("miniRefresh"),
  alert: document.getElementById("miniAlert"),
  risk: document.getElementById("miniRisk"),
  prob: document.getElementById("miniProb"),
  tmax: document.getElementById("miniTmax"),
  tmin: document.getElementById("miniTmin"),
  rain: document.getElementById("miniRain"),
  mix: document.getElementById("miniMix"),
  top: document.getElementById("miniTop"),
};

async function jget(url) {
  const r = await fetch(url, { cache: "no-store" });
  if (!r.ok) throw new Error(`HTTP ${r.status}`);
  return r.json();
}

function tagClass(level) {
  const x = String(level || "").toLowerCase();
  if (x === "red") return "red";
  if (x === "orange") return "orange";
  if (x === "yellow") return "yellow";
  return "green";
}

async function loadMeta() {
  const meta = await jget("/api/meta");
  el.district.innerHTML = (meta.districts || []).map((d) => `<option value="${d}">${d}</option>`).join("");
  el.date.innerHTML = (meta.dates || []).map((d) => `<option value="${d}">${d}</option>`).join("");
  if (meta.latest_date) el.date.value = meta.latest_date;
}

async function loadMini() {
  const district = el.district.value;
  const date = el.date.value;
  const out = await jget(`/api/mini?district=${encodeURIComponent(district)}&date=${encodeURIComponent(date)}`);
  const c = out.current || {};

  el.alert.textContent = String(c.alert_level_next_day || "-");
  el.risk.textContent = Number(c.risk_score_next_day || 0).toFixed(2);
  el.prob.textContent = Number(c.pred_prob_critical_t1 || 0).toFixed(3);
  el.tmax.textContent = `${Number(c.tmax_c || 0).toFixed(1)} deg C`;
  el.tmin.textContent = `${Number(c.tmin_c || 0).toFixed(1)} deg C`;
  el.rain.textContent = `${Number(c.rain_mm || 0).toFixed(1)} mm`;

  const m = out.mix || {};
  el.mix.textContent = `R${m.RED || 0} O${m.ORANGE || 0} Y${m.YELLOW || 0} G${m.GREEN || 0}`;

  const topRows = out.top3 || [];
  el.top.innerHTML = topRows
    .map(
      (r) => `
      <div class="top-item">
        <strong>${r.district}</strong>
        <span>${Number(r.risk || 0).toFixed(1)}</span>
        <span class="tag ${tagClass(r.alert)}">${r.alert}</span>
      </div>`
    )
    .join("");
}

async function init() {
  await loadMeta();
  await loadMini();
}

el.refresh.addEventListener("click", loadMini);
el.district.addEventListener("change", loadMini);
el.date.addEventListener("change", loadMini);

init().catch((e) => {
  console.error(e);
  alert("Mini dashboard failed to load.");
});

