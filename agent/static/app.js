const state = { sessionId: null };

const messagesEl = document.getElementById("messages");
const thinkingEl = document.getElementById("thinking");
const form = document.getElementById("chat-form");
const input = document.getElementById("chat-input");
const auditToggle = document.getElementById("audit-toggle");
const auditDrawer = document.getElementById("audit-drawer");
const auditList = document.getElementById("audit-list");
const auditRefresh = document.getElementById("audit-refresh");

async function init() {
  const res = await fetch("/api/session", { method: "POST" });
  const data = await res.json();
  state.sessionId = data.session_id;
}

function scrollToBottom() {
  messagesEl.scrollTop = messagesEl.scrollHeight;
}

function showThinking(show) {
  thinkingEl.classList.toggle("hidden", !show);
}

function appendUserMessage(text) {
  const wrap = document.createElement("div");
  wrap.className = "message user";
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.textContent = text;
  wrap.appendChild(bubble);
  messagesEl.appendChild(wrap);
  scrollToBottom();
}

function appendAgentMessage(markdownText, toolCalls) {
  const wrap = document.createElement("div");
  wrap.className = "message agent";
  const bubble = document.createElement("div");
  bubble.className = "bubble";
  bubble.innerHTML = marked.parse(markdownText || "");
  wrap.appendChild(bubble);

  (toolCalls || []).forEach((toolCall) => {
    const card = renderToolCard(toolCall);
    if (card) wrap.appendChild(card);
  });

  messagesEl.appendChild(wrap);
  scrollToBottom();
  return wrap;
}

function toolCardShell(badgeClass, badgeLabel, statusText, statusClass) {
  const card = document.createElement("div");
  card.className = "tool-card";
  const header = document.createElement("div");
  header.className = "tool-card-header";
  const badge = document.createElement("span");
  badge.className = `tool-badge ${badgeClass}`;
  badge.textContent = badgeLabel;
  const status = document.createElement("span");
  status.className = `status-pill ${statusClass || ""}`;
  status.textContent = statusText || "";
  header.appendChild(badge);
  header.appendChild(status);
  const body = document.createElement("div");
  body.className = "tool-card-body";
  card.appendChild(header);
  card.appendChild(body);
  return { card, body, status };
}

function renderErrorBox(body, message) {
  const box = document.createElement("div");
  box.className = "error-box";
  box.textContent = message;
  body.appendChild(box);
}

function renderToolCard(toolCall) {
  if (toolCall.name === "sql_query") return renderSqlResult(toolCall.result);
  if (toolCall.name === "explain_scenario") return renderExplainResult(toolCall.result);
  if (toolCall.name === "run_scenario_simulation") return renderActionCard(toolCall.result);
  return null;
}

function renderSqlResult(result) {
  const { card, body } = toolCardShell(
    "sql", "SQL QUERY",
    result.error ? "rejected" : `${result.row_count} row(s)`,
    result.error ? "error" : "ok"
  );
  if (result.error) {
    renderErrorBox(body, result.error);
    return card;
  }
  const wrap = document.createElement("div");
  wrap.className = "result-table-wrap";
  const table = document.createElement("table");
  table.className = "result-table";
  const thead = document.createElement("thead");
  const headRow = document.createElement("tr");
  result.columns.forEach((col) => {
    const th = document.createElement("th");
    th.textContent = col;
    headRow.appendChild(th);
  });
  thead.appendChild(headRow);
  table.appendChild(thead);
  const tbody = document.createElement("tbody");
  result.rows.forEach((row) => {
    const tr = document.createElement("tr");
    row.forEach((cell) => {
      const td = document.createElement("td");
      td.textContent = cell === null ? "" : String(cell);
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  table.appendChild(tbody);
  wrap.appendChild(table);
  body.appendChild(wrap);
  return card;
}

function renderExplainResult(result) {
  const { card, body } = toolCardShell(
    "explain", "EXPLAIN SCENARIO",
    result.error ? "rejected" : `${result.chart_data.length} segment(s)`,
    result.error ? "error" : "ok"
  );
  if (result.error) {
    renderErrorBox(body, result.error);
    return card;
  }
  const chartWrap = document.createElement("div");
  chartWrap.className = "chart-wrap";
  const canvas = document.createElement("canvas");
  chartWrap.appendChild(canvas);
  body.appendChild(chartWrap);

  const labels = result.chart_data.map((r) => r.segment);
  const lossRates = result.chart_data.map((r) => r.loss_rate_pct);

  new Chart(canvas, {
    type: "bar",
    data: {
      labels,
      datasets: [{
        label: "Loss rate (%)",
        data: lossRates,
        backgroundColor: "rgba(108, 141, 255, 0.55)",
        borderColor: "rgba(108, 141, 255, 1)",
        borderWidth: 1,
        borderRadius: 4,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: "#8b93a7" }, grid: { color: "#242b3d" } },
        y: { ticks: { color: "#8b93a7" }, grid: { color: "#242b3d" }, beginAtZero: true },
      },
    },
  });
  return card;
}

function renderActionCard(result) {
  const { card, body, status } = toolCardShell(
    "action", "PROPOSED ACTION",
    result.error ? "rejected" : "awaiting confirmation",
    result.error ? "error" : ""
  );
  card.classList.add("confirm-card");
  if (result.error) {
    renderErrorBox(body, result.error);
    return card;
  }

  const preview = document.createElement("pre");
  preview.className = "code-block command-preview";
  preview.textContent = result.command_preview;
  body.appendChild(preview);

  if (result.dry_run_output) {
    const dryRun = document.createElement("pre");
    dryRun.className = "code-block";
    dryRun.textContent = result.dry_run_output;
    body.appendChild(dryRun);
  }

  const actions = document.createElement("div");
  actions.className = "confirm-actions";
  const confirmBtn = document.createElement("button");
  confirmBtn.className = "btn-confirm";
  confirmBtn.textContent = "Confirm & run for real";
  const cancelBtn = document.createElement("button");
  cancelBtn.className = "btn-cancel";
  cancelBtn.textContent = "Cancel";
  actions.appendChild(confirmBtn);
  actions.appendChild(cancelBtn);
  body.appendChild(actions);

  const note = document.createElement("div");
  note.className = "confirm-note";
  note.textContent = "Nothing real has run yet. This requires your explicit confirmation.";
  body.appendChild(note);

  cancelBtn.addEventListener("click", () => {
    confirmBtn.disabled = true;
    cancelBtn.disabled = true;
    status.textContent = "cancelled";
    note.textContent = "Cancelled — nothing real was run.";
  });

  confirmBtn.addEventListener("click", async () => {
    confirmBtn.disabled = true;
    cancelBtn.disabled = true;
    status.textContent = "running…";
    note.textContent = "Running the real job now — this can take a couple of minutes.";

    const confirmRes = await fetch("/api/confirm", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: state.sessionId, pending_action_id: result.pending_action_id }),
    });
    const { run_id } = await confirmRes.json();

    const poll = async () => {
      const statusRes = await fetch(`/api/run-status/${run_id}`);
      const statusData = await statusRes.json();
      if (statusData.status === "running") {
        setTimeout(poll, 2500);
        return;
      }
      status.textContent = statusData.status;
      status.className = `status-pill ${statusData.status === "done" ? "ok" : "error"}`;
      note.textContent = statusData.status === "done" ? "Completed." : "The run failed — see the agent's reply below.";
      appendAgentMessage(statusData.reply, []);
    };
    poll();
  });

  return card;
}

async function sendMessage(event) {
  event.preventDefault();
  const text = input.value.trim();
  if (!text) return;
  appendUserMessage(text);
  input.value = "";
  showThinking(true);

  try {
    const res = await fetch("/api/chat", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ session_id: state.sessionId, message: text }),
    });
    const data = await res.json();
    showThinking(false);
    appendAgentMessage(data.reply, data.tool_calls || []);
  } catch (err) {
    showThinking(false);
    appendAgentMessage(`_Request failed: ${err}_`, []);
  }
}

function statusClassFor(status) {
  if (status === "ok" || status === "done") return "ok";
  if (status === "error") return "error";
  return "";
}

async function refreshAudit() {
  const res = await fetch("/api/audit?limit=50");
  const data = await res.json();
  auditList.innerHTML = "";
  [...data.events].reverse().forEach((event) => {
    const item = document.createElement("div");
    item.className = "audit-item";

    const top = document.createElement("div");
    top.className = "audit-item-top";
    const tool = document.createElement("span");
    tool.className = "audit-item-tool";
    tool.textContent = event.tool || "?";
    const ts = document.createElement("span");
    ts.className = "audit-item-ts";
    ts.textContent = event.ts || "";
    top.appendChild(tool);
    top.appendChild(ts);
    item.appendChild(top);

    const status = document.createElement("span");
    status.className = `status-pill ${statusClassFor(event.status)}`;
    status.textContent = event.status || "";
    item.appendChild(status);

    const details = document.createElement("details");
    const summary = document.createElement("summary");
    summary.textContent = "raw event";
    const pre = document.createElement("pre");
    pre.textContent = JSON.stringify(event, null, 2);
    details.appendChild(summary);
    details.appendChild(pre);
    item.appendChild(details);

    auditList.appendChild(item);
  });
}

auditToggle.addEventListener("click", () => {
  const nowHidden = auditDrawer.classList.toggle("hidden");
  if (!nowHidden) refreshAudit();
});
auditRefresh.addEventListener("click", refreshAudit);
form.addEventListener("submit", sendMessage);

init();
