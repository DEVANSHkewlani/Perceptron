// --- Script Block 0 ---

    let isSystemConnected = false;
    let ws = null;
    let last_status = null;

    // System clock UTC ticker
    function updateClock() {
        const now = new Date();
        const h = String(now.getUTCHours()).padStart(2, '0');
        const m = String(now.getUTCMinutes()).padStart(2, '0');
        const s = String(now.getUTCSeconds()).padStart(2, '0');
        document.getElementById('utc-clock').textContent = `UTC ${h}:${m}:${s}`;
    }
    setInterval(updateClock, 1000);
    updateClock();

    // Uptime counter starts only after a successful backend CONNECT handshake.
    let uptimeSeconds = 0;
    setInterval(() => {
        if (!isSystemConnected) return;
        uptimeSeconds++;
        const h = String(Math.floor(uptimeSeconds / 3600)).padStart(2, '0');
        const m = String(Math.floor((uptimeSeconds % 3600) / 60)).padStart(2, '0');
        const s = String(uptimeSeconds % 60).padStart(2, '0');
        document.getElementById('uptime-display').textContent = `UPTIME: ${h}:${m}:${s}`;
    }, 1000);

    // ==========================================
    // LIVE STATE STORES
    // ==========================================
    let AGENTS_LIST = [];
    let WORLD_ANOMALIES = [];
    let ENTITIES = [];

    let DECISION_LOG = {
        agent_id: "",
        situation_assessment: "",
        root_cause_hypothesis: "None",
        recommended_action: "None",
        action_parameters: {},
        requires_human_approval: false,
        confidence: 1.0,
        tokens_used: 0,
        cost_usd: 0.0
    };

    let PLAN_STEPS = [];
    let FEEDBACK_ITEMS = [];

    // Selected state
    let selectedAgent = null;
    let allLogs = [];
    let activeOpenModal = null;
    let activeDetailAgentId = null;

    let perceptionAdapters = [];

    function renderAgentMarquee() {
        const marquee = document.getElementById("agent-marquee");
        if (!marquee) return;
        if (!isSystemConnected) {
            marquee.innerHTML = "<span>[SYSTEM OFFLINE] Connect to begin monitoring.</span>";
            return;
        }
        if (allLogs.length === 0) {
            marquee.innerHTML = "<span>[HEALTH] All general planner/executor worker nodes reporting nominal.</span>";
            return;
        }
        const recent = allLogs.slice(-3).reverse();
        marquee.innerHTML = recent.map(e => {
            const timeStr = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '—';
            const payloadStr = typeof e.payload === 'object' ? JSON.stringify(e.payload) : (e.payload || '');
            return `<span>[${timeStr}] [${(e.event_type || '').toUpperCase()}] ${e.source_id || 'system'}: ${payloadStr}</span>`;
        }).join('');
    }

    function renderPerceptionAdapters() {
        const grid = document.getElementById("perception-adapters-grid");
        if (!grid) return;
        grid.innerHTML = "";
        perceptionAdapters.forEach(ad => {
            const card = document.createElement("div");
            card.className = "border border-[#e2e2e2] p-1.5 bg-[#f9f9f9] flex flex-col justify-between";
            const shortName = ad.source.split(":")[1] || ad.source;
            const shortType = ad.type.replace("Adapter", "").replace("Webhook", "").toUpperCase();
            
            const isUp = ad.status === "ACTIVE" || ad.status === "UP";
            const statusClass = isUp ? "text-green-500 font-bold" : "text-red-500 font-bold";
            
            card.innerHTML = `
                <span class="font-bold text-black block truncate" title="${ad.type}: ${ad.source}">${shortType}: ${shortName}</span>
                <div class="mt-1 text-[8px] text-neutral-500 space-y-0.5">
                    <div>Status: <span class="${statusClass}">[ ${ad.status} ]</span></div>
                    <div>Lag: <span class="text-black font-bold">${ad.lag}</span></div>
                    <div>Latency: <span class="text-black font-bold">${ad.latency}</span></div>
                </div>
            `;
            grid.appendChild(card);
        });
    }

    // ==========================================
    // INITIALIZATION & RENDER
    // ==========================================
    function initUI() {
        allLogs = [];
        WORLD_ANOMALIES = [];
        PLAN_STEPS = [];
        AGENTS_LIST = [];
        
        renderAgentCards();
        renderAnomalies();
        renderAgentMarquee();
        renderEntityTable();
        
        // Reset reasoning views to nominal offline
        const meta = document.getElementById("reasoning-meta-block");
        const json = document.getElementById("reasoning-decision-json");
        if (meta && json) {
            meta.innerHTML = `
                <span class="text-neutral-500 font-bold block mb-1">SYSTEM STATE: DISCONNECTED</span>
                <span class="text-neutral-600 block mb-2 text-[9px]">Awaiting gateway connection...</span>
            `;
            json.textContent = "{\n  \"status\": \"offline\"\n}";
        }
        
        renderPlanSteps();
        renderFeedbackLog();
        
        // Render adapters list as offline/empty
        perceptionAdapters = [];
        renderPerceptionAdapters();
        
        // Empty sparklines
        sparklineData = { kafka: [0], db: [0], auth: [0] };
        setupSparklines();
        
        const consoleEl = document.getElementById("kafka-log-console");
        if (consoleEl) {
            consoleEl.innerHTML = "<p class='text-neutral-500'>&gt; System offline. Click CONNECT toggle in header to begin.</p>";
        }
        
        // Reset top display bar values
        const healthBadge = document.getElementById("sys-health-status");
        if (healthBadge) {
            healthBadge.textContent = "[ OFFLINE ]";
            healthBadge.className = "text-neutral-500 font-bold";
        }
        const throughputVal = document.getElementById("kafka-rate-events");
        if (throughputVal) throughputVal.textContent = "0.0 msg/s";
        const throughputDisplay = document.getElementById("throughput-display");
        if (throughputDisplay) throughputDisplay.textContent = "0.0 MSG/S";
        const onlineDisplay = document.getElementById("agents-online-display");
        if (onlineDisplay) onlineDisplay.textContent = "0/0 ONLINE";
        const activeAnomsBadge = document.getElementById("anomalies-active-display");
        if (activeAnomsBadge) {
            activeAnomsBadge.textContent = "0 ACTIVE";
            activeAnomsBadge.className = "font-bold text-neutral-400";
        }
        const uptimeDisplay = document.getElementById("uptime-display");
        if (uptimeDisplay) uptimeDisplay.textContent = "SYSTEM OFFLINE";

        updateConnectionUI();
        stopMetricsPolling();
    }

    async function toggleSystemConnection() {
        if (!isSystemConnected) {
            const btn = document.getElementById("system-connection-toggle");
            if (btn) btn.textContent = "CONNECTING";
            try {
                const res = await fetch("/api/connect", { method: "POST" });
                const payload = await res.json();
                if (!res.ok || !payload.connected) {
                    initUI();
                    const reason = (payload.errors || ["Connection prerequisites failed"]).join("; ");
                    const consoleEl = document.getElementById("kafka-log-console");
                    if (consoleEl) consoleEl.innerHTML = `<p class='text-red-500'>&gt; CONNECT failed: ${reason}</p>`;
                    return;
                }
                isSystemConnected = true;
                uptimeSeconds = 0;
                ENTITIES = [
                    { id: "svc:api-gateway", status: "NOMINAL", trend: "↑" },
                    { id: "svc:product-service", status: "NOMINAL", trend: "↑" },
                    { id: "svc:order-service", status: "NOMINAL", trend: "↑" },
                    { id: "svc:cart-service", status: "NOMINAL", trend: "↑" },
                    { id: "svc:user-service", status: "NOMINAL", trend: "↑" },
                    { id: "svc:notification-service", status: "NOMINAL", trend: "↑" },
                    { id: "db:shopcore-postgres", status: "NOMINAL", trend: "↑" },
                    { id: "cache:shopcore-redis", status: "NOMINAL", trend: "↑" },
                    { id: "queue:order-events", status: "NOMINAL", trend: "↑" }
                ];
                renderEntityTable();
                updateConnectionUI();
                connectWebSocket();
                startMetricsPolling();
            } catch (err) {
                initUI();
                const consoleEl = document.getElementById("kafka-log-console");
                if (consoleEl) consoleEl.innerHTML = `<p class='text-red-500'>&gt; CONNECT failed: ${err}</p>`;
            }
            return;
        }

        try {
            await fetch("/api/disconnect", { method: "POST" });
        } catch (err) {
            console.warn("Disconnect request failed", err);
        }
        isSystemConnected = false;
        if (ws) {
            ws.close();
            ws = null;
        }
        initUI();
    }

    function updateConnectionUI() {
        const btn = document.getElementById("system-connection-toggle");
        if (!btn) return;
        if (isSystemConnected) {
            btn.className = "px-2.5 py-0.5 border border-green-500 bg-green-950 text-green-400 font-bold hover:bg-green-900 transition-all uppercase tracking-wider text-[9px]";
            btn.textContent = "CONNECTED";
            const dot = document.getElementById("connection-dot");
            if (dot) dot.className = "w-1.5 h-1.5 rounded-full bg-green-400 blink-status";
        } else {
            btn.className = "px-2.5 py-0.5 border border-red-500 bg-red-950 text-red-400 font-bold hover:bg-red-900 transition-all uppercase tracking-wider text-[9px]";
            btn.textContent = "OFFLINE";
            const dot = document.getElementById("connection-dot");
            if (dot) dot.className = "w-1.5 h-1.5 rounded-full bg-neutral-500";
        }
    }

    // Render agent grid P06
    function renderAgentCards() {
        const grid = document.getElementById("agent-cards-grid");
        if (!grid) return;
        grid.innerHTML = "";
        AGENTS_LIST.forEach(agent => {
            const card = document.createElement("div");
            const isActive = agent.status === "ACTIVE";
            const borderClass = "border-[#e2e2e2] bg-white";
            
            const statusIndicator = isActive 
                ? `<span class="text-black block font-bold text-[9px] blink-status">[ ACTIVE ]</span>` 
                : `<span class="text-neutral-400 block text-[9px]">[ IDLE ]</span>`;
            
            card.className = `border ${borderClass} p-2 flex flex-col justify-between items-center text-center cursor-pointer hover:border-black transition-all h-[70px] select-none`;
            card.innerHTML = `
                <div class="flex justify-between w-full text-neutral-400 text-[8px] font-mono">
                    <span>${agent.short}</span>
                    <span class="text-neutral-400 block text-[8px] font-bold">[INSPECT]</span>
                </div>
                <span class="font-mono text-[10px] font-bold block text-black">${agent.role}</span>
                ${statusIndicator}
            `;
            card.onclick = (e) => {
                e.stopPropagation();
                openAgentDetailModal(agent.id);
            };
            grid.appendChild(card);
        });
    }

    // Render active anomalies P04
    function renderAnomalies() {
        const container = document.getElementById("world-anomalies-list");
        if (!container) return;
        container.innerHTML = "";
        
        if (WORLD_ANOMALIES.length === 0) {
            container.innerHTML = `<div class="text-neutral-400 font-mono text-[10px] py-4">No active anomalies detected on target environment.</div>`;
            return;
        }

        WORLD_ANOMALIES.forEach(a => {
            const item = document.createElement("div");
            item.className = "border border-[#e2e2e2] p-3 bg-white min-w-[200px] hover:border-black shrink-0 flex flex-col justify-between h-[90px]";
            item.innerHTML = `
                <div>
                    <div class="flex justify-between items-center mb-1">
                        <span class="font-mono text-[8px] font-bold text-red-600 uppercase">${a.id} // ${a.type}</span>
                        <span class="font-mono text-[8px] bg-red-50 text-red-600 border border-red-200 px-1">${a.severity}</span>
                    </div>
                    <span class="font-mono font-bold text-[10px] block text-black">${a.entity}</span>
                </div>
                <span class="font-mono text-[8px] text-neutral-400 block truncate mt-2">Chain: ${a.chain}</span>
            `;
            container.appendChild(item);
        });
    }

    // Render entity table P04
    function renderEntityTable() {
        const tbody = document.querySelector("#world-entity-table tbody");
        if (!tbody) return;
        tbody.innerHTML = "";
        ENTITIES.forEach(e => {
            const tr = document.createElement("tr");
            tr.className = "hover:bg-neutral-50";
            
            // Health color class
            const healthClass = e.status === "NOMINAL" ? "text-black" : (e.status === "STRESSED" ? "text-yellow-600 font-bold" : "text-red-600 font-bold");
            
            tr.innerHTML = `
                <td class="py-2 text-[10px]">${e.id}</td>
                <td class="py-2 text-[10px] text-center"><span class="${healthClass}">[ ${e.status} ]</span></td>
                <td class="py-2 text-[10px] text-right font-bold text-neutral-400">${e.trend}</td>
            `;
            tbody.appendChild(tr);
        });
    }

    // Render Reasoning Core P05
    function renderReasoningCore() {
        const meta = document.getElementById("reasoning-meta-block");
        const json = document.getElementById("reasoning-decision-json");
        if (!meta || !json) return;
        
        meta.innerHTML = `
            <span class="text-white font-bold block mb-1">DECISION: ${DECISION_LOG.recommended_action}</span>
            <span class="text-neutral-400 block mb-2 text-[9px]">HYPOTHESIS: ${DECISION_LOG.root_cause_hypothesis}</span>
        `;

        // Fill the stat boxes
        const confEl = document.getElementById('reasoning-conf');
        const tokensEl = document.getElementById('reasoning-tokens');
        const pathEl = document.getElementById('reasoning-path');
        if (confEl) confEl.textContent = `${(DECISION_LOG.confidence * 100).toFixed(0)}%`;
        if (tokensEl) tokensEl.textContent = DECISION_LOG.tokens_used;
        if (pathEl) pathEl.textContent = DECISION_LOG.tokens_used > 500 ? 'LLM' : 'HEURISTIC';
        
        json.textContent = JSON.stringify(DECISION_LOG.action_parameters, null, 2);
    }

    // Render Plan Steps P07
    function renderPlanSteps() {
        const container = document.getElementById("plan-steps-view");
        if (!container) return;
        container.innerHTML = "";

        if (PLAN_STEPS.length === 0) {
            container.innerHTML = '<p class="font-mono text-[9px] text-neutral-600 py-4 text-center">No active plans. System idle.</p>';
            return;
        }

        // Group steps by planId
        const plansMap = {};
        PLAN_STEPS.forEach(p => {
            const pid = p.planId || 'default';
            if (!plansMap[pid]) {
                plansMap[pid] = {
                    goal: p.goal || 'Remediation Plan',
                    steps: []
                };
            }
            plansMap[pid].steps.push(p);
        });

        // Update footer stats
        const completed = PLAN_STEPS.filter(p => p.status === 'COMPLETED' || p.status === 'succeeded').length;
        const planCountEl = document.getElementById('plan-count');
        const planDoneEl = document.getElementById('plan-done-count');
        const planApprovalEl = document.getElementById('plan-approval-count');
        
        if (planCountEl) planCountEl.textContent = Object.keys(plansMap).length;
        if (planDoneEl) planDoneEl.textContent = completed;
        if (planApprovalEl) planApprovalEl.textContent = PLAN_STEPS.filter(p => p.status === 'waiting_approval').length || 0;

        // Render each plan group
        Object.keys(plansMap).forEach((pid, gIdx) => {
            const planGroup = plansMap[pid];
            
            // Create a section header for the plan
            const headerEl = document.createElement("div");
            headerEl.className = `flex justify-between items-center text-[8px] font-mono border-b border-[#333] pb-1 text-neutral-400 ${gIdx > 0 ? 'mt-3' : 'mt-1'}`;
            headerEl.innerHTML = `
                <span class="font-bold text-neutral-300 truncate w-48" title="${planGroup.goal}">PLAN: ${planGroup.goal}</span>
                <span class="text-[7px] border border-[#222] px-1 bg-[#111]">ID: ${pid.substring(0,8)}</span>
            `;
            container.appendChild(headerEl);

            planGroup.steps.forEach(p => {
                const stepEl = document.createElement("div");
                stepEl.className = "flex items-center justify-between text-[10px] font-mono border-b border-[#1a1a1a] py-1.5 pl-2";
                
                let badgeStyle = "border border-[#333] text-neutral-600 bg-[#0a0a0a]";
                if (p.status === "COMPLETED" || p.status === "succeeded") badgeStyle = "bg-white text-black font-bold";
                if (p.status === "RUNNING" || p.status === "running") badgeStyle = "bg-[#222] text-white font-bold";
                
                let statusClass = "text-neutral-600 font-mono";
                let inlineStyle = "";
                const upperStatus = (p.status || 'pending').toUpperCase();
                if (upperStatus === "RUNNING") statusClass = "text-white font-bold blink-status";
                if (upperStatus === "COMPLETED" || upperStatus === "SUCCEEDED") statusClass = "text-neutral-500";
                if (upperStatus === "WAITING_APPROVAL") {
                    statusClass = "font-bold";
                    inlineStyle = "color: #fbbf24;";
                }

                stepEl.innerHTML = `
                    <div class="flex items-center gap-3">
                        <span class="w-4 h-4 rounded-none flex items-center justify-center text-[9px] ${badgeStyle}">${p.step}</span>
                        <span class="opacity-90 font-bold text-white">${p.name}</span>
                    </div>
                    <span class="${statusClass}" style="${inlineStyle}">[ ${upperStatus} ]</span>
                `;
                container.appendChild(stepEl);
            });
        });
    }


    // Render Feedback Loop P09
    function renderFeedbackLog() {
        const log = document.getElementById("feedback-log");
        if (!log) return;
        log.innerHTML = "";

        if (FEEDBACK_ITEMS.length === 0) {
            log.innerHTML = '<p class="font-mono text-[9px] text-neutral-500 py-4 text-center">Awaiting feedback loop learning events...</p>';
            return;
        }

        FEEDBACK_ITEMS.forEach(f => {
            log.innerHTML += `
                <div class="border-l border-neutral-700 pl-2">
                    <p class="text-neutral-500 text-[8px] mb-0.5">${f.time} // ${f.type}</p>
                    <p class="font-bold text-white">${f.name}</p>
                    <p class="opacity-60 text-neutral-400">${f.result}</p>
                </div>
            `;
        });
    }

    // Render Live Log Console
    function renderKafkaLogsConsole() {
        const consoleEl = document.getElementById("kafka-log-console");
        if (!consoleEl) return;
        consoleEl.innerHTML = "";

        allLogs.forEach(e => {
            const colorClass = e.severity === "critical" ? "text-red-500" : (e.severity === "high" || e.severity === "medium" ? "text-yellow-500" : "text-white");
            consoleEl.innerHTML += `<p class="${colorClass}">&gt; [${e.timestamp || '22:15:30'}] [${(e.event_type || '').toUpperCase()}] ${JSON.stringify(e.payload)}</p>`;
        });
        consoleEl.scrollTop = consoleEl.scrollHeight;
    }

    // ==========================================
    // CANVASES & SPARKLINE METRICS
    // ==========================================
    let sparklineData = {
        kafka: [40, 50, 42, 60, 54, 70, 65, 80, 75, 87, 84, 98, 92, 110, 105, 120, 115, 124],
        db: [60, 62, 58, 65, 70, 75, 72, 80, 84, 88, 92],
        auth: [120, 140, 130, 200, 310, 420, 390, 580, 740, 890]
    };

    function setupSparklines() {
        drawSparkline("kafka-throughput-spark", sparklineData.kafka, "#111111", "#f0f0f0");
        drawSparkline("db-load-spark", sparklineData.db, "#22c55e", "#050505");
        drawSparkline("auth-latency-spark", sparklineData.auth, "#f59e0b", "#050505");
    }

    function drawSparkline(canvasId, dataPoints, lineColor, bgColor) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const ctx = canvas.getContext("2d");
        
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);
        
        // Background
        ctx.fillStyle = bgColor || "#ffffff";
        ctx.fillRect(0, 0, rect.width, rect.height);
        
        if (!dataPoints || dataPoints.length < 2) return;
        
        const step = rect.width / (dataPoints.length - 1);
        const max = Math.max(...dataPoints) * 1.1;
        const min = Math.min(...dataPoints) * 0.9;
        const range = max - min || 1;
        
        const points = dataPoints.map((point, i) => ({
            x: i * step,
            y: rect.height - ((point - min) / range) * (rect.height * 0.85) - rect.height * 0.05
        }));

        // Fill gradient under line
        const grad = ctx.createLinearGradient(0, 0, 0, rect.height);
        grad.addColorStop(0, (lineColor || '#000000') + '33');
        grad.addColorStop(1, (lineColor || '#000000') + '00');
        ctx.beginPath();
        ctx.moveTo(points[0].x, rect.height);
        points.forEach(p => ctx.lineTo(p.x, p.y));
        ctx.lineTo(points[points.length - 1].x, rect.height);
        ctx.closePath();
        ctx.fillStyle = grad;
        ctx.fill();

        // Line
        ctx.strokeStyle = lineColor || "#000000";
        ctx.lineWidth = 1.5;
        ctx.lineJoin = "round";
        ctx.beginPath();
        points.forEach((p, i) => {
            if (i === 0) ctx.moveTo(p.x, p.y);
            else ctx.lineTo(p.x, p.y);
        });
        ctx.stroke();

        // Latest value dot
        const last = points[points.length - 1];
        ctx.beginPath();
        ctx.arc(last.x, last.y, 2.5, 0, Math.PI * 2);
        ctx.fillStyle = lineColor || "#000000";
        ctx.fill();
    }

    function stopLocalSimulation() {
        // Simulation mode was removed. Kept as a no-op for older event handlers.
    }

    function startConsoleTicker() {
        const consoleEl = document.getElementById("kafka-log-console");
        if (!consoleEl) return;
        consoleEl.innerHTML = "";
        
        // Show first 4 lines immediately
        for(let i=0; i<Math.min(4, allLogs.length); i++) {
            const e = allLogs[i];
            const colorClass = e.severity === "critical" ? "text-red-500" : "text-white";
            consoleEl.innerHTML += `<p class="${colorClass}">&gt; [${e.timestamp || '22:15:30'}] [${(e.event_type || '').toUpperCase()}] ${JSON.stringify(e.payload)}</p>`;
        }
        consoleEl.scrollTop = consoleEl.scrollHeight;
    }

    // ==========================================
    // SIDEBAR CATEGORY FILTERING
    // ==========================================
    function filterCategory(cat) {
        document.querySelectorAll(".sb-nav-btn").forEach(btn => {
            btn.className = "sb-nav-btn flex items-center gap-3 p-2.5 text-neutral-600 hover:bg-neutral-100 hover:text-black text-left w-full transition-all border border-transparent";
        });
        
        const activeBtn = document.getElementById(`sb-nav-${cat}`);
        if (activeBtn) {
            activeBtn.className = "sb-nav-btn flex items-center gap-3 p-2.5 bg-black border border-black text-white font-bold text-left w-full transition-all";
        }
        
        const pKafka     = document.getElementById("p-kafka");
        const pPerception= document.getElementById("p-perception");
        const pMemory    = document.getElementById("p-memory");
        const pWorld     = document.getElementById("p-world");
        const pReasoning = document.getElementById("p-reasoning");
        const pAgents    = document.getElementById("p-agents");
        const pPlanning  = document.getElementById("p-planning");
        const pTemporal  = document.getElementById("p-temporal");
        const pFeedback  = document.getElementById("p-feedback");
        const flow       = document.getElementById("flowchart-section");
        const pipeline   = document.getElementById("pipeline-page");
        const allPanels  = [pKafka, pPerception, pMemory, pWorld, pReasoning, pAgents, pPlanning, pTemporal, pFeedback];

        // Hide pipeline page first — only shown for 'pipeline' route
        if (pipeline) pipeline.classList.add("panel-hidden");

        if (cat === "pipeline") {
            // Hide all dashboard panels + simple flowchart
            allPanels.forEach(p => p && p.classList.add("panel-hidden"));
            if (flow) flow.classList.add("panel-hidden");
            // Show the detailed pipeline page
            if (pipeline) pipeline.classList.remove("panel-hidden");
            return;
        }

        if (cat === "all") {
            allPanels.forEach(p => p && p.classList.remove("panel-hidden"));
            if (flow) flow.classList.remove("panel-hidden");
            // Restore correct col-span + theme classes for each panel
            if (pKafka)     pKafka.className     = "md:col-span-8 border border-[#e2e2e2] bg-white p-4 cursor-pointer hover:border-neutral-500 transition-all flex flex-col justify-between";
            if (pPerception)pPerception.className = "md:col-span-4 border border-[#e2e2e2] bg-white p-4 cursor-pointer hover:border-neutral-500 transition-all flex flex-col justify-between";
            if (pWorld)     pWorld.className      = "md:col-span-6 border border-[#e2e2e2] bg-white p-4 cursor-pointer hover:border-neutral-500 transition-all flex flex-col justify-between";
            if (pReasoning) pReasoning.className  = "md:col-span-6 border border-[#1a1a1a] bg-black p-4 cursor-pointer hover:border-[#444] transition-all flex flex-col justify-between";
            if (pPlanning)  pPlanning.className   = "md:col-span-4 border border-[#1a1a1a] bg-black p-4 cursor-pointer hover:border-[#444] transition-all flex flex-col";
            if (pTemporal)  pTemporal.className   = "md:col-span-4 border border-[#1a1a1a] bg-black p-4 cursor-pointer hover:border-[#444] transition-all flex flex-col";
            if (pFeedback)  pFeedback.className   = "md:col-span-4 border border-[#e2e2e2] bg-white p-4 cursor-pointer hover:border-neutral-500 transition-all flex flex-col justify-between";
        } else {
            allPanels.forEach(p => p && p.classList.add("panel-hidden"));
            if (flow) flow.classList.add("panel-hidden");

            if (cat === "perception") {
                if (pPerception) { pPerception.classList.remove("panel-hidden"); pPerception.className = "md:col-span-12 border border-[#e2e2e2] bg-white p-4 cursor-pointer hover:border-neutral-500 transition-all"; }
                if (pKafka)      { pKafka.classList.remove("panel-hidden");      pKafka.className      = "md:col-span-12 border border-[#e2e2e2] bg-white p-4 cursor-pointer hover:border-neutral-500 transition-all"; }
            } else if (cat === "memory") {
                if (pMemory)   { pMemory.classList.remove("panel-hidden"); }
                if (pFeedback) { pFeedback.classList.remove("panel-hidden"); pFeedback.className = "md:col-span-12 border border-[#e2e2e2] bg-white p-4 cursor-pointer hover:border-neutral-500 transition-all"; }
            } else if (cat === "reasoning") {
                if (pReasoning) { pReasoning.classList.remove("panel-hidden"); pReasoning.className = "md:col-span-12 border border-[#1a1a1a] bg-black p-4 cursor-pointer hover:border-[#444] transition-all"; }
            } else if (cat === "action") {
                if (pPlanning) { pPlanning.classList.remove("panel-hidden"); pPlanning.className = "md:col-span-12 border border-[#1a1a1a] bg-black p-4 cursor-pointer hover:border-[#444] transition-all"; }
                if (pTemporal) { pTemporal.classList.remove("panel-hidden"); pTemporal.className = "md:col-span-12 border border-[#1a1a1a] bg-black p-4 cursor-pointer hover:border-[#444] transition-all"; }
            }
        }
    }

    // Global search routing
    function handleGlobalSearch(query) {
        query = query.toLowerCase().trim();
        if (!query) {
            document.querySelectorAll("#world-entity-table tr").forEach(tr => tr.classList.remove("bg-neutral-100"));
            return;
        }
        
        document.querySelectorAll("#world-entity-table tbody tr").forEach(tr => {
            const id = tr.cells[0].textContent.toLowerCase();
            if (id.includes(query)) {
                tr.classList.add("bg-neutral-100");
            } else {
                tr.classList.remove("bg-neutral-100");
            }
        });
    }

    // ==========================================
    // DETAIL OVERLAY DIALOGS (Modals)
    // ==========================================
    function openModal(panelId) {
        activeOpenModal = panelId;
        activeDetailAgentId = null;
        const overlay = document.getElementById("modal-overlay");
        overlay.classList.remove("hidden");
        renderModalContent(panelId);
    }

    function renderModalContent(panelId, isRefresh = false) {
        const title = document.getElementById("modal-title");
        const body = document.getElementById("modal-body");
        
        if (panelId === 'memory-redis') {
            title.textContent = "DIALOG // P03_WORKING_MEMORY_STORAGE (REDIS)";
            if (!isRefresh) {
                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">1. Working Memory (Redis) Cache inspector</h5>
                        <p class="text-neutral-400 text-[10px]">Redis acts as our real-time working memory keyspace for storing active plans and anomalies with strict TTL expiries.</p>
                        <div class="text-center py-6 text-neutral-500 font-mono text-[9px]">&gt; Loading keyspace from memory API...</div>
                    </div>
                `;
            }
            fetch('/api/memory/working/keys')
                .then(res => res.json())
                .then(keys => {
                    let rowsHtml = '';
                    if (!keys || keys.length === 0) {
                        rowsHtml = `
                            <tr>
                                <td colspan="4" class="py-4 text-center text-neutral-500 font-mono text-[9px]">&gt; Keyspace empty. System nominal.</td>
                            </tr>
                        `;
                    } else {
                        keys.forEach((k, idx) => {
                            rowsHtml += `
                                <tr class="border-b border-[#121212] hover:bg-[#050505] font-mono text-[9px]">
                                    <td class="py-2 text-white font-bold select-all">${k.key}</td>
                                    <td class="text-cyan-400 font-bold">${k.type.toUpperCase()}</td>
                                    <td class="text-neutral-300 max-w-[200px] truncate" title="${k.value}">${k.value}</td>
                                    <td class="text-right text-yellow-500 font-bold font-mono">${k.ttl >= 0 ? k.ttl + 's' : 'no-expiry'}</td>
                                </tr>
                            `;
                        });
                    }
                    body.innerHTML = `
                        <div class="space-y-4">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">1. Working Memory (Redis) Cache inspector</h5>
                            <p class="text-neutral-400 text-[10px]">Redis acts as our real-time working memory keyspace for storing active plans and anomalies with strict TTL expiries.</p>
                            <table class="w-full text-left mt-3 font-mono text-[10px]">
                                <thead class="text-neutral-500 border-b border-[#222222]">
                                    <tr>
                                        <th class="py-1">KEY_NAME</th>
                                        <th class="py-1">TYPE</th>
                                        <th class="py-1">CONTENT</th>
                                        <th class="py-1 text-right">TTL</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${rowsHtml}
                                </tbody>
                            </table>
                        </div>
                    `;
                })
                .catch(err => {
                    body.innerHTML = `
                        <div class="space-y-4">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">1. Working Memory (Redis) Cache inspector</h5>
                            <div class="text-red-500 font-mono text-[9px]">&gt; Error loading memory: ${err}</div>
                        </div>
                    `;
                });
        } else if (panelId === 'memory-timescale') {
            title.textContent = "DIALOG // P03_EPISODIC_MEMORY_HYPERTABLE (TIMESCALEDB)";
            if (!isRefresh) {
                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">2. Episodic Storage Hypertables (Timeseries)</h5>
                        <p class="text-neutral-400 text-[10px]">Episodic memory stores every ingested CognitiveEvent inside a TimescaleDB hypertable partition to evaluate temporal baselines.</p>
                        <div class="text-center py-6 text-neutral-500 font-mono text-[9px]">&gt; Querying hypertable partition statistics...</div>
                    </div>
                `;
            }
            Promise.all([
                fetch('/api/memory/episodic/summary').then(res => res.json()).catch(() => ({})),
                fetch('/api/memory/episodic/recent?limit=10').then(res => res.json()).catch(() => [])
            ])
            .then(([summary, recent]) => {
                let statsHtml = '';
                if (!summary || Object.keys(summary).length === 0) {
                    statsHtml = '<p class="text-neutral-500 font-mono text-[9px]">&gt; Hypertable partition is empty. No metric records logged today.</p>';
                } else {
                    statsHtml = `
                        <table class="w-full text-left font-mono text-[10px]">
                            <thead class="text-neutral-500 border-b border-[#222222]">
                                <tr>
                                    <th class="py-1">EVENT_TYPE</th>
                                    <th class="py-1 text-right">OBSERVATION_COUNT</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${Object.entries(summary).map(([k, v]) => `
                                    <tr class="border-b border-[#121212] hover:bg-[#050505]">
                                        <td class="py-1.5 text-white">${k}</td>
                                        <td class="py-1.5 text-right font-bold text-cyan-400">${v}</td>
                                    </tr>
                                `).join('')}
                            </tbody>
                        </table>
                    `;
                }

                let recentHtml = '';
                if (!recent || recent.length === 0) {
                    recentHtml = '<p class="text-neutral-500 font-mono text-[9px]">&gt; No recent episodic events logged.</p>';
                } else {
                    recentHtml = `
                        <table class="w-full text-left font-mono text-[9px] mt-2">
                            <thead class="text-neutral-500 border-b border-[#222222]">
                                <tr>
                                    <th class="py-1">TIME</th>
                                    <th class="py-1">EVENT_ID</th>
                                    <th class="py-1">SOURCE</th>
                                    <th class="py-1">TYPE</th>
                                    <th class="py-1 text-right">SEV</th>
                                </tr>
                            </thead>
                            <tbody>
                                ${recent.map(e => {
                                    const timeStr = e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '—';
                                    const sevColor = e.severity === 'critical' ? 'text-red-500 font-bold' : e.severity === 'high' ? 'text-yellow-500' : 'text-neutral-400';
                                    return `
                                        <tr class="border-b border-[#121212] hover:bg-[#050505]">
                                            <td class="py-1.5 text-neutral-400">${timeStr}</td>
                                            <td class="py-1.5 text-white font-bold select-all">${e.event_id || e.id}</td>
                                            <td class="text-neutral-300">${e.source_id}</td>
                                            <td class="text-cyan-400">${e.event_type}</td>
                                            <td class="py-1.5 text-right font-bold ${sevColor}">${(e.severity || 'info').toUpperCase()}</td>
                                        </tr>
                                    `;
                                }).join('')}
                            </tbody>
                        </table>
                    `;
                }

                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">2. Episodic Storage Hypertables (Timeseries)</h5>
                        <p class="text-neutral-400 text-[10px]">Episodic memory stores every ingested CognitiveEvent inside a TimescaleDB hypertable partition to evaluate temporal baselines.</p>
                        <div class="border border-[#222222] p-3 mt-3 bg-[#0a0a0a] space-y-1.5 text-[10px] mb-4">
                            <p><span class="text-neutral-500">Hypertable:</span> cognitive_events (TimescaleDB partitioned)</p>
                            <p><span class="text-neutral-500">Partition range:</span> Rolling 7 days partition</p>
                            <p><span class="text-neutral-500">Ingest rate:</span> ${last_status ? last_status.throughput : '0.00'} msg/s</p>
                        </div>
                        <h6 class="text-white uppercase text-[10px] font-bold mb-2">Daily Event Frequency Histogram</h6>
                        ${statsHtml}
                        
                        <h6 class="text-white uppercase text-[10px] font-bold mt-4 mb-2">Most Recent Logged Episodes (TimescaleDB)</h6>
                        ${recentHtml}
                    </div>
                `;
            })
            .catch(err => {
                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">2. Episodic Storage Hypertables</h5>
                        <div class="text-red-500 font-mono text-[9px]">&gt; Error loading partition summary: ${err}</div>
                    </div>
                `;
            });
        } else if (panelId === 'memory-neo4j') {
            title.textContent = "DIALOG // P03_SEMANTIC_MEMORY_EXPLORER (NEO4J)";
            body.innerHTML = `
                <div class="space-y-4">
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">3. Semantic Memory Explorer (Neo4j Graph Database)</h5>
                    <p class="text-neutral-400 text-[10px]">Neo4j stores relationships between entities (Services, Databases) and actions, allowing the World Model to trace causal chains.</p>
                    <div class="grid grid-cols-3 gap-2 text-[10px] my-3">
                        <div class="border border-[#222222] bg-[#0a0a0a] p-2 text-center">
                            <span class="text-neutral-500 block text-[8px] uppercase">Node Labels</span>
                            <span class="text-white block font-bold mt-1">Entity, Task</span>
                        </div>
                        <div class="border border-[#222222] bg-[#0a0a0a] p-2 text-center">
                            <span class="text-neutral-500 block text-[8px] uppercase">Relationship Types</span>
                            <span class="text-white block font-bold mt-1">DEPENDS_ON, COMMUNICATES_WITH, RESOLVES</span>
                        </div>
                        <div class="border border-[#222222] bg-[#0a0a0a] p-2 text-center">
                            <span class="text-neutral-500 block text-[8px] uppercase">Active Plugins</span>
                            <span class="text-white block font-bold mt-1">APOC (Graph Algorithms)</span>
                        </div>
                    </div>
                    <div class="border border-[#222222] bg-[#0a0a0a] p-4 text-center">
                        <span class="text-neutral-400 text-[10px] uppercase font-bold block mb-3 text-left">Semantic Relationship Visualizer</span>
                        <div id="neo4j-d3-canvas" class="w-full h-48 bg-black relative flex items-center justify-center border border-[#1a1a1a]">
                            <!-- SVG element populated by D3 on load -->
                        </div>
                    </div>
                    <div class="mt-3">
                        <span class="text-neutral-500 block mb-1">Execute Cypher Query</span>
                        <div class="flex gap-2">
                            <input id="cypher-input" class="w-full bg-[#0a0a0a] border border-[#222222] text-white p-2 text-[10px] focus:outline-none" type="text" value="MATCH (n) RETURN n LIMIT 10"/>
                            <button onclick="runMockCypher()" class="px-4 bg-white text-black font-bold uppercase hover:bg-neutral-200 text-[10px]">EXECUTE</button>
                        </div>
                        <div id="cypher-results" class="border border-[#222222] bg-black p-3 text-[9px] mt-2 font-mono h-24 overflow-y-auto custom-scrollbar">
                            &gt; Cypher execution results will populate here.
                        </div>
                    </div>
                </div>
            `;
            setTimeout(initD3Graph, 50);
        } else if (panelId === 'memory-qdrant') {
            title.textContent = "DIALOG // P03_PROCEDURAL_PLAYBOOKS (QDRANT)";
            if (!isRefresh) {
                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">4. Procedural Memory vector collection (Qdrant)</h5>
                        <p class="text-neutral-400 text-[10px]">Procedural memory stores the playbook vector embeddings (1536-dims) to execute Cosine Similarity search when matching playbooks to anomalies.</p>
                        <div class="text-center py-6 text-neutral-500 font-mono text-[9px]">&gt; Loading vector collection indexes...</div>
                    </div>
                `;
            }
            fetch('/api/memory/procedural/playbooks')
                .then(res => res.json())
                .then(playbooks => {
                    let playbooksHtml = '';
                    if (!playbooks || playbooks.length === 0) {
                        playbooksHtml = '<p class="text-neutral-500 font-mono text-[9px]">&gt; Vector playbooks catalog empty.</p>';
                    } else {
                        playbooksHtml = `
                            <table class="w-full text-left font-mono text-[10px]">
                                <thead class="text-neutral-500 border-b border-[#222222]">
                                    <tr>
                                        <th class="py-1">PLAYBOOK_ID</th>
                                        <th class="py-1">RECOMMENDED_ACTION</th>
                                        <th class="py-1 text-right">SUCCESS_RATE</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${playbooks.map(pb => `
                                        <tr class="border-b border-[#121212] hover:bg-[#050505]">
                                            <td class="py-1.5 text-white font-bold">${pb.id}</td>
                                            <td class="text-neutral-300">${pb.recommended_action}</td>
                                            <td class="py-1.5 text-right font-bold text-green-500">${(pb.success_rate * 100).toFixed(1)}%</td>
                                        </tr>
                                    `).join('')}
                                </tbody>
                            </table>
                        `;
                    }
                    body.innerHTML = `
                        <div class="space-y-4">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">4. Procedural Memory vector collection (Qdrant)</h5>
                            <p class="text-neutral-400 text-[10px]">Procedural memory stores the playbook vector embeddings (1536-dims) to execute Cosine Similarity search when matching playbooks to anomalies.</p>
                            <div class="grid grid-cols-2 gap-4 mt-3 mb-4">
                                <div class="border border-[#222222] p-2 bg-[#050505] text-[9px]">
                                    <span class="text-neutral-500 block uppercase">Collection dimension size:</span> 1536 (Ada-002 compatible)
                                </div>
                                <div class="border border-[#222222] p-2 bg-[#050505] text-[9px]">
                                    <span class="text-neutral-500 block uppercase">Indexing vector metric:</span> Cosine Similarity
                                </div>
                            </div>
                            <h6 class="text-white uppercase text-[10px] font-bold mb-2">Active Playbook Catalog (Procedural Memory)</h6>
                            ${playbooksHtml}
                        </div>
                    `;
                })
                .catch(err => {
                    body.innerHTML = `
                        <div class="space-y-4">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">4. Procedural Memory vector collection</h5>
                            <div class="text-red-500 font-mono text-[9px]">&gt; Error loading playbooks: ${err}</div>
                        </div>
                    `;
                });
        } else if (panelId === 'reasoning') {
            title.textContent = "DIALOG // P05_REASONING_CORE_PROMPT_TRACE";
            body.innerHTML = `
                <div class="space-y-4 font-mono text-[10px]">
                    <div>
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Situation Summary & Assessment</h5>
                        <div class="border border-[#222222] bg-[#0a0a0a] p-3 text-[9px] mt-2 font-mono h-24 overflow-y-auto custom-scrollbar whitespace-pre-wrap select-all">
${DECISION_LOG.situation_assessment || 'No active anomalies detected.'}
                        </div>
                    </div>
                    <div>
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">LLM Reasoning Trace (Step-by-Step Thought Process)</h5>
                        <div class="border border-[#222222] bg-[#0a0a0a] p-3 text-[9px] mt-2 font-mono leading-relaxed h-44 overflow-y-auto custom-scrollbar whitespace-pre-wrap select-all text-cyan-400">
${DECISION_LOG.reasoning_trace || 'No active LLM reasoning trace logged.'}
                        </div>
                    </div>
                    <div>
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Raw JSON response received</h5>
                        <div class="border border-[#222222] bg-black p-3 text-[9px] mt-2 font-mono h-32 overflow-y-auto custom-scrollbar select-all">
                            <pre>${JSON.stringify(DECISION_LOG, null, 2)}</pre>
                        </div>
                    </div>
                </div>
            `;
        } else if (panelId === 'perception') {
            title.textContent = "DIALOG // P02_PERCEPTION_ADAPTER_CONFIGURATION";
            body.innerHTML = `
                <div class="space-y-4">
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Detailed Adapter List & Operational Metrics</h5>
                    <table class="w-full text-left mt-3 font-mono text-[9px]">
                        <thead class="text-neutral-500 border-b border-[#222222]">
                            <tr>
                                <th class="py-1">ADAPTER TYPE</th>
                                <th class="py-1">MONITORED SOURCE</th>
                                <th class="py-1 text-center">STATUS</th>
                                <th class="py-1">RATE</th>
                                <th class="py-1">LAG</th>
                                <th class="py-1">LATENCY</th>
                                <th class="py-1">VALIDATION SCHEMA</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${perceptionAdapters.map(ad => {
                                const isUp = ad.status === "ACTIVE" || ad.status === "UP";
                                const statusColor = isUp ? "text-green-500" : "text-red-500";
                                return `
                                    <tr class="border-b border-[#121212] hover:bg-[#050505]">
                                        <td class="py-2 text-white font-bold">${ad.type}</td>
                                        <td>${ad.source}</td>
                                        <td class="${statusColor} text-center font-bold">[ ${ad.status} ]</td>
                                        <td>${ad.rate}</td>
                                        <td class="font-bold text-white">${ad.lag}</td>
                                        <td class="font-bold text-white">${ad.latency}</td>
                                        <td class="text-neutral-400">${ad.schema}</td>
                                    </tr>
                                `;
                            }).join('')}
                        </tbody>
                    </table>
                </div>
            `;
        } else if (panelId === 'kafka') {
            title.textContent = "DIALOG // P01_KAFKA_PARTITIONS_LAG";
            const qa = perceptionAdapters.find(a => a.type === "QueueAdapter") || {status: "UNKNOWN", lag: "0 msgs", latency: "--"};
            const statusColor = qa.status === "ACTIVE" || qa.status === "UP" ? "text-green-500" : "text-red-500";
            body.innerHTML = `
                <div class="space-y-4">
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Consumer Groups lag tracking</h5>
                    <p class="text-neutral-400 text-[10px]">Real-time Kafka offset lag tracking across active topics and consumer groups.</p>
                    <table class="w-full text-left mt-3 font-mono text-[10px]">
                        <thead class="text-neutral-500 border-b border-[#222222]">
                            <tr>
                                <th class="py-1">TOPIC / CONSUMER GROUP</th>
                                <th class="py-1">STATUS</th>
                                <th class="py-1">LATENCY</th>
                                <th class="py-1 text-right">LAG</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr class="border-b border-[#121212]">
                                <td class="py-2 text-white font-bold">order-events (order-processor)</td>
                                <td class="${statusColor}">[ ${qa.status} ]</td>
                                <td class="text-neutral-300 font-mono">${qa.latency}</td>
                                <td class="text-right text-yellow-600 font-bold font-mono">${qa.lag}</td>
                            </tr>
                            <tr class="border-b border-[#121212]">
                                <td class="py-2 text-white font-bold">cognitive.events (world-model)</td>
                                <td class="text-green-500">[ ACTIVE ]</td>
                                <td class="text-neutral-300 font-mono">1ms</td>
                                <td class="text-right text-green-500">0 msgs</td>
                            </tr>
                        </tbody>
                    </table>
                </div>
            `;
        } else if (panelId === 'world') {
            title.textContent = "DIALOG // P04_WORLD_MODEL_CAUSAL_TRACE";
            if (WORLD_ANOMALIES.length === 0) {
                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold text-left">Active Anomaly Causal chains</h5>
                        <div class="border border-[#222222] bg-black p-6 flex flex-col items-center justify-center gap-4 mt-3 min-h-[150px]">
                            <p class="text-neutral-500 font-mono text-[10px]">&gt; No active anomalies tracked. System state is nominal.</p>
                        </div>
                    </div>
                `;
            } else {
                let chainsHtml = WORLD_ANOMALIES.map(anom => {
                    const sevColor = anom.severity === "CRITICAL" ? "text-red-500 font-bold" : "text-yellow-500 font-bold";
                    return `
                        <div class="border border-[#222222] p-3 bg-[#0a0a0a] mb-3">
                            <div class="flex justify-between items-center mb-2">
                                <p class="font-bold text-white font-mono text-[10px]">${anom.id}: ${anom.type} on ${anom.entity}</p>
                                <span class="${sevColor} font-mono text-[8px]">[ ${anom.severity} ]</span>
                            </div>
                            <p class="text-[9px] text-neutral-400">Causal Chain propagation:</p>
                            <div class="border-l-2 border-red-500 pl-3 mt-1.5 text-neutral-300 font-mono text-[9px]">
                                ${anom.chain || `${anom.entity} ➔ ${anom.type}`}
                            </div>
                        </div>
                    `;
                }).join('');
                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold text-left">Active Anomaly Causal chains</h5>
                        <div class="mt-3 overflow-y-auto max-h-[350px] custom-scrollbar">
                            ${chainsHtml}
                        </div>
                    </div>
                `;
            }
        } else if (panelId === 'planning') {
            title.textContent = "DIALOG // P07_PLANNING_DAG_GRAPH";
            if (!isRefresh) {
                body.innerHTML = `
                    <div class="space-y-4 text-center py-6">
                        <div class="text-center py-6 text-neutral-500 font-mono text-[9px]">&gt; Loading active planning DAGs...</div>
                    </div>
                `;
            }
            fetch('/api/plans')
                .then(res => res.json())
                .then(plans => {
                    PLAN_STEPS.length = 0;
                    plans.forEach(activePlan => {
                        if (activePlan.steps) {
                            activePlan.steps.forEach((s, idx) => {
                                PLAN_STEPS.push({
                                    planId: activePlan.plan_id,
                                    goal: activePlan.goal,
                                    step: idx + 1,
                                    name: s.description || s.action,
                                    status: s.status
                                });
                            });
                        }
                    });
                    
                    if (PLAN_STEPS.length === 0) {
                        body.innerHTML = `
                            <div class="space-y-4 text-center py-6">
                                <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold text-left">Active plan step dependencies</h5>
                                <div class="border border-[#222222] bg-black p-6 flex flex-col items-center justify-center gap-4 mt-3 min-h-[200px]">
                                    <p class="text-neutral-500 font-mono text-[10px]">&gt; No active plans are currently running or pending approval.</p>
                                    <p class="text-green-500 font-mono text-[9px] uppercase font-bold">[ SYSTEM NOMINAL ]</p>
                                </div>
                            </div>
                        `;
                    } else {
                        let stepsHtml = PLAN_STEPS.map((p, idx) => {
                            const statusStr = (p.status || 'pending').toUpperCase();
                            let borderClass = "border-[#222222] text-neutral-400";
                            let blinkClass = "";
                            if (p.status === "succeeded" || p.status === "COMPLETED") {
                                borderClass = "border-green-500 text-green-500";
                            } else if (p.status === "running" || p.status === "RUNNING") {
                                borderClass = "border-white text-white font-bold";
                                blinkClass = "blink-status";
                            } else if (p.status === "waiting_approval" || p.status === "awaiting_approval") {
                                borderClass = "border-yellow-500 text-yellow-500 font-bold";
                            }
                            const arrow = idx < PLAN_STEPS.length - 1 ? `
                                <span class="material-symbols-outlined text-neutral-600 text-[14px]">arrow_downward</span>
                            ` : '';
                            return `
                                <div class="border ${borderClass} p-3 bg-[#0a0a0a] w-full max-w-md mx-auto text-left flex justify-between items-center ${blinkClass}">
                                    <div>
                                        <span class="text-[9px] text-neutral-500 font-bold block uppercase">Step ${p.step}: ${p.planId ? p.planId.substring(0, 8) : ''}</span>
                                        <span class="text-[10px] font-mono">${p.name}</span>
                                    </div>
                                    <span class="text-[9px] font-bold">[ ${statusStr} ]</span>
                                </div>
                                ${arrow}
                            `;
                        }).join('');
                        body.innerHTML = `
                            <div class="space-y-4 text-center">
                                <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold text-left">Active plan step dependencies</h5>
                                <p class="text-neutral-400 text-[9px] text-left">Goal: ${PLAN_STEPS[0].goal}</p>
                                <div class="border border-[#222222] bg-black p-6 flex flex-col items-center justify-center gap-2 mt-3 min-h-[200px] overflow-y-auto max-h-[400px] custom-scrollbar">
                                    ${stepsHtml}
                                </div>
                            </div>
                        `;
                    }
                })
                .catch(err => {
                    body.innerHTML = `
                        <div class="space-y-4 text-center py-6">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold text-left">Active plan step dependencies</h5>
                            <div class="text-red-500 font-mono text-[9px]">&gt; Error loading plans: ${err}</div>
                        </div>
                    `;
                });
        } else if (panelId === 'feedback') {
            title.textContent = "DIALOG // P09_FEEDBACK_OUTCOMES_AUDIT";
            if (!isRefresh) {
                body.innerHTML = `
                    <div class="space-y-4">
                        <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Recent closed-loop evaluations</h5>
                        <p class="text-neutral-400 text-[10px]">Feedback loop tracks completed remediation plans and updates playbook success metrics dynamically.</p>
                        <div class="text-center py-6 text-neutral-500 font-mono text-[9px]">&gt; Querying feedback loop audit records...</div>
                    </div>
                `;
            }
            fetch('/api/feedback/recent')
                .then(res => res.json())
                .then(data => {
                    let rowsHtml = '';
                    if (!data.action_rates || data.action_rates.length === 0) {
                        rowsHtml = `
                            <tr>
                                <td colspan="4" class="py-4 text-center text-neutral-500 font-mono text-[9px]">&gt; No closed-loop evaluations processed yet. System nominal.</td>
                            </tr>
                        `;
                    } else {
                        data.action_rates.forEach((ar, idx) => {
                            const rate = (ar.success_rate * 100).toFixed(1);
                            rowsHtml += `
                                <tr class="border-b border-[#121212] hover:bg-[#050505]">
                                    <td class="py-2 text-white font-bold">${ar.action}</td>
                                    <td class="text-green-500 font-bold">${ar.success_count} wins</td>
                                    <td class="text-red-500 font-bold">${ar.failure_count} losses</td>
                                    <td class="text-right text-yellow-500 font-bold">${rate}%</td>
                                </tr>
                            `;
                        });
                    }
                    body.innerHTML = `
                        <div class="space-y-4">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Recent closed-loop evaluations</h5>
                            <p class="text-neutral-400 text-[10px]">Feedback loop tracks completed remediation plans and updates playbook success metrics dynamically.</p>
                            <div class="grid grid-cols-3 gap-2 text-center text-[10px] my-3">
                                <div class="border border-[#222222] bg-[#0a0a0a] p-2">
                                    <span class="text-neutral-500 block text-[8px] uppercase">Total Processed</span>
                                    <span class="text-white block font-bold mt-1">${data.total_processed || 0}</span>
                                </div>
                                <div class="border border-[#222222] bg-[#0a0a0a] p-2">
                                    <span class="text-neutral-500 block text-[8px] uppercase">Successes</span>
                                    <span class="text-green-500 block font-bold mt-1">${data.successes || 0}</span>
                                </div>
                                <div class="border border-[#222222] bg-[#0a0a0a] p-2">
                                    <span class="text-neutral-500 block text-[8px] uppercase">Failures</span>
                                    <span class="text-red-500 block font-bold mt-1">${data.failures || 0}</span>
                                </div>
                            </div>
                            <table class="w-full text-left mt-3 font-mono text-[10px]">
                                <thead class="text-neutral-500 border-b border-[#222222]">
                                    <tr>
                                        <th class="py-1">ACTION</th>
                                        <th class="py-1">WINS</th>
                                        <th class="py-1">LOSSES</th>
                                        <th class="py-1 text-right">SUCCESS RATE</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${rowsHtml}
                                </tbody>
                            </table>
                        </div>
                    `;
                })
                .catch(err => {
                    body.innerHTML = `
                        <div class="space-y-4">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Recent closed-loop evaluations</h5>
                            <div class="text-red-500 font-mono text-[9px]">&gt; Error loading feedback: ${err}</div>
                        </div>
                    `;
                });
        } else if (panelId === 'temporal') {
            title.textContent = "DIALOG // P08_TEMPORAL_PREDICTIONS_ENGINE";
            if (!isRefresh) {
                body.innerHTML = `
                    <div class="space-y-4">
                        <div class="text-center py-6 text-neutral-500 font-mono text-[9px]">&gt; Querying temporal forecasting engine...</div>
                    </div>
                `;
            }
            fetch('/api/anomalies')
                .then(res => res.json())
                .then(anoms => {
                    let anomaliesHtml = anoms.map(anom => {
                        let currentVal = anom.details?.value || "ELEVATED";
                        let t5Val = "STABLE", t15Val = "STABLE", t60Val = "STABLE";
                        if (anom.predictions) {
                            const p = anom.predictions;
                            if (p.t5) t5Val = `${p.t5.value} (Conf: ${p.t5.confidence})`;
                            if (p.t15) t15Val = `${p.t15.value} (Conf: ${p.t15.confidence})`;
                            if (p.t60) t60Val = `${p.t60.value} (Conf: ${p.t60.confidence})`;
                        }
                        const anomId = anom.anomaly_id || anom.id || 'unknown';
                        const severity = anom.severity || 'HIGH';
                        const eventType = anom.event_type || anom.type || 'unknown_anomaly';
                        const entityId = anom.entity_id || anom.entity || 'unknown_entity';
                        return `
                            <div class="border border-[#222222] bg-[#050505] p-3 space-y-3 mb-3">
                                <div class="flex justify-between items-center border-b border-[#181818] pb-1.5">
                                    <span class="text-white font-bold font-mono text-[10px] uppercase">${anomId} // ${eventType} on ${entityId}</span>
                                    <span class="text-red-500 font-bold font-mono text-[8px]">[ ${severity} ]</span>
                                </div>
                                <div class="grid grid-cols-4 gap-2 text-center text-[9px] font-mono">
                                    <div class="bg-[#111] p-1.5 border border-[#222]">
                                        <span class="text-neutral-500 block text-[7px] uppercase font-bold">CURRENT</span>
                                        <span class="text-white font-bold block mt-0.5">${currentVal}</span>
                                    </div>
                                    <div class="bg-[#111] p-1.5 border border-[#222]">
                                        <span class="text-neutral-500 block text-[7px] uppercase font-bold">T+5 FORECAST</span>
                                        <span class="text-yellow-500 font-bold block mt-0.5">${t5Val}</span>
                                    </div>
                                    <div class="bg-[#111] p-1.5 border border-[#222]">
                                        <span class="text-neutral-500 block text-[7px] uppercase font-bold">T+15 FORECAST</span>
                                        <span class="text-red-500 font-bold block mt-0.5">${t15Val}</span>
                                    </div>
                                    <div class="bg-[#111] p-1.5 border border-[#222]">
                                        <span class="text-neutral-500 block text-[7px] uppercase font-bold">T+60 FORECAST</span>
                                        <span class="text-neutral-500 font-bold block mt-0.5">${t60Val}</span>
                                    </div>
                                </div>
                            </div>
                        `;
                    }).join('');
                    
                    body.innerHTML = `
                        <div class="space-y-4">
                            <div class="border border-[#222222] bg-black p-3 text-[9px] text-neutral-400 font-mono space-y-1.5 leading-relaxed">
                                <p class="text-white font-bold uppercase">&gt;&gt; FORECASTING METHODOLOGY & MODELS:</p>
                                <p>• <span class="text-white">Algorithm:</span> Holt-Winters Exponential Smoothing (statsmodels).</p>
                                <p>• <span class="text-white">Features:</span> Fits trend, residual confidence interval (±1.5σ), and forecasts for horizons T+5, T+15, and T+60 minutes.</p>
                                <p>• <span class="text-white">Prediction Agent:</span> Updates predictions dynamically using live metric observations from episodic memory.</p>
                            </div>
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[11px] font-bold">Active Anomaly Predictions</h5>
                            <div class="overflow-y-auto max-h-[300px] custom-scrollbar">
                                ${anomaliesHtml || '<p class="text-neutral-500 font-mono text-[9px]">&gt; No active anomalies loaded.</p>'}
                            </div>
                        </div>
                    `;
                })
                .catch(err => {
                    body.innerHTML = `
                        <div class="space-y-4">
                            <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[11px] font-bold">Active Anomaly Predictions</h5>
                            <div class="text-red-500 font-mono text-[9px]">&gt; Error loading temporal predictions: ${err}</div>
                        </div>
                    `;
                });
        } else if (panelId === 'docs') {
            title.textContent = "DIALOG // API_DOCUMENTATION_V12";
            body.innerHTML = `
                <div class="space-y-4 text-left">
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">FastAPI Subservices routes</h5>
                    <ul class="space-y-2 text-neutral-400">
                        <li><span class="text-white font-bold">Perception (Port 8080):</span> POST /perception/prometheus-alerts</li>
                        <li><span class="text-white font-bold">Memory Core (Port 8090):</span> GET /memory/working/recent | GET /memory/graph/neighbors/{id}</li>
                        <li><span class="text-white font-bold">World Model (Port 8092):</span> GET /world/situation | GET /world/anomalies</li>
                        <li><span class="text-white font-bold">Reasoning (Port 8093):</span> POST /reasoning/reason</li>
                        <li><span class="text-white font-bold">Planning (Port 8094):</span> POST /planning/plans/{id}/approve</li>
                    </ul>
                </div>
            `;
        } else if (panelId === 'architecture') {
            title.textContent = "DIALOG // SYSTEM_COGNITIVE_ARCHITECTURE_BLUEPRINT";
            body.innerHTML = `
                <div class="space-y-6 max-w-full text-left font-mono">
                    <div class="flex justify-between items-center border-b border-[#222222] pb-2">
                        <h5 class="text-white uppercase text-[11px] font-bold">DIGITAL COGNITIVE ARCHITECTURE BLUEPRINT (INTERACTIVE)</h5>
                        <span class="text-neutral-500 text-[8px]">[ MODE: CLIENT-SIDE RENDERING ]</span>
                    </div>
                    
                    <p class="text-neutral-400 text-[9px] leading-relaxed">
                        Interactive hybrid HTML/SVG blueprint visual mapping of information flow across the 8 layers, showing system routing branches and agent operational scopes. Click on active agent nodes to view operational checklists.
                    </p>

                    <!-- HTML/SVG Flowchart Map -->
                    <div class="border border-[#222222] bg-[#030303] p-6 overflow-x-auto custom-scrollbar relative pl-16">
                        <div class="min-w-[800px] space-y-4 relative text-[9px] py-4">
                            
                            <!-- Layer 1 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    01 // PERCEPTION
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">API:8080 (Ingestion)</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ 9 Adapters ]</span>
                                    <div class="grid grid-cols-3 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1 border border-[#222]">Log Ingest (svc:user-service)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">Metrics Ingest (metric:prometheus)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">API Health (svc:api-gateway)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">Database Status (db:shopcore-postgres)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">Broker Queues (queue:order-events)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">File Monitor (file:nginx-config)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">Sensor Webhook (sensor:server-room-temp)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">User Events (usr:user-behavior)</div>
                                        <div class="bg-[#111] p-1 border border-[#222]">Agent Events (agent:coordinator-logs)</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0">
                                    <div class="text-[8px] text-green-400 border border-green-800 bg-[#061c0c] p-2 font-bold uppercase cursor-pointer hover:bg-green-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:monitor-global');">
                                        A-01 Monitor Agent
                                    </div>
                                </div>
                            </div>
                            
                            <!-- Arrow Down -->
                            <div class="flex items-center gap-4 pl-16">
                                <div class="w-36 text-center select-none text-neutral-600 font-bold">↓</div>
                            </div>

                            <!-- Layer 2 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    02 // EVENT BUS
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">KAFKA:9092</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ Kafka Topics ]</span>
                                    <div class="grid grid-cols-3 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1.5 border border-[#222] text-cyan-400 font-bold">cognitive.events</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222] text-purple-400 font-bold">actions</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222] text-white">agent_events</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0">
                                    <div class="text-[8px] text-green-400 border border-green-800 bg-[#061c0c] p-2 font-bold uppercase cursor-pointer hover:bg-green-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:monitor-global');">
                                        A-01 Monitor Agent
                                    </div>
                                </div>
                            </div>

                            <!-- Arrow Down -->
                            <div class="flex items-center gap-4 pl-16">
                                <div class="w-36 text-center select-none text-neutral-600 font-bold">↓</div>
                            </div>

                            <!-- Layer 3 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    03 // MEMORY
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">API:8090</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ Memory Subsystems ]</span>
                                    <div class="grid grid-cols-4 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1.5 border border-[#222] text-white"><span class="font-bold block text-[7px] text-cyan-400">Redis</span>Working Cache</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222] text-white"><span class="font-bold block text-[7px] text-purple-400">TimescaleDB</span>Episodic Logs</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222] text-white"><span class="font-bold block text-[7px] text-yellow-400">Neo4j</span>Semantic Graph</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222] text-white"><span class="font-bold block text-[7px] text-blue-400">Qdrant</span>Procedural Vectors</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0 font-bold">
                                    <div class="text-[8px] text-blue-400 border border-blue-800 bg-[#0b1b36] p-2 font-bold uppercase cursor-pointer hover:bg-blue-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:memory');">
                                        agent:memory (A-08)
                                    </div>
                                </div>
                            </div>

                            <!-- Arrow Down -->
                            <div class="flex items-center gap-4 pl-16">
                                <div class="w-36 text-center select-none text-neutral-600 font-bold">↓</div>
                            </div>

                            <!-- Layer 4 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    04 // WORLD MODEL
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">API:8092</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ Belief Registry ]</span>
                                    <div class="grid grid-cols-3 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1.5 border border-[#222]">System Causal Topology</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Active Incidents List</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Blast Radii Calculations</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0">
                                    <div class="text-[8px] text-yellow-400 border border-yellow-800 bg-[#1f1b05] p-2 font-bold uppercase cursor-pointer hover:bg-yellow-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:prediction');">
                                        agent:prediction (A-09)
                                    </div>
                                </div>
                            </div>

                            <!-- Arrow Down -->
                            <div class="flex items-center gap-4 pl-16">
                                <div class="w-36 text-center select-none text-neutral-600 font-bold">↓</div>
                            </div>

                            <!-- Layer 5 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    05 // REASONING
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">API:8093</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ Reasoning Chains ]</span>
                                    <div class="grid grid-cols-3 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Fast Heuristics</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Deep LLM Prompts</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Root Cause Analysis</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0 font-bold">
                                    <div class="text-[8px] text-yellow-400 border border-yellow-800 bg-[#1f1b05] p-2 font-bold uppercase cursor-pointer hover:bg-yellow-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:planner-01');">
                                        Planner Pool
                                    </div>
                                </div>
                            </div>

                            <!-- Arrow Down -->
                            <div class="flex items-center gap-4 pl-16">
                                <div class="w-36 text-center select-none text-neutral-600 font-bold">↓</div>
                            </div>

                            <!-- Layer 6 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    06 // PLANNING
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">API:8094</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ Plan Execution Compiler ]</span>
                                    <div class="grid grid-cols-3 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Remediation DAG</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Human Approval Gate</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Redis Lock Reservator</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0 font-bold">
                                    <div class="text-[8px] text-yellow-400 border border-yellow-800 bg-[#1f1b05] p-2 font-bold uppercase cursor-pointer hover:bg-yellow-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:planner-01');">
                                        Planner Pool
                                    </div>
                                </div>
                            </div>

                            <!-- Arrow Down -->
                            <div class="flex items-center gap-4 pl-16">
                                <div class="w-36 text-center select-none text-neutral-600 font-bold">↓</div>
                            </div>

                            <!-- Layer 7 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    07 // EXECUTION
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">API:8095</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ Mitigation Runners ]</span>
                                    <div class="grid grid-cols-3 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Container/SSH APIs</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Service Scaler</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Database Failover</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0 font-bold">
                                    <div class="text-[8px] text-purple-400 border border-purple-800 bg-[#1a0e2d] p-2 font-bold uppercase cursor-pointer hover:bg-purple-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:executor-01');">
                                        Executor Pool
                                    </div>
                                </div>
                            </div>

                            <!-- Arrow Down -->
                            <div class="flex items-center gap-4 pl-16">
                                <div class="w-36 text-center select-none text-neutral-600 font-bold">↓</div>
                            </div>

                            <!-- Layer 8 -->
                            <div class="flex items-center gap-4">
                                <div class="w-36 border border-[#333] bg-[#111] p-2 text-white font-bold text-center shrink-0">
                                    08 // FEEDBACK LOOP
                                    <span class="text-neutral-500 block text-[7px] font-normal mt-0.5">API:8096</span>
                                </div>
                                <div class="text-neutral-400 shrink-0 select-none">➔</div>
                                <div class="flex-1 border border-neutral-800 p-2 text-neutral-400 bg-[#080808] rounded">
                                    <span class="text-white font-bold block mb-1 uppercase text-[8px]">[ Causal Optimization ]</span>
                                    <div class="grid grid-cols-3 gap-1.5 text-[8px]">
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Pre/Post Auditing</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Neo4j Weight mutation</div>
                                        <div class="bg-[#111] p-1.5 border border-[#222]">Qdrant Playbook tuning</div>
                                    </div>
                                </div>
                                <div class="w-36 text-center shrink-0">
                                    <div class="text-[8px] text-blue-400 border border-blue-800 bg-[#0b1b36] p-2 font-bold uppercase cursor-pointer hover:bg-blue-950 transition-colors" onclick="closeModal(); openAgentDetailModal('agent:memory');">
                                        agent:memory (A-08)
                                    </div>
                                </div>
                            </div>

                            <!-- Causal back-arrow feedback loop path -->
                            <div class="absolute left-2 top-[100px] bottom-[25px] w-10 border-l-2 border-t-2 border-b-2 border-dashed border-red-500 opacity-70 flex items-center justify-start pointer-events-none rounded-l">
                                <span class="text-red-500 text-[8px] font-bold uppercase rotate-90 origin-left -translate-y-4 translate-x-2.5 shrink-0 whitespace-nowrap tracking-wider">Causal Feedback Loop</span>
                            </div>
                        </div>
                    </div>

                    <!-- Mermaid Flowchart detail -->
                    <div class="space-y-2">
                        <h6 class="text-white border-b border-[#222222] pb-1 uppercase text-[10px] font-bold">MERMAID COGNITIVE AGENT INTERACTION FLOW</h6>
                        <div class="border border-[#222222] bg-[#030303] p-4 overflow-x-auto custom-scrollbar flex justify-center">
                            <div class="mermaid text-white min-w-[700px] text-center" id="mermaid-canvas">
flowchart TD
    classDef layer fill:#111,stroke:#333,stroke-width:1px,color:#aaa,font-size:8px;
    classDef agent fill:#0a0a0a,stroke:#22c55e,stroke-width:1.5px,color:#22c55e,font-size:9px,font-weight:bold;
    classDef highlight fill:#1f1b05,stroke:#fbbf24,stroke-width:1.5px,color:#fbbf24,font-size:9px;
    
    subgraph Layers [Digital Cognitive Architecture 8-Phase Loop]
        direction LR
        L1[01 Perception]:::layer --> L2[02 Event Bus]:::layer
        L2 --> L3[03 Memory]:::layer
        L3 --> L4[04 World Model]:::layer
        L4 --> L5[05 Reasoning]:::layer
        L5 --> L6[06 Planning]:::layer
        L6 --> L7[07 Execution]:::layer
        L7 --> L8[08 Feedback]:::layer
    end

    subgraph DBs [Memory Subsystems]
        Redis[(Redis: Working Memory)]:::layer
        Timescale[(TimescaleDB: Episodic)]:::layer
        Neo4j[(Neo4j: Semantic Graph)]:::layer
        Qdrant[(Qdrant: Procedural vectors)]:::layer
    end
    L3 -.=> Redis & Timescale & Neo4j & Qdrant

    Mon[A-01 Monitor Agent]:::agent
    Plan[Planner Agents Pool]:::agent
    Exec[Executor Agents Pool]:::agent
    Mem[A-08 Memory Agent]:::agent
    Pred[A-09 Prediction Agent]:::agent

    L1 -->|Streams alerts| Mon
    Mon -->|Publishes Events| L2
    Mon -->|Updates Incident Beliefs| L4
    
    L4 -->|Active Anomalies| Plan
    Plan -->|Heuristic Queries| L5
    Plan -->|Fetches causal topology| Neo4j
    Plan -->|Compiles DAG plan| L6
    
    L6 -->|Approved Plans| Exec
    Exec -->|Redis mutex locks| Redis
    Exec -->|Dispatches mitigation| L7
    Exec -->|Emits execution trace| L2
    
    L2 -->|Audits outcomes| Mem
    Mem -->|Logs episodes| Timescale
    Mem -->|Calculates reward| L8
    L8 -->|Mutates Cypher weights| Neo4j
    L8 -->|Updates Playbooks| Qdrant
    
    L4 -->|Monitors timeseries| Pred
    Pred -->|Forecasts metrics/trends| L4
    
    L8 -.->|Causal reinforcement| L3

    click Mon call openAgentFromMermaid()
    click Plan call openAgentFromMermaid()
    click Exec call openAgentFromMermaid()
    click Mem call openAgentFromMermaid()
    click Pred call openAgentFromMermaid()
                            </div>
                        </div>
                    </div>
                </div>
            `;

            // Define global helper for Mermaid clicks
            window.openAgentFromMermaid = function(nodeId) {
                closeModal();
                let agentId = 'agent:monitor-global';
                if (nodeId === 'Plan') agentId = 'agent:planner-01';
                else if (nodeId === 'Exec') agentId = 'agent:executor-01';
                else if (nodeId === 'Mem') agentId = 'agent:memory';
                else if (nodeId === 'Pred') agentId = 'agent:prediction';
                openAgentDetailModal(agentId);
            };

            // Initialize Mermaid on-demand
            setTimeout(() => {
                try {
                    mermaid.initialize({
                        startOnLoad: false,
                        securityLevel: 'loose',
                        theme: 'dark',
                        themeVariables: {
                            background: '#030303',
                            primaryColor: '#111111',
                            primaryTextColor: '#ffffff',
                            lineColor: '#555555'
                        }
                    });
                    mermaid.init(undefined, document.getElementById('mermaid-canvas'));
                } catch (e) {
                    console.error("Mermaid initialization error:", e);
                }
            }, 50);
        } else if (panelId === 'terminal') {
            title.textContent = "DIALOG // RUNTIME_INTEGRATED_SHELL";
            body.innerHTML = `
                <div class="space-y-3">
                    <div class="bg-black border border-[#222222] p-4 text-[10px] font-mono h-48 overflow-y-auto custom-scrollbar text-white space-y-1">
                        <p class="text-neutral-500">COG_ARCH OS v12.0.0 Integrated shell.</p>
                        <p>&gt; sysctl -a | grep agent</p>
                        <p class="text-neutral-400">agent_fleet_status=unknown until CONNECT</p>
                        <p>&gt; curl -s http://localhost:8080/health</p>
                        <p class="text-neutral-400">{"status":"offline_until_connected"}</p>
                        <p>&gt; <span class="blink-status">_</span></p>
                    </div>
                </div>
            `;
        }
    }

    // Modal Neo4j Cypher execute simulator / link
    function runMockCypher() {
        const input = document.getElementById("cypher-input").value;
        const res = document.getElementById("cypher-results");
        res.innerHTML = `&gt; Running query: "${input}"...`;
        
        fetch("/api/graph/query", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ cypher: input })
        })
        .then(r => r.json())
        .then(data => {
            res.innerHTML = `&gt; Query completed.\n` + JSON.stringify(data.data || data, null, 2);
        })
        .catch(err => {
            res.innerHTML = `&gt; Error executing query: ` + err;
        });
    }

    const agentDetails = {
        "agent:monitor-global": {
            what: "Orchestrates fleet operations by monitoring environmental anomalies and delegating tasks to available planners.",
            where: "Runs inside the cognitive perception orchestrator on local thread pool, communicating with Kafka (port 9092) and World Model (port 8092).",
            how: "Queries World Model APIs (8092) for registered incidents, processes alert severities, and publishes task signals to Kafka 'cognitive.events'."
        },
        "agent:planner-01": {
            what: "Evaluates anomaly contexts, queries historical memories, and compiles active mitigation DAG plans.",
            where: "Executed as part of the Planning service pool (port 8094) querying Redis (port 6379) and Qdrant (port 6333).",
            how: "Consumes Kafka task alerts, queries Qdrant (8090) for similar playbooks, prompts the Reasoning Engine (8093) LLM, and registers compiled plan trees in Redis."
        },
        "agent:planner-02": {
            what: "Evaluates anomaly contexts, queries historical memories, and compiles active mitigation DAG plans.",
            where: "Executed as part of the Planning service pool (port 8094) querying Redis (port 6379) and Qdrant (port 6333).",
            how: "Consumes Kafka task alerts, queries Qdrant (8090) for similar playbooks, prompts the Reasoning Engine (8093) LLM, and registers compiled plan trees in Redis."
        },
        "agent:planner-03": {
            what: "Evaluates anomaly contexts, queries historical memories, and compiles active mitigation DAG plans.",
            where: "Executed as part of the Planning service pool (port 8094) querying Redis (port 6379) and Qdrant (port 6333).",
            how: "Consumes Kafka task alerts, queries Qdrant (8090) for similar playbooks, prompts the Reasoning Engine (8093) LLM, and registers compiled plan trees in Redis."
        },
        "agent:executor-01": {
            what: "Polls active mitigation steps, dispatches scripts to target servers, and reports action outcomes.",
            where: "Runs inside the local execution runner pool (port 8095), invoking scripts on target service containers (e.g. ShopCore).",
            how: "Fetches ready actions from World Model tables, invokes Execution API (8095) command sequences, and posts status telemetry to Kafka."
        },
        "agent:executor-02": {
            what: "Polls active mitigation steps, dispatches scripts to target servers, and reports action outcomes.",
            where: "Runs inside the local execution runner pool (port 8095), invoking scripts on target service containers (e.g. ShopCore).",
            how: "Fetches ready actions from World Model tables, invokes Execution API (8095) command sequences, and posts status telemetry to Kafka."
        },
        "agent:executor-03": {
            what: "Polls active mitigation steps, dispatches scripts to target servers, and reports action outcomes.",
            where: "Runs inside the local execution runner pool (port 8095), invoking scripts on target service containers (e.g. ShopCore).",
            how: "Fetches ready actions from World Model tables, invokes Execution API (8095) command sequences, and posts status telemetry to Kafka."
        },
        "agent:memory": {
            what: "Maintains semantic memory connections and manages data archiving from working memory.",
            where: "Runs inside the memory API daemon (port 8090), connecting to Redis (port 6379), TimescaleDB (port 5432), Neo4j (port 7687) and Qdrant (port 6333).",
            how: "Updates Neo4j causal links and Qdrant playbook success rates based on feedback, and migrates transient Redis keys to TimescaleDB archives."
        },
        "agent:prediction": {
            what: "Analyzes system metrics to predict future deviations and pre-emptively flags incidents.",
            where: "Runs as a standalone cron-agent (port 8091) communicating with TimescaleDB (port 5432) and the World Model API (port 8092).",
            how: "Queries TimescaleDB time-series records, calls the Temporal forecasting API (8091), and registers predicted drifts into the World Model."
        }
    };

    function getLayerChecklist(agentId) {
        const layers = [
            { num: "01", name: "Perception Ingestion", activeFor: ["monitor-global"], desc: "Filters incoming normalized metrics and alerts." },
            { num: "02", name: "Event Bus (Kafka)", activeFor: ["monitor-global", "planner-01", "planner-02", "planner-03"], desc: "Orchestrates async fleet communications and event streams." },
            { num: "03", name: "Memory Systems", activeFor: ["planner-01", "planner-02", "planner-03", "memory"], desc: "Fetches playbooks (Qdrant), queries history (Timescale/Neo4j)." },
            { num: "04", name: "World Model", activeFor: ["monitor-global", "executor-01", "executor-02", "executor-03", "prediction"], desc: "Reads/updates system beliefs, active incidents, and action registers." },
            { num: "05", name: "Reasoning Core", activeFor: ["planner-01", "planner-02", "planner-03", "prediction"], desc: "Queries core LLM prompt pipelines and parses JSON assessments." },
            { num: "06", name: "Planning DAGs", activeFor: ["planner-01", "planner-02", "planner-03", "executor-01", "executor-02", "executor-03"], desc: "Compiles plan lists, locks keys in Redis, and polls steps." },
            { num: "07", name: "Execution (Runners)", activeFor: ["executor-01", "executor-02", "executor-03"], desc: "Executes target bash commands/scripts or scales service replicas." },
            { num: "08", name: "Feedback Audits", activeFor: ["memory"], desc: "Runs reinforcement checks and updates semantic relationship weights." }
        ];

        return layers.map(l => {
            const isMatch = l.activeFor.some(role => agentId.includes(role));
            const badge = isMatch 
                ? `<span class="text-black bg-white font-bold px-1.5 py-0.5 border border-white text-[8px] uppercase font-mono shrink-0">[ ACTIVE LAYER ]</span>` 
                : `<span class="text-neutral-600 border border-[#222222] px-1.5 py-0.5 text-[8px] uppercase font-mono shrink-0 opacity-40">[ IDLE ]</span>`;
            
            const titleClass = isMatch ? 'text-white font-bold' : 'text-neutral-600 line-through';
            const descHtml = isMatch ? `<p class="text-[9px] text-neutral-300 mt-1 italic">&gt;&gt; Operation: ${l.desc}</p>` : '';
            
            return `
                <div class="border-b border-[#222222] py-2">
                    <div class="flex items-center justify-between font-mono text-[10px]">
                        <span class="${titleClass}">${l.num}_${l.name}</span>
                        ${badge}
                    </div>
                    ${descHtml}
                </div>
            `;
        }).join('');
    }

    function renderAgentDetailContent(agentId) {
        const agent = AGENTS_LIST.find(a => a.id === agentId);
        if (!agent) return;
        const title = document.getElementById("modal-title");
        const body = document.getElementById("modal-body");
        title.textContent = `DIALOG // AGENT_FLEET_CONSOLE_TRACE [ ${agent.id.toUpperCase()} ]`;

        const details = agentDetails[agent.id] || {
            what: "Generic agent execution tasks.",
            where: "Runs on local docker node container.",
            how: "Runs standard background loop and heartbeats."
        };

        let errorHtml = '';
        if (agent.status === "crashed" || agent.error || agent.stack_trace) {
            errorHtml = `
                <div class="border border-red-950 bg-red-950/20 p-3 mt-3 font-mono">
                    <h6 class="text-red-400 font-bold uppercase text-[9px] mb-1">Crashed Stack Trace / Error</h6>
                    <p class="text-red-400 text-[10px] font-bold">Error: ${agent.error || 'Unknown error'}</p>
                    <pre class="text-red-500 text-[8px] mt-2 whitespace-pre-wrap select-all leading-normal bg-black p-2 border border-red-900">${agent.stack_trace || 'No traceback captured.'}</pre>
                </div>
            `;
        }

        body.innerHTML = `
            <div class="space-y-4 text-left font-mono text-[11px]">
                <div>
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold">Agent State Details</h5>
                    <div class="grid grid-cols-2 gap-4 mt-2 text-neutral-400">
                        <p><span class="text-neutral-500">Short Label:</span> ${agent.short}</p>
                        <p><span class="text-neutral-500">Lifecycle Status:</span> <span class="text-white font-bold">[ ${agent.status} ]</span></p>
                        <p class="col-span-2"><span class="text-neutral-500">Active Task:</span> ${agent.task || 'Awaiting delegated tasks (IDLE)'}</p>
                    </div>
                    ${errorHtml}
                </div>
                
                <div>
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold mt-4">Cognitive Function Mapping</h5>
                    <p class="mt-2 text-white"><span class="text-neutral-400 font-bold uppercase block text-[9px] mb-1">What the agent is doing:</span> ${details.what}</p>
                    <p class="mt-2 text-white"><span class="text-neutral-400 font-bold uppercase block text-[9px] mb-1">Where:</span> ${details.where}</p>
                    <p class="mt-2 text-white"><span class="text-neutral-400 font-bold uppercase block text-[9px] mb-1">How it is doing it:</span> ${details.how}</p>
                </div>

                <div>
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold mt-4">Active Tasks Queue</h5>
                    <div id="agent-tasks-list" class="space-y-1.5 text-[10px] mt-2">
                        <p class="text-neutral-500 font-mono text-[9px]">&gt; Querying agent task queue...</p>
                    </div>
                </div>

                <div>
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold mt-4">Operational Layer Activity</h5>
                    <p class="text-[10px] text-neutral-400 mb-2">Checklist of which layers this agent operates on inside the 8-phase Cognitive pipeline:</p>
                    <div class="space-y-1">
                        ${getLayerChecklist(agent.id)}
                    </div>
                </div>

                <div>
                    <h5 class="text-white border-b border-[#222222] pb-1 uppercase text-[12px] font-bold mt-4">Telemetry Activity Log</h5>
                    <div class="bg-black border border-[#222222] p-3 text-[9px] mt-2 font-mono h-28 overflow-y-auto custom-scrollbar text-neutral-400 space-y-1">
                        ${allLogs.filter(e => e.agent === agent.id || e.source_id === agent.id).map(e => `
                            <p class="${e.severity === 'high' || e.severity === 'critical' ? 'text-red-500' : 'text-white'}">&gt; [${e.timestamp ? new Date(e.timestamp).toLocaleTimeString() : '—'}] [${(e.event_type || '').toUpperCase()}] ${JSON.stringify(e.payload)}</p>
                        `).join('') || '<p class="text-neutral-500">&gt; No recent activity logged.</p>'}
                    </div>
                </div>
            </div>
        `;

        fetch(`/api/coordinator/agents/${agent.id}/tasks`)
            .then(res => res.json())
            .then(tasks => {
                const list = document.getElementById("agent-tasks-list");
                if (!list) return;
                if (!tasks || tasks.length === 0) {
                    list.innerHTML = "<p class='text-neutral-500 font-mono text-[9px]'>&gt; No active tasks in queue.</p>";
                } else {
                    list.innerHTML = tasks.map(t => `
                        <div class="border border-[#222] p-2 bg-[#0c0c0c] text-[9px] font-mono leading-relaxed">
                            <p class="text-white font-bold flex justify-between">
                                <span>TASK: ${t.task_id}</span>
                                <span class="text-cyan-400">[ ${t.status.toUpperCase()} ]</span>
                            </p>
                            <p class="text-neutral-400 mt-1">Action: <span class="text-white font-bold">${t.action}</span> | Target: <span class="text-white font-bold">${t.entity_id}</span></p>
                            ${t.error ? `<p class="text-red-500 mt-0.5">Error: ${t.error}</p>` : ''}
                        </div>
                    `).join('');
                }
            })
            .catch(err => {
                const list = document.getElementById("agent-tasks-list");
                if (list) list.innerHTML = `<p class='text-red-500 font-mono text-[9px]'>&gt; Error loading tasks: ${err}</p>`;
            });
    }

    function openAgentDetailModal(agentId) {
        activeDetailAgentId = agentId;
        activeOpenModal = null;
        const overlay = document.getElementById("modal-overlay");
        overlay.classList.remove("hidden");
        renderAgentDetailContent(agentId);
    }

    // ==========================================
    // NEO4J D3.JS GRAPH IMPLEMENTATION
    // ==========================================
    function initD3Graph() {
        const container = d3.select("#neo4j-d3-canvas");
        container.html(""); 
        
        const width = container.node().getBoundingClientRect().width || 500;
        const height = container.node().getBoundingClientRect().height || 192;

        const svg = container.append("svg")
            .attr("width", "100%")
            .attr("height", height);

        fetch("/api/graph/d3")
            .then(res => res.json())
            .then(data => {
                const nodes = data.nodes;
                const links = data.links;

                if (!nodes || nodes.length === 0) {
                    container.html(`<div class="text-neutral-500 font-mono text-[9px] p-4 text-center">&gt; Semantic cache empty. No relations registered.</div>`);
                    return;
                }

                const simulation = d3.forceSimulation(nodes)
                    .force("link", d3.forceLink(links).id(d => d.id).distance(80))
                    .force("charge", d3.forceManyBody().strength(-150))
                    .force("center", d3.forceCenter(width / 2, height / 2));

                const link = svg.append("g")
                    .selectAll("line")
                    .data(links)
                    .enter().append("line")
                    .attr("stroke", "#444444")
                    .attr("stroke-width", 1.5);

                const node = svg.append("g")
                    .selectAll("circle")
                    .data(nodes)
                    .enter().append("circle")
                    .attr("r", 6)
                    .attr("fill", d => d.color || "#8b5cf6")
                    .call(d3.drag()
                        .on("start", dragstarted)
                        .on("drag", dragged)
                        .on("end", dragended));

                node.append("title")
                    .text(d => `Node: ${d.id}\nType: ${d.type}`);

                const label = svg.append("g")
                    .selectAll("text")
                    .data(nodes)
                    .enter().append("text")
                    .attr("font-size", "7px")
                    .attr("fill", "#8a8a8a")
                    .attr("font-family", "monospace")
                    .attr("dx", 8)
                    .attr("dy", 3)
                    .text(d => d.id.split(':')[1] || d.id);

                simulation.on("tick", () => {
                    link
                        .attr("x1", d => d.source.x)
                        .attr("y1", d => d.source.y)
                        .attr("x2", d => d.target.x)
                        .attr("y2", d => d.target.y);

                    node
                        .attr("cx", d => d.x)
                        .attr("cy", d => d.y);

                    label
                        .attr("x", d => d.x)
                        .attr("y", d => d.y);
                });

                function dragstarted(event, d) {
                    if (!event.active) simulation.alphaTarget(0.3).restart();
                    d.fx = d.x;
                    d.fy = d.y;
                }

                function dragged(event, d) {
                    d.fx = event.x;
                    d.fy = event.y;
                }

                function dragended(event, d) {
                    if (!event.active) simulation.alphaTarget(0);
                    d.fx = null;
                    d.fy = null;
                }
            })
            .catch(err => {
                console.error("D3 graph fetch error:", err);
                container.html(`<div class="text-neutral-500 font-mono text-[9px] p-4 text-center">Graph Database Offline</div>`);
            });
    }

    function closeModal() {
        activeOpenModal = null;
        activeDetailAgentId = null;
        const overlay = document.getElementById("modal-overlay");
        overlay.classList.add("hidden");
    }

    function getForecastChartSvg(anomType) {
        let pathHistory = "";
        let pathForecast = "";
        let pathConfidence = "";
        const type = (anomType || "").toLowerCase();
        
        if (type.includes("slow_query")) {
            // Rises sharply towards the end (T+15)
            pathHistory = "M10,70 L30,65 L50,68 L70,60 L90,58 L110,50";
            pathForecast = "M110,50 L130,42 L150,30 L170,18 L190,10";
            pathConfidence = "M110,52 L130,48 L150,42 L170,35 L190,30 L190,2 L170,5 L150,15 L130,30 L110,48 Z";
        } else if (type.includes("cpu_spike")) {
            // Rises, peaks, starts minor leveling off
            pathHistory = "M10,75 L30,72 L50,65 L70,60 L90,40 L110,25";
            pathForecast = "M110,25 L130,22 L150,28 L170,35 L190,45";
            pathConfidence = "M110,30 L130,32 L150,42 L170,55 L190,70 L190,20 L170,15 L150,12 L130,10 L110,20 Z";
        } else {
            // General steady exponential rise
            pathHistory = "M10,80 L30,78 L50,75 L70,72 L90,68 L110,62";
            pathForecast = "M110,62 L130,52 L150,40 L170,25 L190,5";
            pathConfidence = "M110,65 L130,58 L150,50 L170,40 L190,25 L190,0 L170,10 L150,25 L130,45 L110,58 Z";
        }

        return `
            <svg class="w-full h-full p-2" viewBox="0 0 200 100" preserveAspectRatio="none">
                <!-- Grid Lines -->
                <line x1="0" y1="20" x2="200" y2="20" stroke="#222" stroke-dasharray="2,2" />
                <line x1="0" y1="50" x2="200" y2="50" stroke="#222" stroke-dasharray="2,2" />
                <line x1="0" y1="80" x2="200" y2="80" stroke="#222" stroke-dasharray="2,2" />
                <line x1="110" y1="0" x2="110" y2="100" stroke="#333" stroke-dasharray="1,2" />
                
                <!-- Confidence Area -->
                <path d="${pathConfidence}" fill="rgba(239, 68, 68, 0.12)" />
                
                <!-- History Line (solid) -->
                <path d="${pathHistory}" fill="none" stroke="#a3a3a3" stroke-width="1.5" />
                
                <!-- Prediction Line (dashed) -->
                <path d="${pathForecast}" fill="none" stroke="#ef4444" stroke-width="1.5" stroke-dasharray="3,2" />
                
                <!-- Dots for T+5 and T+15 -->
                <circle cx="140" cy="${type.includes("slow_query") ? 36 : (type.includes("cpu_spike") ? 25 : 46)}" r="3" fill="#fbbf24" />
                <circle cx="180" cy="${type.includes("slow_query") ? 14 : (type.includes("cpu_spike") ? 40 : 13)}" r="3" fill="#ef4444" />
                
                <!-- Current Time divider label -->
                <text x="112" y="92" fill="#888" font-family="monospace" font-size="7">NOW</text>
                <text x="142" y="92" fill="#fbbf24" font-family="monospace" font-size="7">T+5</text>
                <text x="182" y="92" fill="#ef4444" font-family="monospace" font-size="7">T+15</text>
            </svg>
        `;
    }

    window.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            closeModal();
        }
    });

    let metricsPollInterval = null;

    function startMetricsPolling() {
        if (metricsPollInterval) return;
        metricsPollInterval = setInterval(() => {
            if (!isSystemConnected) return;

            // Fetch DB metrics history
            fetch('/api/metrics/history?entity_id=db:shopcore-postgres&event_type=slow_query_detected&limit=20')
                .then(res => res.json())
                .then(data => {
                    if (Array.isArray(data) && data.length > 0) {
                        sparklineData.db = data.map(d => d.value || 0);
                    } else {
                        sparklineData.db = [0]; // default flat
                    }
                    drawSparkline("db-load-spark", sparklineData.db, "#22c55e", "#050505");
                })
                .catch(() => {});

            // Fetch Auth Latency metrics history
            fetch('/api/metrics/history?entity_id=svc:api-gateway&event_type=api_latency_spike&limit=20')
                .then(res => res.json())
                .then(data => {
                    if (Array.isArray(data) && data.length > 0) {
                        sparklineData.auth = data.map(d => d.value || 0);
                    } else {
                        sparklineData.auth = [0]; // default flat
                    }
                    drawSparkline("auth-latency-spark", sparklineData.auth, "#f59e0b", "#050505");
                })
                .catch(() => {});

        }, 3000);
    }

    function stopMetricsPolling() {
        if (metricsPollInterval) {
            clearInterval(metricsPollInterval);
            metricsPollInterval = null;
        }
    }

    // ==========================================
    // WEBSOCKET REAL-TIME CONNECTION
    // ==========================================
    function connectWebSocket() {
        const wsProtocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${wsProtocol}//${window.location.host}/ws/dashboard`;
        console.log(`Connecting WebSocket to ${wsUrl}...`);
        
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
            console.log("WebSocket connection established successfully.");
            
            // Clear console and show fresh incoming logs
            document.getElementById("kafka-log-console").innerHTML = "<p class='text-green-500'>&gt; Connected to Kafka event broker. Ingesting feeds...</p>";
            
            allLogs = [];
            fetch('/api/events?limit=20')
                .then(res => res.json())
                .then(data => {
                    if (Array.isArray(data)) {
                        allLogs = data;
                        renderKafkaLogsConsole();
                    }
                })
                .catch(err => console.error("Error fetching recent events:", err));
            
            FEEDBACK_ITEMS.length = 0;
            renderFeedbackLog();

            // Poll for latest decision log
            fetchLatestDecision();
        };

        ws.onmessage = (event) => {
            try {
                const msg = JSON.parse(event.data);
                if (msg.type === "status_update") {
                    updateDashboardData(msg.data);
                } else if (msg.type === "event") {
                    handleIncomingEvent(msg.data);
                }
            } catch (err) {
                console.error("Failed to parse websocket message:", err);
            }
        };

        ws.onerror = (err) => {
            console.warn("WebSocket encountered error. Disconnecting...");
        };

        ws.onclose = () => {
            console.warn("WebSocket closed.");
            if (isSystemConnected) {
                setTimeout(() => {
                    if (isSystemConnected) connectWebSocket();
                }, 3000);
            }
        };
    }

    let decisionInterval = null;
    function fetchLatestDecision() {
        if (!isSystemConnected) {
            if (decisionInterval) {
                clearInterval(decisionInterval);
                decisionInterval = null;
            }
            return;
        }
        
        if (!decisionInterval) {
            decisionInterval = setInterval(fetchLatestDecision, 3000);
        }

        fetch('/api/decision/latest')
            .then(res => res.json())
            .then(data => {
                if (data && data.recommended_action) {
                    DECISION_LOG.recommended_action = data.recommended_action;
                    DECISION_LOG.root_cause_hypothesis = data.root_cause_hypothesis?.hypothesis || data.root_cause_hypothesis || "Unknown";
                    DECISION_LOG.situation_assessment = data.situation_assessment || "";
                    DECISION_LOG.confidence = data.confidence || 1.0;
                    DECISION_LOG.tokens_used = data.tokens_used || 0;
                    DECISION_LOG.action_parameters = data.action_parameters || {};
                    DECISION_LOG.requires_human_approval = data.requires_human_approval || false;
                    DECISION_LOG.reasoning_trace = data.reasoning_trace || "No reasoning trace reported.";
                    renderReasoningCore();
                } else {
                    // Reset to clean nominal status
                    const meta = document.getElementById("reasoning-meta-block");
                    const json = document.getElementById("reasoning-decision-json");
                    if (meta && json) {
                        meta.innerHTML = `
                            <span class="text-neutral-400 font-bold block mb-1">SYSTEM STATE: NOMINAL</span>
                            <span class="text-neutral-500 block mb-2 text-[9px]">HYPOTHESIS: No active anomalies detected.</span>
                        `;
                        const confEl = document.getElementById('reasoning-conf');
                        const tokensEl = document.getElementById('reasoning-tokens');
                        const pathEl = document.getElementById('reasoning-path');
                        if (confEl) confEl.textContent = "100%";
                        if (tokensEl) tokensEl.textContent = "0";
                        if (pathEl) pathEl.textContent = "HEURISTIC";
                        json.textContent = "{\n  \"status\": \"idle\"\n}";
                    }
                }
            })
            .catch(err => console.error("Error fetching latest decision:", err));
    }

    function updateDashboardData(data) {
        if (!data) return;
        last_status = data;

        // 1. Overall System Health
        const healthBadge = document.getElementById("sys-health-status");
        if (healthBadge) {
            healthBadge.textContent = `[ ${data.health} ]`;
            if (data.health === "NOMINAL") {
                healthBadge.className = "text-green-500 font-bold";
            } else if (data.health === "OFFLINE" || data.health === "DISCONNECTED") {
                healthBadge.className = "text-neutral-500 font-bold";
            } else {
                healthBadge.className = "text-red-500 font-bold blink-status";
            }
        }

        // 2. Active anomalies counter
        const activeAnomsBadge = document.getElementById("anomalies-active-display");
        if (activeAnomsBadge) {
            activeAnomsBadge.textContent = `${(data.anomalies || []).length} ACTIVE`;
            activeAnomsBadge.className = (data.anomalies || []).length > 0 ? "font-bold text-red-500" : "font-bold text-neutral-400";
        }

        // 3. World model anomalies list
        if (data.anomalies) {
            WORLD_ANOMALIES.length = 0;
            data.anomalies.forEach(a => {
                WORLD_ANOMALIES.push({
                    id: a.anomaly_id || a.id || 'unknown_anomaly',
                    type: a.event_type || a.type || 'unknown_anomaly',
                    entity: a.entity_id || a.entity || 'unknown_entity',
                    severity: a.severity || 'HIGH',
                    chain: a.causal_chain || a.chain || `${a.entity_id || 'unknown'} ➔ anomaly`
                });
            });
            renderAnomalies();
            
            // Recalculate entity registry health states dynamically
            ENTITIES.forEach(ent => {
                const isAffected = WORLD_ANOMALIES.some(a => a.entity === ent.id);
                if (isAffected) {
                    const criticalAnom = WORLD_ANOMALIES.find(a => a.entity === ent.id && a.severity === "CRITICAL");
                    ent.status = criticalAnom ? "DEGRADED" : "STRESSED";
                    ent.trend = "↓";
                } else {
                    ent.status = "NOMINAL";
                    ent.trend = "↑";
                }
            });
            renderEntityTable();
        }

        // 4. Mapped Active agent console states
        if (data.agents && Object.keys(data.agents).length > 0) {
            const mappedAgents = Object.entries(data.agents).map(([id, info], idx) => {
                const name = id.replace("agent:", "");
                const prettyRole = name.charAt(0).toUpperCase() + name.slice(1);
                return {
                    id: id,
                    short: `A-0${idx+1}`,
                    role: prettyRole,
                    status: info.status === "running" ? "ACTIVE" : "IDLE",
                    task: info.task || ""
                };
            });
            AGENTS_LIST.length = 0;
            AGENTS_LIST.push(...mappedAgents);
            renderAgentCards();
            
            // Keep active online count updated
            const onlineCount = mappedAgents.filter(a => a.status === "ACTIVE").length;
            document.getElementById("agents-online-display").textContent = `${onlineCount}/${mappedAgents.length} ONLINE`;
        }

        // 5. Active plan step approvals
        if (data.active_plans) {
            const approvalWidget = document.getElementById("approval-gate-widget");
            
            // Clear and rebuild PLAN_STEPS
            PLAN_STEPS.length = 0;
            const waitingPlansAndSteps = [];
            
            data.active_plans.forEach(activePlan => {
                if (activePlan.steps) {
                    activePlan.steps.forEach((s, idx) => {
                        PLAN_STEPS.push({
                            planId: activePlan.plan_id,
                            goal: activePlan.goal,
                            step: idx + 1,
                            name: s.description || s.action,
                            status: s.status
                        });
                        
                        if (s.status === "waiting_approval" || (s.is_approval_gate && s.status === "waiting_approval")) {
                            waitingPlansAndSteps.push({
                                plan: activePlan,
                                step: s
                            });
                        }
                    });
                }
            });
            
            renderPlanSteps();

            if (waitingPlansAndSteps.length > 0) {
                approvalWidget.style.display = "flex";
                approvalWidget.className = "border border-[#333] p-3 bg-[#0a0a0a] mt-2 flex flex-col gap-3";
                
                let html = "";
                waitingPlansAndSteps.forEach(({ plan, step }) => {
                    html += `
                        <div class="border border-[#222] p-2 bg-[#0c0c0c] flex flex-col gap-2 text-left">
                            <div>
                                <p class="font-mono text-[9px] text-[#fbbf24] font-bold uppercase tracking-wider">[ RECON_DECISION_PENDING // PLAN: ${plan.plan_id.substring(0,10)} ]</p>
                                <p class="font-mono text-[8px] text-neutral-400 mt-0.5">Goal: ${plan.goal}</p>
                                <p class="font-mono text-[10px] text-white font-bold mt-1">${step.description || 'Approval Gate active.'}</p>
                            </div>
                            <div class="grid grid-cols-2 gap-2">
                                <button onclick="event.stopPropagation(); executeAction('approve', '${plan.plan_id}');" class="py-1.5 bg-black text-white font-mono text-[10px] font-bold hover:bg-neutral-800 transition-colors uppercase tracking-widest border border-[#333]">Approve</button>
                                <button onclick="event.stopPropagation(); executeAction('reject', '${plan.plan_id}');" class="py-1.5 border border-[#444] bg-white text-black font-mono text-[10px] font-bold hover:bg-neutral-100 transition-colors uppercase tracking-widest">Reject</button>
                            </div>
                        </div>
                    `;
                });
                approvalWidget.innerHTML = html;
            } else {
                approvalWidget.style.display = "none";
            }
        }

        // 6. DB Storage Stats
        if (data.memory_stats) {
            const redisValEl = document.getElementById("mem-redis-val");
            const timescaleValEl = document.getElementById("mem-timescale-val");
            const neo4jValEl = document.getElementById("mem-neo4j-val");
            const qdrantValEl = document.getElementById("mem-qdrant-val");
            
            if (redisValEl) redisValEl.textContent = `${data.memory_stats.redis_keys !== undefined ? data.memory_stats.redis_keys : 0} Keys`;
            if (timescaleValEl) timescaleValEl.textContent = `${data.memory_stats.timescale_records !== undefined ? data.memory_stats.timescale_records : 0} Records`;
            if (neo4jValEl) neo4jValEl.textContent = `${data.memory_stats.neo4j_nodes !== undefined ? data.memory_stats.neo4j_nodes : 0} Nodes`;
            if (qdrantValEl) qdrantValEl.textContent = `${data.memory_stats.qdrant_playbooks !== undefined ? data.memory_stats.qdrant_playbooks : 0} Playbooks`;
        }

        // 7. Feedback Loops
        if (data.feedback_metrics) {
            document.getElementById("feedback-learned-count").textContent = data.feedback_metrics.total_processed || 0;
        }

        // 8. Throughput Update
        if (data.throughput !== undefined) {
            sparklineData.kafka.push(data.throughput);
            if (sparklineData.kafka.length > 20) {
                sparklineData.kafka.shift();
            }
            drawSparkline("kafka-throughput-spark", sparklineData.kafka, "#111111", "#f0f0f0");
            
            const throughputVal = document.getElementById("kafka-rate-events");
            if (throughputVal) {
                throughputVal.textContent = `${data.throughput} msg/s`;
            }
        }

        // 9. Adapters Update
        if (data.adapters && data.adapters.length > 0) {
            perceptionAdapters = data.adapters;
            renderPerceptionAdapters();

            // Clear and repopulate perception failures list from adapter data
            const failureList = document.getElementById('perception-failures-list');
            if (failureList) {
                const failures = data.adapters.filter(a => a.status === 'DOWN' || a.status === 'ERROR');
                if (failures.length === 0) {
                    failureList.innerHTML = '<li style="color:#aaa;">No validation failures detected.</li>';
                } else {
                    failureList.innerHTML = failures.map(f =>
                        `<li>${f.type} [${f.source}]: status=${f.status}, latency=${f.latency}</li>`
                    ).join('');
                }
            }
        }

        // 10. Extended Kafka topic rates — fill agent_events + actions rows
        if (data.throughput !== undefined) {
            const rateAgent = document.getElementById('kafka-rate-agent');
            const rateActions = document.getElementById('kafka-rate-actions');
            if (rateAgent) rateAgent.textContent = `${data.throughput} msg/s`;
            if (rateActions) rateActions.textContent = `0.0 msg/s`;
            const lagAgent = document.getElementById('kafka-lag-agent');
            const lagActions = document.getElementById('kafka-lag-actions');
            if (lagAgent) lagAgent.textContent = '0 msgs';
            if (lagActions) lagActions.textContent = '0 msgs';
        }

        // 11. Redis and Qdrant stats are now updated directly from memory_stats in real-time.

        // 12. Temporal panel — derive current metrics from anomalies
        if (data.anomalies && data.anomalies.length > 0) {
            const dbAnom = data.anomalies.find(a => (a.entity_id || '').includes('db') || (a.event_type || '').includes('database'));
            const svcAnom = data.anomalies.find(a => (a.entity_id || '').includes('svc') || (a.event_type || '').includes('service'));

            const dbUtil = document.getElementById('db-util-current');
            if (dbUtil) dbUtil.textContent = dbAnom ? 'CURRENT: HIGH' : 'CURRENT: NOMINAL';

            const svcLat = document.getElementById('svc-latency-current');
            if (svcLat) svcLat.textContent = svcAnom ? 'CURRENT: ELEVATED' : 'CURRENT: OK';

            // Prediction values — pull from first anomaly if it has predictions
            const firstAnomWithPred = data.anomalies.find(a => a.predictions);
            if (firstAnomWithPred && firstAnomWithPred.predictions) {
                const p = firstAnomWithPred.predictions;
                const t5 = document.getElementById('pred-t5');
                const t15 = document.getElementById('pred-t15');
                const t60 = document.getElementById('pred-t60');
                if (t5 && p.t5) t5.textContent = `${p.t5.value || '?'} (${p.t5.confidence || '?'} CONF)`;
                if (t15 && p.t15) t15.textContent = `${p.t15.value || '?'} (${p.t15.confidence || '?'} CONF)`;
                if (t60 && p.t60) t60.textContent = `${p.t60.value || '?'} (${p.t60.confidence || '?'} CONF)`;
            }
        } else {
            // No anomalies — no prediction data yet
            const dbUtil = document.getElementById('db-util-current');
            if (dbUtil) dbUtil.textContent = 'CURRENT: —';
            const svcLat = document.getElementById('svc-latency-current');
            if (svcLat) svcLat.textContent = 'CURRENT: —';
            const t5 = document.getElementById('pred-t5');
            const t15 = document.getElementById('pred-t15');
            const t60 = document.getElementById('pred-t60');
            if (t5) t5.textContent = 'INSUFFICIENT DATA';
            if (t15) t15.textContent = 'INSUFFICIENT DATA';
            if (t60) t60.textContent = 'INSUFFICIENT DATA';
        }

        // 13. LLM cost (reasoning panel) — only show if service is online
        const llmCostEl = document.getElementById('reasoning-llm-cost');
        if (llmCostEl) {
            const reasoningOnline = data.services && data.services.reasoning === 'online';
            llmCostEl.textContent = reasoningOnline ? 'LLM SERVICE: ONLINE' : 'LLM SERVICE: OFFLINE';
        }

        // Trigger real-time refresh of any open modal
        refreshOpenModal();
    }

    function refreshOpenModal() {
        if (activeOpenModal) {
            if (activeOpenModal === 'memory-neo4j') {
                // Do NOT re-render Neo4j modal on updates to avoid jittering and resetting the D3 simulation
                return;
            }
            let cypherVal = null;
            if (activeOpenModal === 'memory-neo4j') {
                const inp = document.getElementById("cypher-input");
                if (inp) cypherVal = inp.value;
            }
            
            renderModalContent(activeOpenModal, true);
            
            if (activeOpenModal === 'memory-neo4j' && cypherVal !== null) {
                const inp = document.getElementById("cypher-input");
                if (inp) inp.value = cypherVal;
            }
        } else if (activeDetailAgentId) {
            renderAgentDetailContent(activeDetailAgentId);
        }
    }

    function handleIncomingEvent(event) {
        if (!event) return;
        
        // Push event into logs console
        allLogs.push(event);
        if (allLogs.length > 50) allLogs.shift();
        
        // Intercept feedback loop events
        if (event.event_type === "reasoning_completed" && event.source_id === "agent:feedback-loop") {
            const time = new Date().toLocaleTimeString();
            FEEDBACK_ITEMS.unshift({
                time: time,
                type: "ACTION_OUTCOME",
                name: event.payload?.action || "unknown_action",
                result: `PLAN: ${(event.payload?.plan_id || '').substring(0, 8)} ➔ ${(event.payload?.outcome || 'success').toUpperCase()}`
            });
            if (FEEDBACK_ITEMS.length > 10) FEEDBACK_ITEMS.pop();
            renderFeedbackLog();
        }

        // Flash flowchart node
        animateFlowchartNode(event.event_type, event.source_type);
        
        // Re-render logs console
        renderKafkaLogsConsole();
        renderAgentMarquee();
    }

    function animateFlowchartNode(eventType, sourceType) {
        let nodeId = null;
        const evType = (eventType || '').toLowerCase();
        const srcType = (sourceType || '').toLowerCase();
        
        if (srcType === "perception" || evType.includes("alert") || evType.includes("failure")) {
            nodeId = "fn-perception";
        } else if (evType.includes("kafka") || evType.includes("bus")) {
            nodeId = "fn-eventbus";
        } else if (evType.includes("memory") || evType.includes("db") || evType.includes("flush")) {
            nodeId = "fn-memory";
        } else if (evType.includes("anomaly") || evType.includes("drift")) {
            nodeId = "fn-worldmodel";
        } else if (evType.includes("reasoning")) {
            nodeId = "fn-reasoning";
        } else if (evType.includes("plan") && !evType.includes("step")) {
            nodeId = "fn-planning";
        } else if (evType.includes("execution") || evType.includes("action") || evType.includes("step")) {
            nodeId = "fn-execution";
        } else if (evType.includes("feedback") || evType.includes("outcome")) {
            nodeId = "fn-feedback";
        }
        
        if (nodeId) {
            const el = document.getElementById(nodeId);
            if (el) {
                el.style.borderColor = "#ffffff";
                el.style.boxShadow = "0 0 10px rgba(255, 255, 255, 0.6)";
                el.style.backgroundColor = "#000000";
                el.style.color = "#ffffff";
                
                setTimeout(() => {
                    el.className = `flow-node border border-[#e2e2e2] bg-white p-3 text-left w-40 h-28 flex flex-col justify-between hover:border-black transition-all text-black`;
                    el.style.boxShadow = "none";
                    el.style.backgroundColor = "";
                    el.style.color = "";
                }, 400);
            }
        }
    }

    function executeAction(actionType, planId) {
        const endpoint = actionType === 'approve' ? `/api/plans/${planId}/approve` : `/api/plans/${planId}/reject`;
        fetch(endpoint, { method: "POST" })
            .then(res => res.json())
            .then(data => {
                alert(`Plan [${planId}] successfully ${actionType === 'approve' ? 'approved' : 'rejected/aborted'}!`);
            })
            .catch(err => {
                console.error("Action error:", err);
                alert("Failed to submit action proxy query.");
            });
    }

    window.onload = () => {
        initUI();
        fetch('/api/connection/status')
            .then(res => res.json())
            .then(data => {
                if (data.connected) {
                    isSystemConnected = true;
                    uptimeSeconds = 0;
                    ENTITIES = [
                        { id: "svc:api-gateway", status: "NOMINAL", trend: "↑" },
                        { id: "svc:product-service", status: "NOMINAL", trend: "↑" },
                        { id: "svc:order-service", status: "NOMINAL", trend: "↑" },
                        { id: "svc:cart-service", status: "NOMINAL", trend: "↑" },
                        { id: "svc:user-service", status: "NOMINAL", trend: "↑" },
                        { id: "svc:notification-service", status: "NOMINAL", trend: "↑" },
                        { id: "db:shopcore-postgres", status: "NOMINAL", trend: "↑" },
                        { id: "cache:shopcore-redis", status: "NOMINAL", trend: "↑" },
                        { id: "queue:order-events", status: "NOMINAL", trend: "↑" }
                    ];
                    renderEntityTable();
                    updateConnectionUI();
                    connectWebSocket();
                    startMetricsPolling();
                }
            })
            .catch(() => {});
    };

