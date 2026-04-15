/**
 * updateStats.js
 * Dashboard polling script - updates every 3 seconds.
 *
 * Fixes applied:
 *  - Health status was stuck on "Loading" because fetch errors were silently
 *    swallowed; now each section handles its own errors independently so one
 *    failing endpoint cannot block the others.
 *  - Processing metrics (proc-perf, proc-error, proc-cpu, proc-severity) now
 *    always write a value even when the API returns 0.
 *  - Event headings show the exact index that was fetched.
 */

const VM_IP = "172.169.248.121"

const PROCESSING_STATS_API_URL = `http://${VM_IP}/processing/stats`
const ANALYZER_STATS_API_URL = `http://${VM_IP}/analyzer/stats`
const ANALYZER_PERFORMANCE_API_BASE = `http://${VM_IP}/analyzer/performance`
const ANALYZER_ERROR_API_BASE = `http://${VM_IP}/analyzer/error`
const HEALTH_CHECK_API_URL = `http://${VM_IP}/healthcheck/health-status`

/* ─────────────────────────────────────────────────────────────────────────── */
/* Helpers                                                                     */
/* ─────────────────────────────────────────────────────────────────────────── */

const getLocaleDateStr = () => (new Date()).toLocaleString();

const getTimeAgo = (isoTimestamp) => {
    try {
        const secondsAgo = Math.floor((Date.now() - new Date(isoTimestamp)) / 1000);
        if (secondsAgo < 0)    return "just now";
        if (secondsAgo < 60)   return `${secondsAgo}s ago`;
        if (secondsAgo < 3600) return `${Math.floor(secondsAgo / 60)}m ago`;
        return `${Math.floor(secondsAgo / 3600)}h ago`;
    } catch (e) {
        return "unknown";
    }
};

/**
 * Fetch a URL and call cb(data) on success.
 * On failure, call onError(errorMessage) if provided — never throws.
 */
const makeReq = (url, cb, onError) => {
    fetch(url)
        .then(res => {
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            return res.json();
        })
        .then(data => {
            console.log(`✓ ${url}`);
            cb(data);
        })
        .catch(err => {
            console.error(`✗ ${url}:`, err.message);
            if (onError) onError(err.message);
        });
};

const setEl = (id, value) => {
    const el = document.getElementById(id);
    if (el) el.innerText = value;
};

const setInnerHTML = (id, html) => {
    const el = document.getElementById(id);
    if (el) el.innerHTML = html;
};

const setJSON = (id, data) => {
    setEl(id, JSON.stringify(data, null, 2));
};

/* ─────────────────────────────────────────────────────────────────────────── */
/* Section updaters                                                             */
/* ─────────────────────────────────────────────────────────────────────────── */

/**
 * Processing service — 4 metric tiles + raw JSON block.
 */
const fetchProcessingStats = () => {
    makeReq(
        PROCESSING_STATS_API_URL,
        (stats) => {
            // Metric tiles
            setEl("proc-perf",     stats.num_performance_readings ?? 0);
            setEl("proc-error",    stats.num_error_readings       ?? 0);
            setEl("proc-cpu",      ((stats.max_cpu_reading ?? 0).toFixed(1)) + "%");
            setEl("proc-severity", stats.max_severity_level       ?? 0);
            // Raw JSON
            setJSON("processing-stats", stats);
        },
        (err) => {
            setEl("processing-stats", `Error: ${err}`);
        }
    );
};

/**
 * Analyzer service — event counts + one random event of each type.
 */
const fetchAnalyzerStats = () => {
    makeReq(
        ANALYZER_STATS_API_URL,
        (stats) => {
            // Count tiles
            setEl("ana-perf",  stats.num_performance_events ?? 0);
            setEl("ana-error", stats.num_error_events       ?? 0);
            setJSON("analyzer-stats", stats);

            // ── Random performance event ───────────────────────────────────
            const numPerf = stats.num_performance_events || 0;
            if (numPerf > 0) {
                const idx = Math.floor(Math.random() * numPerf);
                setEl("performance-event-heading", `📉 Performance Event (Index ${idx})`);
                makeReq(
                    `${ANALYZER_PERFORMANCE_API_BASE}?index=${idx}`,
                    (event) => setJSON("event-performance", event),
                    (err)   => setEl("event-performance", `Error fetching index ${idx}: ${err}`)
                );
            } else {
                setEl("performance-event-heading", "📉 Performance Event (No data)");
                setEl("event-performance", "No performance events available yet.");
            }

            // ── Random error event ─────────────────────────────────────────
            const numErr = stats.num_error_events || 0;
            if (numErr > 0) {
                const idx = Math.floor(Math.random() * numErr);
                setEl("error-event-heading", `⚠️ Error Event (Index ${idx})`);
                makeReq(
                    `${ANALYZER_ERROR_API_BASE}?index=${idx}`,
                    (event) => setJSON("event-error", event),
                    (err)   => setEl("event-error", `Error fetching index ${idx}: ${err}`)
                );
            } else {
                setEl("error-event-heading", "⚠️ Error Event (No data)");
                setEl("event-error", "No error events available yet.");
            }
        },
        (err) => {
            setEl("analyzer-stats", `Error: ${err}`);
        }
    );
};

/**
 * Health check service — update each service card dynamically.
 */
const fetchHealthStatus = () => {
    makeReq(
        HEALTH_CHECK_API_URL,
        (data) => {
            const grid = document.getElementById("health-services");
            if (!grid) return;

            const services = {
                receiver:   "Receiver",
                storage:    "Storage",
                processing: "Processing",
                analyzer:   "Analyzer",
            };

            grid.innerHTML = "";

            for (const [key, label] of Object.entries(services)) {
                const status = data[key] || "Unknown";
                const cls    = status === "Up" ? "up" : status === "Down" ? "down" : "unknown";
                const icon   = status === "Up" ? "✓ Up" : status === "Down" ? "✗ Down" : "? Unknown";

                grid.innerHTML += `
                    <div class="service-item ${cls}">
                        <div class="service-indicator ${cls}"></div>
                        <div class="service-name">${label}</div>
                        <div class="service-status ${cls}">${icon}</div>
                    </div>`;
            }

            setEl("health-last-update", getTimeAgo(data.last_update));
        },
        (err) => {
            // Don't wipe the grid on error — just update the timestamp line
            setEl("health-last-update", `fetch failed: ${err}`);
        }
    );
};

/* ─────────────────────────────────────────────────────────────────────────── */
/* Main poll loop                                                               */
/* ─────────────────────────────────────────────────────────────────────────── */

const refresh = () => {
    console.log("🔄 Refreshing dashboard…");
    setEl("last-updated-value", getLocaleDateStr());

    fetchProcessingStats();
    fetchAnalyzerStats();
    fetchHealthStatus();
};

// Run immediately on DOMContentLoaded, then every 3 seconds.
document.addEventListener("DOMContentLoaded", () => {
    console.log("🚀 Dashboard initialized");
    refresh();
    setInterval(refresh, 5000);
});