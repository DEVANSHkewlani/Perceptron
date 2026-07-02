/**
 * Cognitive Perception Browser SDK
 * ================================
 * Zero-dependency, lightweight script to capture user interactions,
 * browser console/exceptions, and Core Web Vitals.
 * 
 * Usage:
 *   <script src="/sdk/cognitive_sdk.js"></script>
 *   <script>
 *     CognitiveSdk.init({
 *       endpoint: 'http://localhost:8080',
 *       userId: 'user_99482',
 *       tags: ['production', 'beta']
 *     });
 *   </script>
 */

(function (root, factory) {
    if (typeof define === 'function' && define.amd) {
        define([], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory();
    } else {
        root.CognitiveSdk = factory();
    }
}(typeof self !== 'undefined' ? self : this, function () {
    'use strict';

    let config = {
        endpoint: 'http://localhost:8080',
        sessionId: '',
        userId: null,
        batchIntervalMs: 5000,
        debug: false
    };

    let eventQueue = [];
    let batchTimer = null;
    let isInitialized = false;

    // Helper: generate simple UUID / Session ID
    function generateUUID() {
        return 'sess-' + Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
    }

    // Helper: debug logging
    function log(...args) {
        if (config.debug) {
            console.log('[CognitiveSDK]', ...args);
        }
    }

    // Send data immediately to /perception/browser-events
    async function sendBrowserEvent(payload) {
        const url = `${config.endpoint}/perception/browser-events`;
        const enriched = {
            ...payload,
            session_id: config.sessionId,
            browser: navigator.userAgent,
            url: window.location.href,
            timestamp: new Date().toISOString()
        };
        log('Sending immediate browser event:', enriched);
        try {
            if (navigator.sendBeacon) {
                navigator.sendBeacon(url, JSON.stringify(enriched));
            } else {
                await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(enriched)
                });
            }
        } catch (e) {
            console.error('[CognitiveSDK] Failed to send browser event', e);
        }
    }

    // Queue user interaction events for batched sending
    function queueUserEvent(type, payload = {}) {
        const item = {
            event_type: type,
            timestamp: new Date().toISOString(),
            page: window.location.pathname,
            ...payload
        };
        log('Queueing user event:', item);
        eventQueue.push(item);
    }

    // Flush eventQueue to /perception/user-events
    async function flushBatch() {
        if (eventQueue.length === 0) return;

        const batch = {
            events: [...eventQueue],
            session_id: config.sessionId,
            user_id: config.userId
        };

        eventQueue = []; // clear queue immediately to prevent double-send
        log('Flushing user event batch:', batch);

        try {
            const url = `${config.endpoint}/perception/user-events`;
            await fetch(url, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(batch)
            });
        } catch (e) {
            console.error('[CognitiveSDK] Failed to flush user events batch', e);
            // Put items back in the queue on failure
            eventQueue = batch.events.concat(eventQueue);
        }
    }

    // Start auto-flushing batches
    function startBatchTimer() {
        if (batchTimer) clearInterval(batchTimer);
        batchTimer = setInterval(flushBatch, config.batchIntervalMs);
    }

    // Setup global error observers
    function setupErrorTracking() {
        // Capture JS runtime exceptions
        window.addEventListener('error', function (event) {
            sendBrowserEvent({
                type: 'js_error',
                message: event.message || (event.error && event.error.message) || 'Script error',
                stack: event.error ? event.error.stack : '',
                filename: event.filename,
                lineno: event.lineno
            });
        });

        // Capture unhandled promise rejections
        window.addEventListener('unhandledrejection', function (event) {
            sendBrowserEvent({
                type: 'unhandled_rejection',
                message: event.reason ? (event.reason.message || String(event.reason)) : 'Promise rejected',
                stack: event.reason && event.reason.stack ? event.reason.stack : ''
            });
        });
    }

    // Setup Core Web Vitals tracking using PerformanceObserver
    function setupWebVitals() {
        if (!('PerformanceObserver' in window)) return;

        try {
            // 1. Largest Contentful Paint (LCP)
            const lcpObserver = new PerformanceObserver((entryList) => {
                const entries = entryList.getEntries();
                const lastEntry = entries[entries.length - 1];
                sendBrowserEvent({
                    type: 'LCP',
                    value: lastEntry.startTime,
                    element: lastEntry.element ? lastEntry.element.tagName : ''
                });
            });
            lcpObserver.observe({ type: 'largest-contentful-paint', buffered: true });

            // 2. First Input Delay (FID)
            const fidObserver = new PerformanceObserver((entryList) => {
                entryList.getEntries().forEach((entry) => {
                    const delay = entry.processingStart - entry.startTime;
                    sendBrowserEvent({
                        type: 'FID',
                        value: delay,
                        element: entry.target ? entry.target.tagName : ''
                    });
                });
            });
            fidObserver.observe({ type: 'first-input', buffered: true });

            // 3. Cumulative Layout Shift (CLS)
            let clsValue = 0;
            const clsObserver = new PerformanceObserver((entryList) => {
                for (const entry of entryList.getEntries()) {
                    if (!entry.hadRecentInput) {
                        clsValue += entry.value;
                    }
                }
                // Send shift value incrementally
                if (clsValue > 0.05) {
                    sendBrowserEvent({
                        type: 'CLS',
                        value: clsValue
                    });
                }
            });
            clsObserver.observe({ type: 'layout-shift', buffered: true });

            // 4. Time to First Byte (TTFB) & First Contentful Paint (FCP)
            window.addEventListener('load', () => {
                setTimeout(() => {
                    const navEntry = performance.getEntriesByType('navigation')[0];
                    if (navEntry) {
                        sendBrowserEvent({
                            type: 'TTFB',
                            value: navEntry.responseStart
                        });
                    }
                    const paintEntries = performance.getEntriesByType('paint');
                    paintEntries.forEach((entry) => {
                        if (entry.name === 'first-contentful-paint') {
                            sendBrowserEvent({
                                type: 'FCP',
                                value: entry.startTime
                            });
                        }
                    });
                }, 0);
            });
        } catch (e) {
            log('Error setting up Web Vitals observers:', e);
        }
    }

    // Setup DOM-level interaction observers (Clicks, Page views, Forms)
    function setupInteractionTracking() {
        // Track page view on load
        queueUserEvent('page_view', {
            title: document.title,
            referrer: document.referrer
        });

        // Track clicks & rage-click detection
        let lastClickTime = 0;
        let lastTarget = null;
        let rapidClickCount = 0;

        document.addEventListener('click', function (e) {
            const target = e.target;
            if (!target) return;

            const now = Date.now();
            const timeDiff = now - lastClickTime;

            // Rage click detection: 3 clicks within 2 seconds on the same element
            if (target === lastTarget && timeDiff < 800) {
                rapidClickCount++;
                if (rapidClickCount >= 3) {
                    queueUserEvent('rage_click', {
                        element: target.tagName + '#' + target.id + '.' + target.className,
                        text: target.innerText ? target.innerText.substring(0, 30) : ''
                    });
                    rapidClickCount = 0; // reset
                }
            } else {
                rapidClickCount = 1;
            }

            lastClickTime = now;
            lastTarget = target;

            // Log basic click if it's an interactive element
            if (target.closest('a, button, input[type="submit"], [role="button"]')) {
                const el = target.closest('a, button, input[type="submit"], [role="button"]');
                queueUserEvent('click', {
                    element: el.tagName + '#' + el.id + '.' + el.className,
                    text: el.innerText ? el.innerText.substring(0, 30) : '',
                    href: el.href || ''
                });
            }
        });

        // Track form submissions
        document.addEventListener('submit', function (e) {
            const form = e.target;
            if (!form) return;
            queueUserEvent('form_submit', {
                form_id: form.id || form.name || 'unnamed-form',
                action: form.action || ''
            });
        });

        // Track page exits
        window.addEventListener('beforeunload', function () {
            queueUserEvent('page_exit', {
                duration_seconds: performance.now() / 1000
            });
            // Use synchronous beacon to flush remaining events on exit
            flushBatch();
        });
    }

    return {
        /**
         * Initialize the Cognitive Perception Browser SDK.
         * @param {Object} opts Initialization options.
         */
        init: function (opts = {}) {
            if (isInitialized) return;

            config.endpoint = opts.endpoint || config.endpoint;
            config.userId = opts.userId || null;
            config.batchIntervalMs = opts.batchIntervalMs || config.batchIntervalMs;
            config.debug = !!opts.debug;
            config.sessionId = generateUUID();

            log('Initializing with config:', config);

            setupErrorTracking();
            setupWebVitals();
            setupInteractionTracking();
            startBatchTimer();

            isInitialized = true;
            log('SDK successfully initialized.');
        },

        /**
         * Manually track a custom user event.
         * @param {string} eventName Name of the event.
         * @param {Object} payload Event details.
         */
        track: function (eventName, payload = {}) {
            if (!isInitialized) {
                console.error('[CognitiveSDK] Call init() before tracking events.');
                return;
            }
            queueUserEvent(eventName, payload);
        },

        /**
         * Associate the session with a specific user ID.
         * @param {string} userId The logged-in user identifier.
         */
        identify: function (userId) {
            config.userId = userId;
            log('Identified user:', userId);
        },

        /**
         * Force flush all queued events immediately.
         */
        flush: function () {
            flushBatch();
        }
    };
}));
