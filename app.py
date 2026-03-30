# CST8916 – Week 10 Lab: Clickstream Analytics with Azure Event Hubs
#
# This Flask app has two roles:
#   1. PRODUCER  – receives click events from the browser and sends them to Azure Event Hubs
#   2. CONSUMER  – reads the last N events from Event Hubs and serves a live dashboard
#
# Routes:
#   GET  /              → serves the demo e-commerce store (client.html)
#   POST /track         → receives a click event and publishes it to Event Hubs
#   GET  /dashboard     → serves the live analytics dashboard (dashboard.html)
#   GET  /api/events    → returns recent events as JSON (polled by the dashboard)

import os
import json
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, request, send_from_directory, abort
from flask_cors import CORS

# ---------------------------------------------------------------------------
# Azure Event Hubs SDK
# EventHubProducerClient  – sends events to Event Hubs
# EventHubConsumerClient  – reads events from Event Hubs
# EventData               – wraps a single event payload
# ---------------------------------------------------------------------------
from azure.eventhub import EventHubProducerClient, EventHubConsumerClient, EventData

app = Flask(__name__, static_folder="static", template_folder="templates")
CORS(app)

# ---------------------------------------------------------------------------
# Configuration – read from environment variables so secrets never live in code
#
# Set these before running locally:
#   export EVENT_HUB_CONNECTION_STR="Endpoint=sb://..."
#   export EVENT_HUB_NAME="clickstream"
#
# On Azure App Service, set them as Application Settings in the portal.
# ---------------------------------------------------------------------------
CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR", "")
CLICK_EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME", "clickstream")
DEVICE_EVENT_HUB_NAME = os.environ.get("DEVICE_EVENT_HUB_NAME", "stream-analytics-results")
TRAFFIC_EVENT_HUB_NAME = os.environ.get("TRAFFIC_EVENT_HUB_NAME", "stream-analytics-spikes")
TRAFFIC_SPIKE_THRESHOLD = int(os.environ.get("TRAFFIC_SPIKE_THRESHOLD", "20"))

# In-memory buffer: stores the last 50 events received by the consumer thread.
# In a production system you would query a database or Azure Stream Analytics output.
_event_buffer = []
_device_event_buffer = []
_traffic_event_buffer = []
_buffer_lock = threading.Lock()
_consumer_start_lock = threading.Lock()
_consumers_started = False
MAX_BUFFER = 50


# ---------------------------------------------------------------------------
# Helper – send a single event dict to Azure Event Hubs
# ---------------------------------------------------------------------------
def send_to_event_hubs(event_dict: dict):
    """Serialize event_dict to JSON and publish it to Event Hubs."""
    if not CONNECTION_STR:
        # Gracefully skip if the connection string is not configured yet
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – skipping Event Hubs publish")
        return

    # EventHubProducerClient is created fresh per request here for simplicity.
    # In a high-throughput production app you would keep a shared client instance.
    producer = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        eventhub_name=CLICK_EVENT_HUB_NAME,
    )
    with producer:
        # create_batch() lets the SDK manage event size limits automatically
        event_batch = producer.create_batch()
        event_batch.add(EventData(json.dumps(event_dict)))
        producer.send_batch(event_batch)


# ---------------------------------------------------------------------------
# Background consumer thread – reads events from Event Hubs and buffers them
# ---------------------------------------------------------------------------
def _on_event(partition_context, event):
    """Callback invoked by the consumer client for each incoming event."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _buffer_lock:
        _event_buffer.append(data)
        # Keep the buffer at MAX_BUFFER entries (drop the oldest)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    # Acknowledge the event so Event Hubs advances the consumer offset
    partition_context.update_checkpoint(event)


def _on_device_event(partition_context, event):
    """Callback invoked for device-type breakdown events."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _buffer_lock:
        _device_event_buffer.append(data)
        if len(_device_event_buffer) > MAX_BUFFER:
            _device_event_buffer.pop(0)

    partition_context.update_checkpoint(event)


def _on_traffic_event(partition_context, event):
    """Callback invoked for traffic spike detection events."""
    body = event.body_as_str(encoding="UTF-8")
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        data = {"raw": body}

    with _buffer_lock:
        # Some jobs emit an array with one-or-more window records.
        if isinstance(data, list):
            for item in data:
                _traffic_event_buffer.append(item)
        else:
            _traffic_event_buffer.append(data)

        while len(_traffic_event_buffer) > MAX_BUFFER:
            _traffic_event_buffer.pop(0)

    partition_context.update_checkpoint(event)


def _start_consumer_for_hub(eventhub_name, on_event_callback, stream_label):
    """Start an Event Hubs consumer thread for a single hub."""
    if not CONNECTION_STR:
        app.logger.warning("EVENT_HUB_CONNECTION_STR is not set – %s consumer not started", stream_label)
        return

    consumer = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group="$Default",
        eventhub_name=eventhub_name,
    )

    def run():
        with consumer:
            consumer.receive(
                on_event=on_event_callback,
                starting_position="-1",
            )

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    app.logger.info("Event Hubs consumer thread started for %s (%s)", stream_label, eventhub_name)


def _build_device_breakdown(device_events):
    """Aggregate latest device-type window events into a compact summary."""
    latest_window_end = None
    breakdown = {}

    # Iterate newest-to-oldest so we can stop at the latest complete window.
    for item in reversed(device_events):
        if not isinstance(item, dict):
            continue

        device_type = item.get("deviceType")
        event_count = item.get("eventCount")
        window_end = item.get("windowEnd")
        if not device_type or window_end is None or event_count is None:
            continue

        if latest_window_end is None:
            latest_window_end = window_end
        if window_end != latest_window_end:
            continue

        try:
            count = int(event_count)
        except (TypeError, ValueError):
            continue

        breakdown[device_type] = breakdown.get(device_type, 0) + count

    return {
        "windowEnd": latest_window_end,
        "breakdown": breakdown,
        "total": sum(breakdown.values()),
    }


def _build_traffic_spike_summary(traffic_events):
    """Aggregate latest traffic window and evaluate spike status."""
    latest_window_end = None
    latest_count = 0

    for item in reversed(traffic_events):
        if not isinstance(item, dict):
            continue

        event_count = item.get("eventCount")
        window_end = item.get("windowEnd")
        if window_end is None or event_count is None:
            continue

        try:
            count = int(event_count)
        except (TypeError, ValueError):
            continue

        latest_window_end = window_end
        latest_count = count
        break

    return {
        "windowEnd": latest_window_end,
        "eventCount": latest_count,
        "threshold": TRAFFIC_SPIKE_THRESHOLD,
        "isSpike": latest_count >= TRAFFIC_SPIKE_THRESHOLD,
    }


def start_consumers():
    """Start Event Hubs consumers in background daemon threads.

    The consumer must run on a separate thread because consumer.receive() blocks
    forever waiting for events. Running it on the main thread would freeze Flask
    and make the web server unable to handle any HTTP requests.
    """
    _start_consumer_for_hub(CLICK_EVENT_HUB_NAME, _on_event, "clickstream")
    _start_consumer_for_hub(DEVICE_EVENT_HUB_NAME, _on_device_event, "device-type")
    _start_consumer_for_hub(TRAFFIC_EVENT_HUB_NAME, _on_traffic_event, "traffic-spike")


def ensure_consumers_started():
    """Start consumers once per worker process (works for Gunicorn/App Service)."""
    global _consumers_started
    if _consumers_started:
        return

    with _consumer_start_lock:
        if _consumers_started:
            return
        app.logger.info("Starting Event Hubs consumers in worker pid=%s", os.getpid())
        start_consumers()
        _consumers_started = True
        app.logger.info("Event Hubs consumers ready in worker pid=%s", os.getpid())


@app.before_request
def _bootstrap_background_consumers():
    """Ensure consumers are running before serving app routes."""
    ensure_consumers_started()


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    """Serve the demo e-commerce store."""
    return send_from_directory("templates", "client.html")


@app.route("/dashboard")
def dashboard():
    """Serve the live analytics dashboard."""
    return send_from_directory("templates", "dashboard.html")


@app.route("/health", methods=["GET"])
def health():
    """Health check – used by Azure App Service to verify the app is running."""
    return jsonify({"status": "healthy"}), 200


@app.route("/api/consumer-status", methods=["GET"])
def consumer_status():
    """Diagnostic status for background consumers in the current worker."""
    with _buffer_lock:
        click_count = len(_event_buffer)
        device_count = len(_device_event_buffer)
        traffic_count = len(_traffic_event_buffer)

    return jsonify(
        {
            "pid": os.getpid(),
            "consumers_started": _consumers_started,
            "buffers": {
                "clickstream": click_count,
                "device": device_count,
                "traffic": traffic_count,
            },
            "hubs": {
                "clickstream": CLICK_EVENT_HUB_NAME,
                "device": DEVICE_EVENT_HUB_NAME,
                "traffic": TRAFFIC_EVENT_HUB_NAME,
            },
        }
    ), 200


@app.route("/track", methods=["POST"])
def track():
    """
    Receive a click event from the browser and publish it to Event Hubs.

    Expected JSON body:
    {
        "event_type": "page_view" | "product_click" | "add_to_cart" | "purchase",
        "page":       "/products/shoes",
        "product_id": "p_shoe_42",       (optional)
        "user_id":    "u_1234"
    }
    """
    if not request.json:
        abort(400)

    # Enrich the event with a server-side timestamp
    event = {
        "event_type": request.json.get("event_type", "unknown"),
        "page":       request.json.get("page", "/"),
        "product_id": request.json.get("product_id"),
        "user_id":    request.json.get("user_id", "anonymous"),
        "timestamp":  datetime.now(timezone.utc).isoformat(),
        "deviceType": determine_device_type(
            request.headers.get("User-Agent") or "",
            request.headers.get("Sec-CH-UA-Mobile") or ""
        ),
        "browser":    request.headers.get("Sec-CH-UA") or request.user_agent.browser or "unknown",
        "os":         request.headers.get("Sec-CH-UA-Platform") or request.user_agent.platform or "unknown",
    }

    send_to_event_hubs(event)

    # Also buffer locally so the dashboard works even without a consumer thread
    with _buffer_lock:
        _event_buffer.append(event)
        if len(_event_buffer) > MAX_BUFFER:
            _event_buffer.pop(0)

    return jsonify({"status": "ok", "event": event}), 201


@app.route("/api/events", methods=["GET"])
def get_events():
    """
    Return the buffered events as JSON.
    The dashboard polls this endpoint every 2 seconds.

    Optional query param:  ?limit=20  (default 20, max 50)
    """
    try:
        # request.args.get("limit", 20) reads ?limit=N from the URL, defaulting to 20.
        # int() converts it from a string (all URL params are strings) to an integer.
        # min(..., MAX_BUFFER) clamps the value so callers can never request more
        # events than the buffer holds — e.g. ?limit=999 silently becomes 50.
        limit = min(int(request.args.get("limit", 20)), MAX_BUFFER)
    except ValueError:
        # int() raises ValueError if the param is non-numeric (e.g. ?limit=abc)
        limit = 20

    with _buffer_lock:
        recent = list(_event_buffer[-limit:])
        recent_device = list(_device_event_buffer[-limit:])
        recent_traffic = list(_traffic_event_buffer[-limit:])

    # Build a simple summary for the dashboard
    summary = {}
    for e in recent:
        et = e.get("event_type", "unknown")
        summary[et] = summary.get(et, 0) + 1

    return jsonify(
        {
            "events": recent,
            "summary": summary,
            "total": len(recent),
            "device_events": recent_device,
            "traffic_events": recent_traffic,
        }
    ), 200


@app.route("/api/insights", methods=["GET"])
def get_insights():
    """Return latest device breakdown and traffic-spike streams."""
    try:
        limit = min(int(request.args.get("limit", 20)), MAX_BUFFER)
    except ValueError:
        limit = 20

    with _buffer_lock:
        recent_device = list(_device_event_buffer[-limit:])
        recent_traffic = list(_traffic_event_buffer[-limit:])

    device_breakdown = _build_device_breakdown(recent_device)
    traffic_spike = _build_traffic_spike_summary(recent_traffic)

    return jsonify(
        {
            "device_events": recent_device,
            "traffic_events": recent_traffic,
            "device_total": len(recent_device),
            "traffic_total": len(recent_traffic),
            "device_breakdown": device_breakdown,
            "traffic_spike": traffic_spike,
        }
    ), 200


def determine_device_type(ua, ch_mobile):
    if ch_mobile in ("?1", "1", "true"):
        device_type = "mobile"
    elif any(t in ua for t in ("ipad", "tablet", "kindle", "silk", "playbook")):
        device_type = "tablet"
    else:
        device_type = "desktop"
    return device_type


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    # Start the background consumer so the dashboard receives live events
    ensure_consumers_started()
    # Run on 0.0.0.0 so it is reachable both locally and inside Azure App Service
    app.run(debug=False, host="0.0.0.0", port=8000)
