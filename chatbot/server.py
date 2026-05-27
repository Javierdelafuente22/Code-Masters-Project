"""
Local Flask server that exposes the Gemini-backed chatbot to the frontend.

Run from the project root:
    python chatbot/server.py

The frontend probes GET /health on mount. If reachable, it uses this server;
otherwise it falls back to the hard-coded chat logic in app/AssistantTab.jsx.

Requires: flask  (pip install flask)
"""
import os
import sys
import json
from multiprocessing import Process

from flask import Flask, request, jsonify, send_from_directory, abort

# Make sibling modules (chatbot_API, chatbot_data) importable when this file
# is run directly with `python chatbot/server.py`.
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

# All data paths in chatbot_data.py are relative to the project root.
os.chdir(PROJECT_ROOT)

from chatbot_API import EnergyChatbot
from chatbot_data import apply_lifestyle_update, get_plot_window, plot_demand_comparison


app = Flask(__name__)

# Initialise the agent once at startup. If the key is missing the server still
# starts but /health returns a non-OK status so the frontend falls back.
try:
    AGENT = EnergyChatbot()
    AGENT_ERROR = None
except Exception as e:
    AGENT = None
    AGENT_ERROR = str(e)
    print(f"[server] EnergyChatbot init failed: {e}")


# ---------- CORS (allow the static frontend to call us) ----------
@app.after_request
def add_cors(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    resp.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return resp


@app.route('/<path:_>', methods=['OPTIONS'])
@app.route('/', methods=['OPTIONS'])
def cors_preflight(_=None):
    return ('', 204)


# ---------- Endpoints ----------
@app.get('/health')
def health():
    if AGENT is None:
        return jsonify({'ok': False, 'error': AGENT_ERROR}), 503
    return jsonify({'ok': True, 'model': AGENT.model_name})


@app.post('/chat')
def chat():
    if AGENT is None:
        return jsonify({'ok': False, 'error': AGENT_ERROR}), 503
    data = request.get_json(silent=True) or {}
    text = (data.get('text') or '').strip()
    if not text:
        return jsonify({'ok': False, 'error': 'Empty input'}), 400
    result = AGENT.get_chat_response(user_input=text)
    return jsonify({'ok': True, 'result': result})


def _run_apply_and_plot(payload):
    """Runs in a subprocess so plt.show() blocks there instead of the server."""
    try:
        df = apply_lifestyle_update(json_payload=payload)
        if df is None:
            return
        # apply_lifestyle_update may have stashed a year_shift on the payload
        # (when the LLM produced dates beyond 31/12/2025). Forward it so the
        # plot's x-axis shows the originally requested year.
        window = get_plot_window(df, payload)
        plot_demand_comparison(
            window,
            payload.get('category', ''),
            year_shift=int(payload.get('year_shift', 0)),
        )
    except Exception as e:
        print(f"[server:_run_apply_and_plot] {e}")


# ---------- Static frontend ----------
# Serving the JSX/CSS/HTML from the same origin avoids the file:// CORS issues
# Chromium browsers hit when Babel tries to XHR sibling .jsx files. The frontend
# lives at http://localhost:5000/ and is otherwise unchanged from GitHub Pages.
_STATIC_ROOTS = {'app', 'components', 'data', 'vendor'}


@app.get('/')
def index():
    return send_from_directory(PROJECT_ROOT, 'index.html')


@app.get('/<path:filename>')
def static_passthrough(filename):
    # Allow tokens.css, logo.png etc. at the root, and anything under our known
    # static folders. Refuse everything else so the API endpoints stay distinct.
    top = filename.split('/', 1)[0]
    if top in _STATIC_ROOTS or '/' not in filename:
        return send_from_directory(PROJECT_ROOT, filename)
    abort(404)


@app.post('/apply')
def apply():
    data = request.get_json(silent=True) or {}
    payload = data.get('payload') or {}
    if not payload or 'modification' not in payload or 'timing' not in payload:
        return jsonify({'ok': False, 'error': 'Invalid payload'}), 400

    # Spawn the plot in a child process so this HTTP call returns immediately
    # and the frontend can show the "Done" message without waiting on the
    # matplotlib window being closed.
    p = Process(target=_run_apply_and_plot, args=(payload,))
    p.daemon = True
    p.start()
    return jsonify({'ok': True})


if __name__ == '__main__':
    # threaded=True so the frontend's ~20 parallel JSX fetches don't serialise
    # behind each other. Plot generation already runs in its own subprocess.
    print("[server] Listening on http://localhost:5000")
    app.run(host='127.0.0.1', port=5000, debug=False, threaded=True, use_reloader=False)
