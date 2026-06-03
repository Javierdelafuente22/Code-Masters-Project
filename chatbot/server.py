"""
Local Flask server that connects the chatbot to the frontend.
Run from the project root: python chatbot/server.py
"""
import os
import sys
import json
from multiprocessing import Process

from flask import Flask, request, jsonify, send_from_directory, abort

# Let this file import its sibling modules when run directly
HERE = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(HERE)
sys.path.insert(0, HERE)

# Data paths in chatbot_data.py are relative to the project root
os.chdir(PROJECT_ROOT)

from chatbot_API import EnergyChatbot
from chatbot_data import apply_lifestyle_update, get_plot_window, plot_demand_comparison


app = Flask(__name__)

# Start the chatbot once at startup. If the API key is missing the server still
# runs, but /health reports the error so the frontend can fall back.
try:
    AGENT = EnergyChatbot()
    AGENT_ERROR = None
except Exception as e:
    AGENT = None
    AGENT_ERROR = str(e)
    print(f"[server] EnergyChatbot init failed: {e}")


# Allow the frontend to call this server from the browser
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
    """Applies the change and draws the plot; runs in a subprocess so it doesn't block the server."""
    try:
        df = apply_lifestyle_update(json_payload=payload)
        if df is None:
            return
        # Pass on the year_shift so the plot shows the requested year
        window = get_plot_window(df, payload)
        plot_demand_comparison(
            window,
            payload.get('category', ''),
            year_shift=int(payload.get('year_shift', 0)),
        )
    except Exception as e:
        print(f"[server:_run_apply_and_plot] {e}")


# Serve the frontend files from this server so the browser loads them
# from the same origin and avoids CORS problems.
_STATIC_ROOTS = {'app', 'components', 'data', 'vendor'}


@app.get('/')
def index():
    return send_from_directory(PROJECT_ROOT, 'index.html')


@app.get('/<path:filename>')
def static_passthrough(filename):
    # Allow files at the root and inside the known static folders; block the rest
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

    # Run the plot in a child process so this request returns straight away
    p = Process(target=_run_apply_and_plot, args=(payload,))
    p.daemon = True
    p.start()
    return jsonify({'ok': True})


if __name__ == '__main__':
    # threaded=True so the frontend's parallel file requests don't queue up
    print("[server] Listening on http://localhost:5000")
    app.run(host='127.0.0.1', port=5000, debug=False, threaded=True, use_reloader=False)
