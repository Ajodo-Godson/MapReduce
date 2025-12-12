"""
MapReduce Web Dashboard

Simple real-time visualization of cluster state:
- Worker status (working, idle, failed)
- Job progress through the system

Polls the master's HTTP status endpoint for data.

Usage:
    docker-compose -f docker-compose.benchmark.yml up --build
    # Dashboard at http://localhost:5000
"""

import os
import json
import time
import threading
import urllib.request
import urllib.error
from datetime import datetime
from flask import Flask, render_template, jsonify, Response

app = Flask(__name__, 
            template_folder='templates',
            static_folder='static')

# Master's HTTP status endpoint
MASTER_HTTP_URL = os.environ.get('MASTER_HTTP_URL', 'http://master:8080/status')

# Cluster state (updated by polling)
cluster_state = {
    'workers': {},
    'tasks': {
        'map': {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0, 'total': 0},
        'reduce': {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0, 'total': 0}
    },
    'events': [],
    'status': 'idle',
    'last_update': None,
    'connected': False
}

state_lock = threading.Lock()


def poll_master():
    """Poll master's HTTP status endpoint."""
    global cluster_state
    
    try:
        req = urllib.request.Request(MASTER_HTTP_URL, headers={'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=2) as response:
            data = json.loads(response.read().decode())
            
            with state_lock:
                cluster_state['workers'] = data.get('workers', {})
                cluster_state['tasks'] = data.get('tasks', cluster_state['tasks'])
                cluster_state['status'] = data.get('status', 'idle')
                cluster_state['events'] = data.get('events', [])
                cluster_state['last_update'] = datetime.now().isoformat()
                cluster_state['connected'] = True
            
    except Exception:
        with state_lock:
            cluster_state['connected'] = False


def polling_loop():
    """Background polling thread."""
    while True:
        poll_master()
        time.sleep(1)


# Start polling thread
threading.Thread(target=polling_loop, daemon=True).start()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/status')
def get_status():
    with state_lock:
        return jsonify(cluster_state)


@app.route('/api/stream')
def stream():
    """Server-Sent Events for live updates."""
    def generate():
        last_state = None
        while True:
            with state_lock:
                current = json.dumps(cluster_state)
            if current != last_state:
                yield f"data: {current}\n\n"
                last_state = current
            time.sleep(0.5)
    
    return Response(generate(), mimetype='text/event-stream')


if __name__ == '__main__':
    print("MapReduce Dashboard - http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True, threaded=True)
