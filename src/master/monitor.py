import time
from datetime import datetime


class WorkerMonitor:
    """Monitors worker health via heartbeats"""
    
    def __init__(self, state, heartbeat_timeout=10):
        self.state = state
        self.heartbeat_timeout = heartbeat_timeout  # seconds
        self.running = True
    
    def run(self):
        """Main monitoring loop"""
        print(f"[{datetime.now()}] Worker monitor started")
        
        while self.running:
            time.sleep(5)  # Check every 5 seconds
            self.check_worker_health()
    
    def check_worker_health(self):
        """Check if any workers have missed heartbeats"""
        current_time = time.time()
        
        for worker_id, worker_info in self.state.workers.items():
            last_heartbeat = worker_info.get('last_heartbeat', 0)
            time_since_heartbeat = current_time - last_heartbeat
            
            # Check if worker has timed out
            if time_since_heartbeat > self.heartbeat_timeout:
                if worker_info['status'] != 'failed':
                    print(f"[{datetime.now()}] Worker {worker_id} missed heartbeat "
                          f"(last seen {time_since_heartbeat:.1f}s ago)")
                    
                    # Mark worker as failed
                    self.state.mark_worker_failed(worker_id)
                    
                    # Reassign its tasks
                    self.state.reassign_worker_tasks(worker_id)
    
    def stop(self):
        """Stop the monitor"""
        self.running = False