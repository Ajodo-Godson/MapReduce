#!/usr/bin/env python3
"""
Live monitoring dashboard for MapReduce cluster
Displays real-time system state by parsing Docker logs
"""

import subprocess
import time
import re
import os
from datetime import datetime
from collections import defaultdict


class ClusterMonitor:
    def __init__(self):
        self.workers = {}
        self.tasks = defaultdict(lambda: {'pending': 0, 'running': 0, 'completed': 0, 'failed': 0})
        
    def clear_screen(self):
        """Clear terminal screen"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def parse_logs(self):
        """Parse Docker logs to extract system state"""
        try:
            # Get master logs
            result = subprocess.run(
                ['docker-compose', 'logs', '--tail=100', 'master'],
                capture_output=True,
                text=True,
                timeout=5
            )
            
            logs = result.stdout

            # Build fresh worker map for this parse to avoid acumulation
            local_workers = {}
            
            # Parse worker registrations
            worker_pattern = r'Worker registered: (\w+)'
            for match in re.finditer(worker_pattern, logs):
                worker_id = match.group(1)
                local_workers[worker_id] = {'status': 'registered', 'tasks': 0, 'failed': False}
                
            # if worker_id not in self.workers:
            #     self.workers[worker_id] = {'status': 'registered', 'tasks': 0, 'failed': False}
            
            # Parse worker failures
            failure_pattern = r'Worker (\w+) marked as FAILED'
            for match in re.finditer(failure_pattern, logs):
                worker_id = match.group(1)
                if worker_id not in local_workers:
                    local_workers[worker_id] = {'status': 'FAILED', 'tasks': 0, 'failed': True}
                else:
                    local_workers[worker_id]['status'] = 'FAILED'
                    local_workers[worker_id]['failed'] = True
                # if worker_id in self.workers:
                #     self.workers[worker_id]['status'] = 'FAILED'
                #     self.workers[worker_id]['failed'] = True
            
            # Parse task completions
            complete_pattern = r'Task (\w+) completed by (\w+)'
            for match in re.finditer(complete_pattern, logs):
                task_id, worker_id = match.groups()
                # if worker_id in self.workers:
                #     self.workers[worker_id]['tasks'] += 1
                if worker_id not in local_workers:
                    local_workers[worker_id] = {'status': 'registered', 'tasks': 1, 'failed': False}
                local_workers[worker_id]['tasks'] += 1
            
            # Parse status updates (most recent)
            status_pattern = r'(Pending|Running|Completed|Failed): (\d+)'
            status_lines = logs.split('SYSTEM STATUS')[-1] if 'SYSTEM STATUS' in logs else ''
            overall = {}
            
            for match in re.finditer(status_pattern, status_lines):
                status_type, count = match.groups()
                overall[status_type.lower()] = int(count)
            
            self.tasks['overall'] = overall
            
            # Replace persistent worker map with fresh snapshot
            self.workers = local_workers

        except subprocess.TimeoutExpired:
            pass
        except Exception as e:
            print(f"Error parsing logs: {e}")
    
    def display(self):
        """Display the monitoring dashboard"""
        self.clear_screen()
        
        print("╔" + "═" * 78 + "╗")
        print("║" + " " * 20 + "MapReduce Cluster Monitor" + " " * 33 + "║")
        print("║" + " " * 25 + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + " " * 28 + "║")
        print("╚" + "═" * 78 + "╝")
        print()
        
        # Worker Status
        print("┌─ WORKERS " + "─" * 68 + "┐")
        
        if not self.workers:
            print("│  No workers registered yet..." + " " * 47 + "│")
        else:
            for worker_id, info in sorted(self.workers.items()):
                status_icon = "💀" if info['failed'] else "✓"
                status_color = "\033[91m" if info['failed'] else "\033[92m"
                reset_color = "\033[0m"
                
                status_text = f"{status_icon} {worker_id:<15} Status: {info['status']:<12} Tasks: {info['tasks']:<3}"
                padding = 78 - len(status_text) - 3  # Account for color codes
                print(f"│  {status_color}{status_text}{reset_color}" + " " * padding + "│")
        
        print("└" + "─" * 78 + "┘")
        print()
        
        # Task Status
        print("┌─ TASKS " + "─" * 70 + "┐")
        
        overall = self.tasks.get('overall', {})
        total_tasks = sum(overall.values())
        
        if total_tasks == 0:
            print("│  No tasks created yet..." + " " * 52 + "│")
        else:
            pending = overall.get('pending', 0)
            running = overall.get('running', 0)
            completed = overall.get('completed', 0)
            failed = overall.get('failed', 0)
            
            print(f"│  Total Tasks: {total_tasks:<10}" + " " * 54 + "│")
            print("│" + " " * 78 + "│")
            print(f"│    ⏳ Pending:   {pending:<5}" + " " * 56 + "│")
            print(f"│    ⚙️  Running:   {running:<5}" + " " * 56 + "│")
            print(f"│    ✅ Completed: {completed:<5}" + " " * 56 + "│")
            print(f"│    ❌ Failed:    {failed:<5}" + " " * 56 + "│")
            
            # Progress bar
            if total_tasks > 0:
                progress = (completed / total_tasks) * 100
                bar_length = 50
                filled = int((progress / 100) * bar_length)
                bar = "█" * filled + "░" * (bar_length - filled)
                print("│" + " " * 78 + "│")
                print(f"│  Progress: [{bar}] {progress:.1f}%" + " " * (78 - 65 - len(f"{progress:.1f}%")) + "│")
        
        print("└" + "─" * 78 + "┘")
        print()
        
        # Instructions
        print("┌─ COMMANDS " + "─" * 67 + "┐")
        print("│  • View logs:       docker-compose logs -f master" + " " * 25 + "│")
        print("│  • Kill worker:     docker-compose kill worker1" + " " * 27 + "│")
        print("│  • Stop monitoring: Ctrl+C" + " " * 49 + "│")
        print("└" + "─" * 78 + "┘")
    
    def run(self, interval=3):
        """Run the monitoring loop"""
        print("Starting cluster monitor...")
        print("Press Ctrl+C to stop")
        time.sleep(2)
        
        try:
            while True:
                self.parse_logs()
                self.display()
                time.sleep(interval)
        except KeyboardInterrupt:
            print("\n\nMonitoring stopped.")
            print("Cluster is still running. Use './scripts/run_cluster.sh stop' to stop it.")


if __name__ == '__main__':
    monitor = ClusterMonitor()
    monitor.run()