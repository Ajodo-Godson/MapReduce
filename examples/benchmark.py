#!/usr/bin/env python3
"""
MapReduce Cluster Benchmark Script

This script runs actual MapReduce jobs on the Docker cluster and measures:
1. Job completion time with 1 worker
2. Job completion time with 3 workers  
3. Recovery time when a worker fails mid-job

Prerequisites:
- Docker Desktop running
- Run from project root directory

Usage:
    python3 examples/benchmark.py
    python3 examples/benchmark.py --test 1worker
    python3 examples/benchmark.py --test 3workers
    python3 examples/benchmark.py --test failure
    
Note: Make sure Docker Desktop is running before executing this script.
"""

import subprocess
import time
import re
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path


class ClusterBenchmark:
    def __init__(self, project_root=None):
        self.project_root = project_root or Path(__file__).parent.parent
        os.chdir(self.project_root)
        self.results = {}
        
    def check_docker(self):
        """Check if Docker is running."""
        result = subprocess.run(
            "docker ps", shell=True, capture_output=True, text=True, timeout=10
        )
        if result.returncode != 0:
            print("ERROR: Docker is not running!")
            print("Please start Docker Desktop and try again.")
            print(f"\nDocker error: {result.stderr}")
            return False
        return True
        
    def run_command(self, cmd, capture=True, timeout=120):
        """Run a shell command and return output."""
        try:
            if capture:
                result = subprocess.run(
                    cmd, shell=True, capture_output=True, 
                    text=True, timeout=timeout, cwd=self.project_root
                )
                return result.stdout + result.stderr
            else:
                subprocess.run(cmd, shell=True, timeout=timeout, cwd=self.project_root)
                return ""
        except subprocess.TimeoutExpired:
            return "TIMEOUT"
        except Exception as e:
            return f"ERROR: {e}"
    
    def stop_cluster(self):
        """Stop any running cluster."""
        print("  Stopping existing cluster...")
        self.run_command("docker-compose down 2>/dev/null", timeout=30)
        time.sleep(2)
    
    def start_cluster(self, workers=3):
        """Start cluster with specified number of workers."""
        print(f"  Starting cluster with {workers} worker(s)...")
        
        if workers == 1:
            # Only start master and worker1
            self.run_command("docker-compose up -d master worker1", timeout=60)
        elif workers == 2:
            self.run_command("docker-compose up -d master worker1 worker2", timeout=60)
        else:
            self.run_command("docker-compose up -d", timeout=60)
        
        # Wait for services to be ready and workers to register
        time.sleep(8)  # Give time for sleep(3) in workers + registration
        
    def wait_for_job_completion(self, timeout=180):
        """Monitor logs and wait for job to complete. Returns elapsed time."""
        start_time = time.time()
        job_complete = False
        last_check = start_time
        workers_registered = False
        
        print("  Waiting for job completion...")
        
        while time.time() - start_time < timeout:
            # Check master logs for completion
            logs = self.run_command("docker-compose logs master 2>&1", timeout=15)
            
            # First wait for workers to register
            if not workers_registered:
                if "Worker registered" in logs:
                    workers_registered = True
                    print("  Workers registered, job running...")
                else:
                    time.sleep(1)
                    continue
            
            if "MAPREDUCE JOB COMPLETE" in logs:
                job_complete = True
                break
            
            # Also check for all reduce tasks completed
            if "reduce_0 completed" in logs and "reduce_1 completed" in logs and "reduce_2 completed" in logs:
                job_complete = True
                break
            
            # Print progress every 10 seconds
            if time.time() - last_check > 10:
                # Get task counts from logs
                map_done = len(re.findall(r"Task map_\d+ completed", logs))
                reduce_done = len(re.findall(r"Task reduce_\d+ completed", logs))
                elapsed = time.time() - start_time
                print(f"    [{elapsed:.0f}s] Map tasks done: {map_done}, Reduce tasks done: {reduce_done}")
                last_check = time.time()
            
            time.sleep(1)
        
        if not job_complete:
            print(f"  WARNING: Job did not complete within {timeout}s timeout")
            return -1
        
        # Extract actual timing from logs
        elapsed = self.extract_job_duration_from_logs()
        if elapsed < 0:
            elapsed = time.time() - start_time
        
        return elapsed
    
    def extract_job_duration_from_logs(self):
        """Extract job duration from log timestamps."""
        logs = self.run_command("docker-compose logs master 2>&1", timeout=15)
        
        # Find first worker registration and last reduce completion
        reg_pattern = r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\] Worker registered'
        reduce_pattern = r'\[(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+)\] Task reduce_\d+ completed'
        
        reg_matches = re.findall(reg_pattern, logs)
        reduce_matches = re.findall(reduce_pattern, logs)
        
        if reg_matches and reduce_matches:
            try:
                from datetime import datetime as dt
                start = dt.strptime(reg_matches[0], "%Y-%m-%d %H:%M:%S.%f")
                end = dt.strptime(reduce_matches[-1], "%Y-%m-%d %H:%M:%S.%f")
                return (end - start).total_seconds()
            except Exception as e:
                print(f"  Warning: Could not parse timestamps: {e}")
                return -1
        return -1
    
    def kill_worker(self, worker_name="worker2", delay=5):
        """Kill a worker after a delay."""
        print(f"  Killing {worker_name} in {delay}s...")
        time.sleep(delay)
        self.run_command(f"docker-compose kill {worker_name}", timeout=10)
        print(f"  {worker_name} killed!")
    
    def get_final_logs(self):
        """Get the final state of cluster logs."""
        return self.run_command("docker-compose logs master 2>&1", timeout=30)
    
    def count_workers_registered(self, logs):
        """Count how many workers registered."""
        matches = re.findall(r"Worker registered: (\w+)", logs)
        return len(set(matches))
    
    def benchmark_1_worker(self):
        """Benchmark with 1 worker."""
        print("\n" + "=" * 60)
        print("BENCHMARK: 1 Worker")
        print("=" * 60)
        
        self.stop_cluster()
        self.start_cluster(workers=1)
        
        elapsed = self.wait_for_job_completion(timeout=120)
        
        logs = self.get_final_logs()
        workers = self.count_workers_registered(logs)
        
        self.stop_cluster()
        
        self.results['1_worker'] = {
            'elapsed_seconds': elapsed,
            'workers_used': workers,
            'status': 'success' if elapsed > 0 else 'timeout'
        }
        
        print(f"\n  Result: {elapsed:.2f}s with {workers} worker(s)")
        return elapsed
    
    def benchmark_3_workers(self):
        """Benchmark with 3 workers."""
        print("\n" + "=" * 60)
        print("BENCHMARK: 3 Workers")
        print("=" * 60)
        
        self.stop_cluster()
        self.start_cluster(workers=3)
        
        elapsed = self.wait_for_job_completion(timeout=120)
        
        logs = self.get_final_logs()
        workers = self.count_workers_registered(logs)
        
        self.stop_cluster()
        
        self.results['3_workers'] = {
            'elapsed_seconds': elapsed,
            'workers_used': workers,
            'status': 'success' if elapsed > 0 else 'timeout'
        }
        
        print(f"\n  Result: {elapsed:.2f}s with {workers} worker(s)")
        return elapsed
    
    def benchmark_worker_failure(self):
        """Benchmark with worker failure mid-job."""
        print("\n" + "=" * 60)
        print("BENCHMARK: Worker Failure Recovery")
        print("=" * 60)
        
        self.stop_cluster()
        self.start_cluster(workers=3)
        
        # Start a thread to kill worker2 after 8 seconds
        import threading
        kill_thread = threading.Thread(target=self.kill_worker, args=("worker2", 8))
        kill_thread.start()
        
        elapsed = self.wait_for_job_completion(timeout=180)
        kill_thread.join()
        
        logs = self.get_final_logs()
        
        # Check for failure detection
        failure_detected = "FAILED" in logs or "marked as FAILED" in logs or "missed heartbeat" in logs.lower()
        task_reassigned = "reassign" in logs.lower() or "Reassign" in logs
        
        self.stop_cluster()
        
        self.results['worker_failure'] = {
            'elapsed_seconds': elapsed,
            'failure_detected': failure_detected,
            'tasks_reassigned': task_reassigned,
            'status': 'success' if elapsed > 0 else 'timeout'
        }
        
        print(f"\n  Result: {elapsed:.2f}s")
        print(f"  Failure detected: {failure_detected}")
        print(f"  Tasks reassigned: {task_reassigned}")
        return elapsed
    
    def run_all_benchmarks(self):
        """Run all benchmarks and print summary."""
        print("\n" + "#" * 60)
        print("#" + " " * 15 + "MAPREDUCE BENCHMARKS" + " " * 23 + "#")
        print("#" * 60)
        print(f"\nStarted at: {datetime.now()}")
        print(f"Input: data/input/calgary_combined.txt (~1.7MB, 42K lines)")
        
        # Check Docker first
        if not self.check_docker():
            return None
        
        # Run benchmarks
        time_1w = self.benchmark_1_worker()
        time_3w = self.benchmark_3_workers()
        time_fail = self.benchmark_worker_failure()
        
        # Print summary
        print("\n" + "=" * 60)
        print("BENCHMARK RESULTS SUMMARY")
        print("=" * 60)
        
        print("\n| Configuration                  | Time (s) | Notes                    |")
        print("|--------------------------------|----------|--------------------------|")
        
        if time_1w > 0:
            print(f"| 1 worker                       | {time_1w:>7.2f}s | Baseline (sequential)    |")
        else:
            print(f"| 1 worker                       |  TIMEOUT | Check logs               |")
        
        if time_3w > 0:
            speedup = time_1w / time_3w if time_1w > 0 else 0
            print(f"| 3 workers                      | {time_3w:>7.2f}s | {speedup:.2f}x speedup             |")
        else:
            print(f"| 3 workers                      |  TIMEOUT | Check logs               |")
        
        if time_fail > 0:
            overhead = time_fail - time_3w if time_3w > 0 else 0
            print(f"| 3 workers (1 killed mid-job)   | {time_fail:>7.2f}s | +{overhead:.2f}s recovery overhead |")
        else:
            print(f"| 3 workers (1 killed mid-job)   |  TIMEOUT | Check logs               |")
        
        print("\nCompleted at:", datetime.now())
        
        # Save results to file
        results_file = self.project_root / "benchmark_results.txt"
        with open(results_file, 'w') as f:
            f.write(f"MapReduce Benchmark Results\n")
            f.write(f"Generated: {datetime.now()}\n\n")
            for name, data in self.results.items():
                f.write(f"{name}: {data}\n")
        
        print(f"\nResults saved to: {results_file}")
        
        return self.results


def main():
    parser = argparse.ArgumentParser(description='MapReduce Cluster Benchmarks')
    parser.add_argument('--test', choices=['1worker', '3workers', 'failure', 'all'],
                        default='all', help='Which benchmark to run')
    args = parser.parse_args()
    
    benchmark = ClusterBenchmark()
    
    # Check Docker first
    if not benchmark.check_docker():
        sys.exit(1)
    
    if args.test == '1worker':
        benchmark.benchmark_1_worker()
    elif args.test == '3workers':
        benchmark.benchmark_3_workers()
    elif args.test == 'failure':
        benchmark.benchmark_worker_failure()
    else:
        benchmark.run_all_benchmarks()


if __name__ == "__main__":
    main()
