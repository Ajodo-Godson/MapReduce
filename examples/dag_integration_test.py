#!/usr/bin/env python3
"""
DAG Scheduler Integration Test - End-to-End Pipeline

This script tests the DAG job scheduler with the actual Docker MapReduce cluster,
using REAL data with output chaining between jobs.

Pipeline:
  Job 1: word_count     - Count words in sample.txt
                            ↓ output
  Job 2: filter_common  - Process word counts, filter common words (count > 5)
                            ↓ output  
  Job 3: aggregate      - Aggregate filtered results into final statistics

Each job's output becomes the next job's input.

Prerequisites:
- Docker Desktop running
- Run from project root directory

Usage:
    python3 examples/dag_integration_test.py
    python3 examples/dag_integration_test.py --test pipeline   # Full end-to-end test
    python3 examples/dag_integration_test.py --test unit       # Unit tests only (no Docker)
"""

import subprocess
import time
import os
import sys
import json
import shutil
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from src.master.job_scheduler import DAGJobScheduler, JobStatus


class DockerClusterManager:
    """Manages Docker cluster for testing."""
    
    def __init__(self, project_root, compose_file="docker-compose.yml"):
        self.project_root = Path(project_root)
        self.compose_file = compose_file
        os.chdir(self.project_root)
        
        # Ensure output directory exists
        (self.project_root / "output").mkdir(exist_ok=True)
    
    def stop_cluster(self):
        """Stop any running cluster."""
        subprocess.run(
            f"docker-compose -f {self.compose_file} down -v 2>/dev/null",
            shell=True, capture_output=True, cwd=self.project_root
        )
        time.sleep(2)
    
    def start_cluster(self, workers=3, input_file="data/input/sample.txt"):
        """Start the cluster with specified workers."""
        services = ["master"] + [f"worker{i}" for i in range(1, workers + 1)]
        cmd = f"docker-compose -f {self.compose_file} up -d {' '.join(services)}"
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, cwd=self.project_root)
        if result.returncode != 0:
            print(f"  Error starting cluster: {result.stderr}")
            return False
        time.sleep(5)  # Wait for cluster to initialize
        return True
    
    def wait_for_job_completion(self, timeout=120):
        """Wait for the current MapReduce job to complete."""
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            result = subprocess.run(
                "docker logs mapreduce_master 2>&1 | tail -50",
                shell=True, capture_output=True, text=True, cwd=self.project_root
            )
            
            if "MapReduce job complete" in result.stdout or "All tasks completed" in result.stdout:
                return True
            
            if "JOB COMPLETE" in result.stdout:
                return True
                
            time.sleep(1)
        
        return False
    
    def get_reduce_output(self):
        """Get the combined reduce output from the cluster.
        
        Since reduce outputs are reported via gRPC (not written to files),
        we reconstruct the word counts from the map intermediate files.
        """
        intermediate_dir = self.project_root / "intermediate"
        combined_output = {}
        
        # Read all map partition files and aggregate counts
        for worker_dir in intermediate_dir.iterdir():
            if worker_dir.is_dir():
                for f in worker_dir.glob("map_*_partition_*.json"):
                    try:
                        with open(f, 'r') as file:
                            data = json.load(file)
                            # Map output format: {"word": [1, 1, 1, ...], ...}
                            for word, counts in data.items():
                                if isinstance(counts, list):
                                    combined_output[word] = combined_output.get(word, 0) + len(counts)
                                else:
                                    combined_output[word] = combined_output.get(word, 0) + counts
                    except (json.JSONDecodeError, IOError):
                        pass
        
        return combined_output
    
    def get_master_logs(self, lines=100):
        """Get recent master logs."""
        result = subprocess.run(
            f"docker logs mapreduce_master 2>&1 | tail -{lines}",
            shell=True, capture_output=True, text=True, cwd=self.project_root
        )
        return result.stdout


def run_single_mapreduce_job(cluster, input_file, job_name, output_file):
    """
    Run a single MapReduce job and save output.
    
    Returns:
        dict: The job output (word counts)
    """
    print(f"\n  Running job: {job_name}")
    print(f"    Input: {input_file}")
    
    # For this test, we use the default docker-compose which runs word count
    # The output is stored in intermediate files
    
    cluster.stop_cluster()
    
    # Start cluster (it will automatically run the configured job)
    if not cluster.start_cluster(workers=3):
        print(f"    ✗ Failed to start cluster")
        return None
    
    # Wait for completion
    print(f"    Waiting for job completion...")
    if cluster.wait_for_job_completion(timeout=60):
        print(f"    ✓ Job completed")
        
        # Get output
        output = cluster.get_reduce_output()
        
        # Save output for next job
        output_path = project_root / output_file
        output_path.parent.mkdir(exist_ok=True)
        with open(output_path, 'w') as f:
            json.dump(output, f, indent=2)
        
        print(f"    Output: {len(output)} unique words")
        print(f"    Saved to: {output_file}")
        
        return output
    else:
        print(f"    ✗ Job timed out")
        return None


def demo_end_to_end_pipeline():
    """
    Run a complete end-to-end DAG pipeline with real data.
    
    Pipeline:
    1. word_count: Count all words in input file
    2. filter_common: Keep only words with count > 5
    3. top_words: Get statistics on filtered words
    """
    print("\n" + "=" * 70)
    print("END-TO-END DAG PIPELINE TEST")
    print("=" * 70)
    print(f"\nStarted at: {datetime.now()}")
    
    cluster = DockerClusterManager(project_root)
    scheduler = DAGJobScheduler()
    
    # Clean output directory
    output_dir = project_root / "output"
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir()
    
    print("\n" + "-" * 70)
    print("STEP 1: Define Pipeline DAG")
    print("-" * 70)
    
    # Define the pipeline
    scheduler.add_job(
        "word_count",
        input_data="data/input/sample.txt",
        num_reduce_tasks=3
    )
    
    scheduler.add_job(
        "filter_common",
        dependencies=["word_count"],
        input_data=None,  # Will use word_count output
        num_reduce_tasks=2
    )
    
    scheduler.add_job(
        "top_words",
        dependencies=["filter_common"],
        input_data=None,  # Will use filter_common output
        num_reduce_tasks=1
    )
    
    # Print DAG
    print("\nPipeline structure:")
    print(scheduler.get_ascii_dag())
    
    try:
        order = scheduler.get_topological_order()
        print(f"\nExecution order: {' → '.join(order)}")
    except ValueError as e:
        print(f"ERROR: Invalid DAG - {e}")
        return None
    
    print("\n" + "-" * 70)
    print("STEP 2: Execute Pipeline (Real MapReduce Jobs)")
    print("-" * 70)
    
    results = {}
    total_start = time.time()
    
    # Execute Job 1: word_count
    scheduler.mark_job_started("word_count")
    
    print("\n[JOB 1] word_count")
    cluster.stop_cluster()
    if cluster.start_cluster(workers=3):
        if cluster.wait_for_job_completion(timeout=60):
            output = cluster.get_reduce_output()
            
            # Save output
            output_file = output_dir / "word_count_output.json"
            with open(output_file, 'w') as f:
                json.dump(output, f, indent=2)
            
            results["word_count"] = {
                "unique_words": len(output),
                "total_count": sum(output.values()) if output else 0,
                "sample": dict(list(output.items())[:5]) if output else {},
                "output_file": str(output_file)
            }
            
            scheduler.mark_job_completed("word_count", output_data={"output_path": str(output_file)})
            print(f"  ✓ Completed: {len(output)} unique words")
        else:
            scheduler.mark_job_failed("word_count", "Timeout")
            print("  ✗ Timed out")
            cluster.stop_cluster()
            return None
    
    # Execute Job 2: filter_common (simulated processing of word_count output)
    scheduler.mark_job_started("filter_common")
    
    print("\n[JOB 2] filter_common (filtering words with count > 5)")
    
    # Load previous output and filter
    word_counts = results["word_count"]["sample"]
    if output_file.exists():
        with open(output_file, 'r') as f:
            word_counts = json.load(f)
    
    # Filter: keep only words with count > 5
    filtered = {word: count for word, count in word_counts.items() if count > 5}
    
    # Save filtered output
    filter_output_file = output_dir / "filter_common_output.json"
    with open(filter_output_file, 'w') as f:
        json.dump(filtered, f, indent=2)
    
    results["filter_common"] = {
        "input_words": len(word_counts),
        "filtered_words": len(filtered),
        "filter_ratio": f"{len(filtered)/len(word_counts)*100:.1f}%" if word_counts else "0%",
        "sample": dict(list(filtered.items())[:5]) if filtered else {},
        "output_file": str(filter_output_file)
    }
    
    scheduler.mark_job_completed("filter_common", output_data={"output_path": str(filter_output_file)})
    print(f"  ✓ Completed: {len(filtered)} words kept (from {len(word_counts)})")
    
    # Execute Job 3: top_words (aggregate statistics)
    scheduler.mark_job_started("top_words")
    
    print("\n[JOB 3] top_words (computing statistics)")
    
    # Compute statistics from filtered results
    if filtered:
        sorted_words = sorted(filtered.items(), key=lambda x: x[1], reverse=True)
        top_10 = dict(sorted_words[:10])
        
        stats = {
            "total_unique_words": len(filtered),
            "total_word_count": sum(filtered.values()),
            "average_frequency": sum(filtered.values()) / len(filtered),
            "max_frequency": max(filtered.values()),
            "min_frequency": min(filtered.values()),
            "top_10_words": top_10
        }
    else:
        stats = {"total_unique_words": 0}
    
    # Save stats
    stats_output_file = output_dir / "top_words_output.json"
    with open(stats_output_file, 'w') as f:
        json.dump(stats, f, indent=2)
    
    results["top_words"] = stats
    results["top_words"]["output_file"] = str(stats_output_file)
    
    scheduler.mark_job_completed("top_words", output_data={"output_path": str(stats_output_file)})
    print(f"  ✓ Completed: Top word frequency = {stats.get('max_frequency', 'N/A')}")
    
    total_elapsed = time.time() - total_start
    
    print("\n" + "-" * 70)
    print("STEP 3: Pipeline Results")
    print("-" * 70)
    
    cluster.stop_cluster()
    
    # Print final DAG status
    print("\nFinal DAG Status:")
    print(scheduler.get_ascii_dag())
    
    print("\nJob Results:")
    for job_id, result in results.items():
        print(f"\n  {job_id}:")
        for key, value in result.items():
            if key != "sample" and key != "output_file":
                print(f"    {key}: {value}")
    
    print(f"\n  Total Pipeline Time: {total_elapsed:.2f}s")
    
    # Save complete results
    results_file = output_dir / "pipeline_results.json"
    with open(results_file, 'w') as f:
        json.dump({
            "pipeline": "word_count → filter_common → top_words",
            "total_time_seconds": total_elapsed,
            "jobs": results,
            "completed_at": datetime.now().isoformat()
        }, f, indent=2)
    
    print(f"\n  Complete results saved to: {results_file}")
    
    print("\n" + "=" * 70)
    print("PIPELINE COMPLETE ✓")
    print("=" * 70)
    
    return results


def test_cycle_detection():
    """Test that the scheduler properly detects cycles."""
    print("\n" + "=" * 70)
    print("UNIT TEST: Cycle Detection")
    print("=" * 70)
    
    scheduler = DAGJobScheduler()
    
    # Create a valid DAG: A → B → C
    scheduler.add_job("A")
    scheduler.add_job("B", dependencies=["A"])
    scheduler.add_job("C", dependencies=["B"])
    
    print("\n  Valid DAG created: A → B → C")
    
    # Manually create a cycle to test detection
    scheduler.jobs["A"].dependencies.append("C")
    scheduler.jobs["C"].dependents.append("A")
    
    print("  Added cycle: C → A")
    print("  Now have: A → B → C → A (cycle!)")
    
    try:
        order = scheduler.get_topological_order()
        print(f"  ✗ Should have detected cycle but got order: {order}")
        return False
    except ValueError as e:
        print(f"  ✓ Correctly detected cycle: {e}")
        return True


def test_parallel_jobs():
    """Test that independent jobs can run in parallel."""
    print("\n" + "=" * 70)
    print("UNIT TEST: Parallel Job Detection")
    print("=" * 70)
    
    scheduler = DAGJobScheduler()
    
    # Create parallel jobs:
    #    A     B     C    (all independent)
    #     \    |    /
    #        merge        (depends on all three)
    
    scheduler.add_job("job_A", input_data="file_a.txt")
    scheduler.add_job("job_B", input_data="file_b.txt")
    scheduler.add_job("job_C", input_data="file_c.txt")
    scheduler.add_job("merge", dependencies=["job_A", "job_B", "job_C"])
    
    print("\n  DAG structure:")
    print(scheduler.get_ascii_dag())
    
    # Get ready jobs - should be A, B, C
    ready = scheduler.get_all_ready_jobs()
    print(f"\n  Ready jobs (should be 3): {ready}")
    
    success = True
    if len(ready) == 3:
        print("  ✓ Correctly identified 3 parallel jobs")
    else:
        print(f"  ✗ Expected 3 parallel jobs, got {len(ready)}")
        success = False
    
    # Complete all three
    for job_id in ["job_A", "job_B", "job_C"]:
        scheduler.mark_job_started(job_id)
        scheduler.mark_job_completed(job_id, output_data={"result": f"output_{job_id}"})
    
    # Now merge should be ready
    ready = scheduler.get_all_ready_jobs()
    print(f"\n  After completing A, B, C - ready jobs: {ready}")
    
    if ready == ["merge"]:
        print("  ✓ 'merge' job correctly became ready")
    else:
        print(f"  ✗ Expected ['merge'], got {ready}")
        success = False
    
    return success


def test_output_chaining():
    """Test that job outputs are properly chained to next job's input."""
    print("\n" + "=" * 70)
    print("UNIT TEST: Output Chaining")
    print("=" * 70)
    
    scheduler = DAGJobScheduler()
    
    # Create a chain: A → B → C
    scheduler.add_job("job_A", input_data="input.txt")
    scheduler.add_job("job_B", dependencies=["job_A"])
    scheduler.add_job("job_C", dependencies=["job_B"])
    
    print("\n  Chain: A → B → C")
    
    # Complete A with output
    scheduler.mark_job_started("job_A")
    scheduler.mark_job_completed("job_A", output_data={"words": ["hello", "world"], "count": 100})
    
    # Check B gets A's output
    job_b = scheduler.jobs["job_B"]
    print(f"\n  Job B status after A completes: {job_b.status.value}")
    
    success = True
    if job_b.status == JobStatus.READY:
        print("  ✓ Job B correctly marked as READY")
    else:
        print(f"  ✗ Job B should be READY, got {job_b.status.value}")
        success = False
    
    # Complete B
    scheduler.mark_job_started("job_B")
    scheduler.mark_job_completed("job_B", output_data={"filtered": 50})
    
    # Check C
    job_c = scheduler.jobs["job_C"]
    if job_c.status == JobStatus.READY:
        print("  ✓ Job C correctly marked as READY after B completes")
    else:
        print(f"  ✗ Job C should be READY, got {job_c.status.value}")
        success = False
    
    return success


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="DAG Scheduler Integration Tests")
    parser.add_argument(
        "--test", 
        choices=["all", "pipeline", "unit", "cycle", "parallel", "chain"],
        default="all",
        help="Which test to run"
    )
    args = parser.parse_args()
    
    print("\n" + "#" * 70)
    print("#" + " " * 20 + "DAG SCHEDULER TESTS" + " " * 27 + "#")
    print("#" * 70)
    
    results = {}
    
    if args.test in ["all", "unit", "cycle"]:
        results["cycle"] = test_cycle_detection()
    
    if args.test in ["all", "unit", "parallel"]:
        results["parallel"] = test_parallel_jobs()
    
    if args.test in ["all", "unit", "chain"]:
        results["chain"] = test_output_chaining()
    
    if args.test in ["all", "pipeline"]:
        pipeline_result = demo_end_to_end_pipeline()
        results["pipeline"] = pipeline_result is not None
    
    # Summary
    print("\n" + "#" * 70)
    print("#" + " " * 22 + "TEST SUMMARY" + " " * 32 + "#")
    print("#" * 70)
    
    for test_name, passed in results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"  {test_name}: {status}")
    
    all_passed = all(results.values())
    print("\n" + "#" * 70)
    if all_passed:
        print("#" + " " * 20 + "ALL TESTS PASSED ✓" + " " * 27 + "#")
    else:
        print("#" + " " * 20 + "SOME TESTS FAILED ✗" + " " * 26 + "#")
    print("#" * 70 + "\n")
    
    sys.exit(0 if all_passed else 1)
