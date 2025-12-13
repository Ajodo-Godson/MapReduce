"""
DAG-based Job Scheduler for MapReduce with Task Dependencies.

Supports complex workflows like:
- Job A and Job B must complete before Job C starts
- Multiple MapReduce jobs chained together
- Automatic dependency resolution using topological sort

Based on concepts from:
- Google's MapReduce paper (job coordination)
- Apache Airflow (DAG-based workflows)
- Apache Oozie (Hadoop job orchestration)
"""

import json
import threading
from datetime import datetime
from collections import defaultdict, deque
from enum import Enum
from typing import List, Dict, Optional, Any, Callable


class JobStatus(Enum):
    """Status of a job in the DAG scheduler."""
    PENDING = "pending"      # Waiting for dependencies
    READY = "ready"          # All dependencies satisfied, waiting to run
    RUNNING = "running"      # Currently executing
    COMPLETED = "completed"  # Successfully finished
    FAILED = "failed"        # Failed with error


class Job:
    """
    Represents a MapReduce job with dependencies.
    
    A job can:
    - Depend on other jobs (won't start until they complete)
    - Have dependents (jobs that wait for this one)
    - Chain output → input with dependent jobs
    """
    
    def __init__(self, job_id: str, input_data: Any = None, 
                 num_reduce_tasks: int = 3, job_type: str = "mapreduce",
                 map_func: Optional[Callable] = None,
                 reduce_func: Optional[Callable] = None):
        self.job_id = job_id
        self.job_type = job_type  # mapreduce, map_only, reduce_only
        self.input_data = input_data
        self.num_reduce_tasks = num_reduce_tasks
        self.status = JobStatus.PENDING
        
        # Dependency tracking
        self.dependencies: List[str] = []  # Jobs this job depends on
        self.dependents: List[str] = []    # Jobs that depend on this job
        
        # Custom map/reduce functions (optional)
        self.map_func = map_func
        self.reduce_func = reduce_func
        
        # Results and timing
        self.output_data = None
        self.created_at = datetime.now()
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None
        self.error: Optional[str] = None
        
        # Task tracking (for internal MapReduce execution)
        self.map_tasks_completed = 0
        self.reduce_tasks_completed = 0
        self.total_map_tasks = 0
        self.total_reduce_tasks = 0
    
    def is_ready(self) -> bool:
        """Check if job is ready to run (all dependencies met)."""
        return self.status == JobStatus.READY
    
    def is_completed(self) -> bool:
        """Check if job has completed (success or failure)."""
        return self.status in (JobStatus.COMPLETED, JobStatus.FAILED)
    
    def to_dict(self) -> Dict:
        """Serialize job to dictionary."""
        return {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'status': self.status.value,
            'dependencies': self.dependencies,
            'dependents': self.dependents,
            'created_at': self.created_at.isoformat(),
            'started_at': self.started_at.isoformat() if self.started_at else None,
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'error': self.error,
            'progress': {
                'map': f"{self.map_tasks_completed}/{self.total_map_tasks}",
                'reduce': f"{self.reduce_tasks_completed}/{self.total_reduce_tasks}"
            }
        }
    
    def __repr__(self):
        deps = f" (deps: {self.dependencies})" if self.dependencies else ""
        return f"Job({self.job_id}, status={self.status.value}{deps})"


class DAGJobScheduler:
    """
    Directed Acyclic Graph (DAG) based job scheduler.
    
    Features:
    - Define job dependencies (A → C, B → C means C waits for both A and B)
    - Automatic topological ordering
    - Cycle detection to prevent deadlocks
    - Parallel execution of independent jobs
    - Job chaining (output of one job → input of next)
    
    Example usage:
        scheduler = DAGJobScheduler()
        
        # Add independent jobs
        scheduler.add_job("preprocess_logs", input_data=log_files)
        scheduler.add_job("preprocess_events", input_data=event_files)
        
        # Add job that depends on both
        scheduler.add_job("combine_data", 
                          dependencies=["preprocess_logs", "preprocess_events"],
                          use_dependency_output=True)
        
        # Add final aggregation
        scheduler.add_job("final_report", dependencies=["combine_data"])
        
        # Execute all jobs in correct order
        scheduler.run_all()
    """
    
    def __init__(self):
        self.jobs: Dict[str, Job] = {}
        self.lock = threading.RLock()
        self.ready_queue: deque = deque()  # Jobs ready to execute
        self.completed_jobs: set = set()
        self.running_jobs: set = set()
        self.failed_jobs: set = set()
        
        # Event log for debugging
        self.events: List[Dict] = []
        
        # Callback for when a job completes (to notify task scheduler)
        self.on_job_complete: Optional[Callable] = None
        self.on_job_ready: Optional[Callable] = None
    
    def add_job(self, job_id: str, input_data: Any = None, 
                num_reduce_tasks: int = 3, dependencies: List[str] = None,
                job_type: str = "mapreduce",
                use_dependency_output: bool = False,
                map_func: Optional[Callable] = None,
                reduce_func: Optional[Callable] = None) -> Job:
        """
        Add a job to the scheduler.
        
        Args:
            job_id: Unique identifier for the job
            input_data: Input data for the job (or None if using dependency output)
            num_reduce_tasks: Number of reduce partitions (R)
            dependencies: List of job_ids that must complete before this job
            job_type: Type of job (mapreduce, map_only, reduce_only)
            use_dependency_output: If True, use output from dependencies as input
            map_func: Custom map function (optional)
            reduce_func: Custom reduce function (optional)
        
        Returns:
            The created Job object
        
        Raises:
            ValueError: If job_id already exists or dependency doesn't exist
        """
        with self.lock:
            if job_id in self.jobs:
                raise ValueError(f"Job '{job_id}' already exists")
            
            dependencies = dependencies or []
            
            # Validate dependencies exist
            for dep_id in dependencies:
                if dep_id not in self.jobs:
                    raise ValueError(
                        f"Dependency '{dep_id}' does not exist. "
                        f"Add it before adding '{job_id}'."
                    )
            
            # Create job
            job = Job(job_id, input_data, num_reduce_tasks, job_type,
                      map_func, reduce_func)
            job.dependencies = dependencies
            
            # Check for cycles before adding
            if dependencies and self._would_create_cycle(job_id, dependencies):
                raise ValueError(
                    f"Adding job '{job_id}' with dependencies {dependencies} "
                    f"would create a cycle in the DAG"
                )
            
            # Register this job as a dependent of its dependencies
            for dep_id in dependencies:
                self.jobs[dep_id].dependents.append(job_id)
            
            self.jobs[job_id] = job
            
            # Mark flag for using dependency output
            if use_dependency_output:
                job._use_dependency_output = True
            
            # Check if job is immediately ready
            if self._check_dependencies_met(job_id):
                job.status = JobStatus.READY
                self.ready_queue.append(job_id)
                self._log_event("job_ready", {"job_id": job_id})
                print(f"[{datetime.now()}] Job '{job_id}' is READY (no dependencies)")
            else:
                self._log_event("job_pending", {
                    "job_id": job_id, 
                    "waiting_for": dependencies
                })
                print(f"[{datetime.now()}] Job '{job_id}' PENDING, waiting for: {dependencies}")
            
            return job
    
    def _would_create_cycle(self, new_job_id: str, dependencies: List[str]) -> bool:
        """
        Check if adding a job with given dependencies would create a cycle.
        Uses DFS to detect if any dependency can reach back to new_job_id.
        """
        # In this scheduler, dependencies are edges: dep -> new_job.
        # Adding edges dep -> new_job creates a cycle iff there is already
        # a path new_job -> dep in the existing graph.
        #
        # Note: new_job doesn't exist in self.jobs yet when this is called.
        # However, it *can* still be referenced as a dependency of existing jobs
        # (i.e., some job already depends on new_job_id). In that case, there may
        # be existing edges: new_job_id -> ...

        def has_path(start: str, target: str) -> bool:
            """Return True if there's a path start -> ... -> target via dependents."""
            if start == target:
                return True

            visited: set = set()
            stack = [start]

            while stack:
                node = stack.pop()
                if node == target:
                    return True
                if node in visited:
                    continue
                visited.add(node)

                job = self.jobs.get(node)
                if not job:
                    continue
                # follow edges node -> dependent
                stack.extend(job.dependents)

            return False

        # If new_job can already reach any dep, then dep -> new_job closes a cycle.
        for dep in dependencies:
            if has_path(new_job_id, dep):
                return True

        return False
    
    def _check_dependencies_met(self, job_id: str) -> bool:
        """Check if all dependencies for a job are completed."""
        job = self.jobs.get(job_id)
        if not job:
            return False
        
        for dep_id in job.dependencies:
            dep_job = self.jobs.get(dep_id)
            if not dep_job or dep_job.status != JobStatus.COMPLETED:
                return False
        
        return True
    
    def _propagate_completion(self, job_id: str):
        """
        When a job completes, check if any dependent jobs are now ready.
        This cascades through the DAG.
        """
        job = self.jobs.get(job_id)
        if not job:
            return
        
        for dependent_id in job.dependents:
            dependent = self.jobs.get(dependent_id)
            if not dependent:
                continue
            
            # Skip if already running or completed
            if dependent.status not in (JobStatus.PENDING,):
                continue
            
            # Check if all dependencies are now met
            if self._check_dependencies_met(dependent_id):
                dependent.status = JobStatus.READY
                
                # If using dependency output, collect it
                if getattr(dependent, '_use_dependency_output', False):
                    combined_output = []
                    for dep_id in dependent.dependencies:
                        dep_job = self.jobs.get(dep_id)
                        if dep_job and dep_job.output_data:
                            combined_output.append(dep_job.output_data)
                    dependent.input_data = combined_output
                
                self.ready_queue.append(dependent_id)
                self._log_event("job_ready", {
                    "job_id": dependent_id,
                    "triggered_by": job_id
                })
                print(f"[{datetime.now()}] Job '{dependent_id}' is now READY "
                      f"(dependency '{job_id}' completed)")
                
                if self.on_job_ready:
                    self.on_job_ready(dependent_id)
    
    def mark_job_started(self, job_id: str):
        """Mark a job as running."""
        with self.lock:
            job = self.jobs.get(job_id)
            if job:
                job.status = JobStatus.RUNNING
                job.started_at = datetime.now()
                self.running_jobs.add(job_id)
                self._log_event("job_started", {"job_id": job_id})
                print(f"[{datetime.now()}] Job '{job_id}' STARTED")
    
    def mark_job_completed(self, job_id: str, output_data: Any = None):
        """
        Mark a job as completed and propagate to dependents.
        
        Args:
            job_id: The completed job
            output_data: Output to pass to dependent jobs
        """
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.now()
            job.output_data = output_data
            
            self.running_jobs.discard(job_id)
            self.completed_jobs.add(job_id)
            
            duration = (job.completed_at - job.started_at).total_seconds() if job.started_at else 0
            self._log_event("job_completed", {
                "job_id": job_id,
                "duration_seconds": duration
            })
            print(f"[{datetime.now()}] Job '{job_id}' COMPLETED in {duration:.2f}s")
            
            # Propagate to dependents
            self._propagate_completion(job_id)
            
            if self.on_job_complete:
                self.on_job_complete(job_id)
    
    def mark_job_failed(self, job_id: str, error: str):
        """Mark a job as failed."""
        with self.lock:
            job = self.jobs.get(job_id)
            if not job:
                return
            
            job.status = JobStatus.FAILED
            job.completed_at = datetime.now()
            job.error = error
            
            self.running_jobs.discard(job_id)
            self.failed_jobs.add(job_id)
            
            self._log_event("job_failed", {"job_id": job_id, "error": error})
            print(f"[{datetime.now()}] Job '{job_id}' FAILED: {error}")
            
            # Mark all dependents as failed too (cascade failure)
            self._cascade_failure(job_id, error)
    
    def _cascade_failure(self, failed_job_id: str, original_error: str):
        """When a job fails, mark all downstream jobs as failed."""
        job = self.jobs.get(failed_job_id)
        if not job:
            return
        
        for dependent_id in job.dependents:
            dependent = self.jobs.get(dependent_id)
            if dependent and dependent.status == JobStatus.PENDING:
                dependent.status = JobStatus.FAILED
                dependent.error = f"Dependency '{failed_job_id}' failed: {original_error}"
                self.failed_jobs.add(dependent_id)
                print(f"[{datetime.now()}] Job '{dependent_id}' FAILED "
                      f"(dependency '{failed_job_id}' failed)")
                self._cascade_failure(dependent_id, original_error)
    
    def get_next_ready_job(self) -> Optional[str]:
        """Get the next job that's ready to run."""
        with self.lock:
            while self.ready_queue:
                job_id = self.ready_queue.popleft()
                job = self.jobs.get(job_id)
                if job and job.status == JobStatus.READY:
                    return job_id
            return None
    
    def get_all_ready_jobs(self) -> List[str]:
        """Get all jobs that are ready to run (for parallel execution)."""
        with self.lock:
            ready = []
            while self.ready_queue:
                job_id = self.ready_queue.popleft()
                job = self.jobs.get(job_id)
                if job and job.status == JobStatus.READY:
                    ready.append(job_id)
            return ready
    
    def get_topological_order(self) -> List[str]:
        """
        Get jobs in topological order (respecting dependencies).
        Uses Kahn's algorithm.
        
        Returns:
            List of job_ids in execution order
        
        Raises:
            ValueError: If there's a cycle in the DAG
        """
        with self.lock:
            # Count incoming edges (dependencies)
            in_degree = {job_id: len(job.dependencies) for job_id, job in self.jobs.items()}
            
            # Start with jobs that have no dependencies
            queue = deque([job_id for job_id, degree in in_degree.items() if degree == 0])
            
            result = []
            
            while queue:
                job_id = queue.popleft()
                result.append(job_id)
                
                # Reduce in-degree for all dependents
                job = self.jobs[job_id]
                for dependent_id in job.dependents:
                    in_degree[dependent_id] -= 1
                    if in_degree[dependent_id] == 0:
                        queue.append(dependent_id)
            
            # Check for cycle
            if len(result) != len(self.jobs):
                remaining = set(self.jobs.keys()) - set(result)
                raise ValueError(f"Cycle detected in DAG! Jobs involved: {remaining}")
            
            return result
    
    def is_all_complete(self) -> bool:
        """Check if all jobs have completed (success or failure)."""
        with self.lock:
            return all(job.is_completed() for job in self.jobs.values())
    
    def get_status(self) -> Dict:
        """Get current status of all jobs."""
        with self.lock:
            return {
                'total_jobs': len(self.jobs),
                'completed': len(self.completed_jobs),
                'running': len(self.running_jobs),
                'failed': len(self.failed_jobs),
                'pending': len([j for j in self.jobs.values() 
                               if j.status == JobStatus.PENDING]),
                'ready': len([j for j in self.jobs.values() 
                             if j.status == JobStatus.READY]),
                'jobs': {job_id: job.to_dict() for job_id, job in self.jobs.items()}
            }
    
    def _log_event(self, event_type: str, data: Dict):
        """Log an event for debugging."""
        self.events.append({
            'timestamp': datetime.now().isoformat(),
            'type': event_type,
            'data': data
        })
    
    def get_ascii_dag(self) -> str:
        """Generate ASCII visualization of the job DAG."""
        if not self.jobs:
            return "No jobs in scheduler"
        
        lines = []
        lines.append("╔══════════════════════════════════════════════════════════════╗")
        lines.append("║                      JOB DEPENDENCY GRAPH                    ║")
        lines.append("╠══════════════════════════════════════════════════════════════╣")
        
        # Status icons
        status_icons = {
            JobStatus.PENDING: "⏳",
            JobStatus.READY: "🔵",
            JobStatus.RUNNING: "🔄",
            JobStatus.COMPLETED: "✅",
            JobStatus.FAILED: "❌"
        }
        
        # Get topological order for display
        try:
            ordered = self.get_topological_order()
        except ValueError:
            ordered = list(self.jobs.keys())
        
        for job_id in ordered:
            job = self.jobs[job_id]
            icon = status_icons.get(job.status, "❓")
            
            # Show dependencies
            if job.dependencies:
                deps_str = f" ← [{', '.join(job.dependencies)}]"
            else:
                deps_str = " (root)"
            
            line = f"║  {icon} {job_id}: {job.status.value}{deps_str}"
            line = line.ljust(65) + "║"
            lines.append(line)
        
        lines.append("╠══════════════════════════════════════════════════════════════╣")
        
        # Summary
        status = self.get_status()
        summary = (f"║  Total: {status['total_jobs']} | "
                   f"✅ {status['completed']} | "
                   f"🔄 {status['running']} | "
                   f"⏳ {status['pending']} | "
                   f"❌ {status['failed']}")
        summary = summary.ljust(65) + "║"
        lines.append(summary)
        lines.append("╚══════════════════════════════════════════════════════════════╝")
        
        return "\n".join(lines)
    
    def clear(self):
        """Clear all jobs from the scheduler."""
        with self.lock:
            self.jobs.clear()
            self.ready_queue.clear()
            self.completed_jobs.clear()
            self.running_jobs.clear()
            self.failed_jobs.clear()
            self.events.clear()


# Example usage and testing
if __name__ == '__main__':
    print("=" * 60)
    print("DAG Job Scheduler Demo")
    print("=" * 60)
    
    scheduler = DAGJobScheduler()
    
    # Create a workflow:
    #
    #   preprocess_logs ──┐
    #                     ├──► combine_data ──► final_report
    #   preprocess_events ┘
    #
    
    print("\n1. Adding jobs with dependencies...\n")
    
    # Root jobs (no dependencies)
    scheduler.add_job("preprocess_logs", input_data=["log1.txt", "log2.txt"])
    scheduler.add_job("preprocess_events", input_data=["events.json"])
    
    # Job that depends on both preprocessing jobs
    scheduler.add_job(
        "combine_data",
        dependencies=["preprocess_logs", "preprocess_events"],
        use_dependency_output=True
    )
    
    # Final job
    scheduler.add_job(
        "final_report",
        dependencies=["combine_data"]
    )
    
    print("\n2. Initial DAG state:\n")
    print(scheduler.get_ascii_dag())
    
    print("\n3. Executing jobs in order...\n")
    
    # Get topological order
    order = scheduler.get_topological_order()
    print(f"Topological order: {order}")
    
    # Simulate execution
    print("\n4. Simulating execution...\n")
    
    # Execute ready jobs
    while not scheduler.is_all_complete():
        ready_jobs = scheduler.get_all_ready_jobs()
        if not ready_jobs:
            print("No ready jobs, waiting...")
            break
        
        for job_id in ready_jobs:
            scheduler.mark_job_started(job_id)
            # Simulate work
            import time
            time.sleep(0.5)
            scheduler.mark_job_completed(job_id, output_data=f"output_of_{job_id}")
        
        print(scheduler.get_ascii_dag())
        print()
    
    print("\n5. Final state:\n")
    print(scheduler.get_ascii_dag())
