use crate::spec::{JobSpec, Priority};
use log::error;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct JobId(usize);

struct Job {
    spec: JobSpec,
    state: JobState,

    /// Execution context for the job.
    ///
    /// Scheduler threads take the value when executing the job, putting it back when the job has finished/yielded.
    context: Option<JobExecutionContext>,
}

#[derive(Debug, PartialEq, Eq)]
enum JobState {
    Ready,
    Running,
    Finished,
}

#[derive(Clone)]
pub struct Scheduler {
    state: Arc<Mutex<SchedulerState>>,
}

struct SchedulerState {
    _threads: Vec<std::thread::JoinHandle<()>>,

    /// All jobs currently known to the scheduler
    jobs: HashMap<JobId, Job>,
    /// Work queues for each priority level
    ready_jobs: [VecDeque<JobId>; Priority::COUNT],
    next_job_id: usize,

    paused: bool,
    exiting: bool,
}

struct JobExecutionContext {
    job_id: JobId,
    body: Box<dyn FnOnce() + Send + 'static>,
}

impl Scheduler {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::with_workers(num_cpus::get_physical())
    }

    pub fn with_workers(num_workers: usize) -> Self {
        let state = Arc::new(Mutex::new(SchedulerState {
            _threads: Vec::with_capacity(num_workers),
            jobs: HashMap::new(),
            next_job_id: 1,
            ready_jobs: Default::default(),
            paused: false,
            exiting: false,
        }));

        for i in 0..num_workers {
            let state_clone = Arc::clone(&state);
            let handle = std::thread::Builder::new()
                .name(format!("job-executor-{}", i))
                .spawn(move || {
                    scheduler_thread(state_clone);
                })
                .expect("Failed to spawn scheduler thread");
            state.lock()._threads.push(handle);
        }

        Scheduler { state }
    }

    pub fn num_workers(&self) -> usize {
        self.state.lock()._threads.len()
    }

    pub fn schedule_job<F>(&self, spec: JobSpec, body: F) -> JobId
    where
        F: FnOnce() + Send + 'static,
    {
        let mut state = self.state.lock();
        let job_id = JobId(state.next_job_id);
        let priority = spec.priority;
        state.jobs.insert(
            job_id,
            Job {
                state: JobState::Ready,
                context: Some(JobExecutionContext {
                    job_id,
                    body: Box::new(body),
                }),
                spec,
            },
        );

        state.next_job_id = state.next_job_id.wrapping_add(1);

        state.ready_jobs[priority as usize].push_back(job_id);

        job_id
    }

    pub fn job_exists(&self, job_id: JobId) -> bool {
        self.state.lock().jobs.contains_key(&job_id)
    }

    pub fn wait_for(&self, job_id: JobId) {
        loop {
            {
                // Finished jobs are removed from the job list
                if !self.job_exists(job_id) {
                    break;
                }
            }
            std::thread::yield_now();
        }
    }

    /// Wait for all scheduled jobs to finish execution.
    pub fn wait_for_all(&self) {
        loop {
            {
                let state = self.state.lock();
                if state.jobs.is_empty() {
                    break;
                }
            }
            std::thread::yield_now();
        }
    }

    pub fn pause(&self) {
        self.state.lock().paused = true;
    }

    pub fn resume(&self) {
        self.state.lock().paused = false;
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        {
            let mut state = self.state.lock();
            state.exiting = true;
        }

        let threads = std::mem::take(&mut self.state.lock()._threads);
        for handle in threads {
            let _ = handle.join();
        }
    }
}

fn scheduler_thread(state: Arc<Mutex<SchedulerState>>) {
    'thread: loop {
        let job_id = match find_work(Arc::clone(&state)) {
            Some(id) => id,
            None => break 'thread, // Scheduler is exiting
        };

        let (ctx, name) = {
            let mut state = state.lock();
            let Some(job) = state.jobs.get_mut(&job_id) else {
                error!("Job {:?} not found in scheduler!", job_id);
                continue;
            };
            job.state = JobState::Running;
            (job.context.take(), job.spec.name.handle())
        };

        let Some(ctx) = ctx else {
            error!("Job {:?} has no execution context!", job_id);
            continue;
        };

        {
            let _ = name; // Silence unused variable warning if profiling is disabled
            profiling::scope!(&name);
            (ctx.body)();
        }

        {
            let mut state = state.lock();
            if let Some(job) = state.jobs.get_mut(&job_id) {
                job.state = JobState::Finished;
                job.context = None;
                state.jobs.remove(&job_id);
            }
        }
    }
}

/// Searches for a job that is ready to run, taking into account dependencies and conditions.
///
/// Returns `Some(JobId)` if a job is found, or `None` if the scheduler is exiting.
fn find_work(state: Arc<Mutex<SchedulerState>>) -> Option<JobId> {
    'work_search: loop {
        {
            let state = { state.lock() };
            if state.paused {
                std::thread::yield_now();
                continue 'work_search;
            }
            if state.exiting {
                return None;
            }
        }

        {
            let SchedulerState {
                ready_jobs, jobs, ..
            } = &mut *{ state.lock() };
            for priority_queue in ready_jobs.iter_mut().rev() {
                // Check dependencies before popping
                let mut to_remove = None;
                'find_job_in_queue: for (index, &job_id) in priority_queue.iter().enumerate() {
                    let job = match jobs.get(&job_id) {
                        Some(job) => job,
                        None => continue 'find_job_in_queue, // Job might have been removed
                    };
                    let mut conditions_met = true;
                    for &dep_id in &job.spec.dependencies {
                        if jobs.contains_key(&dep_id) {
                            conditions_met = false;
                            break;
                        }
                    }

                    if let Some(condition) = &job.spec.condition
                        && !condition.is_met()
                    {
                        conditions_met = false;
                    }

                    if conditions_met {
                        to_remove = Some(index);
                        break 'find_job_in_queue;
                    }
                }

                if let Some(index) = to_remove {
                    let job_id = priority_queue
                        .remove(index)
                        .expect("Job no longer exists in ready queue");
                    return Some(job_id);
                }
            }
        }

        std::thread::yield_now();
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduler::Scheduler;
    use crate::spec::{JobSpec, Priority};
    use parking_lot::Mutex;

    #[test]
    fn test_simple_jobs() {
        let scheduler = Scheduler::with_workers(4);

        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

        for i in 0..3 {
            JobSpec::builder("simple")
                .priority(Priority::High)
                .schedule(&scheduler, move || {
                    println!("Hello, world {i}!");
                    COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                });
        }

        scheduler.wait_for_all();
        assert_eq!(COUNTER.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    // Launch multiple jobs, but only wait for one of them to complete
    #[test]
    fn wait_for_single_job() {
        let scheduler = Scheduler::with_workers(5);

        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

        let mut job_ids = Vec::new();
        for i in 0..5 {
            let id = JobSpec::builder("wait_single")
                .priority(Priority::Medium)
                .schedule(&scheduler, move || {
                    println!("Job {i} starting.");
                    std::thread::sleep(std::time::Duration::from_millis(120 * (i + 1) as u64));
                    println!("Job {i} completed.");
                    COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                });
            job_ids.push(id);
        }

        // Wait for the third job only (effectively waiting for the first and second to finish too)
        scheduler.wait_for(job_ids[2]);
        assert_eq!(COUNTER.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[test]
    fn job_priorities() {
        let scheduler = Scheduler::with_workers(1);

        static ORDER: Mutex<Vec<usize>> = Mutex::new(Vec::new());

        // Pause the scheduler while we schedule jobs to ensure deterministic order
        scheduler.pause();
        // Schedule jobs with different priorities
        // Effectively [Low, Medium, High] repeated 4 times
        for _ in 0..4 {
            for i in 0..3 {
                let priority = match i {
                    0 => Priority::Low,
                    1 => Priority::Medium,
                    2 => Priority::High,
                    _ => unreachable!(),
                };
                JobSpec::builder("priority_test")
                    .priority(priority)
                    .schedule(&scheduler, move || {
                        ORDER.lock().push(i);
                    });
            }
        }
        scheduler.resume();

        scheduler.wait_for_all();

        let order = ORDER.lock();
        assert_eq!(*order, vec![2, 2, 2, 2, 1, 1, 1, 1, 0, 0, 0, 0]); // High priority (2) should execute first
    }

    #[test]
    fn job_dependencies() {
        let scheduler = Scheduler::with_workers(3);

        static LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());

        let job_a = JobSpec::builder("job_a")
            .priority(Priority::Medium)
            .schedule(&scheduler, || {
                std::thread::sleep(std::time::Duration::from_millis(50));
                LOG.lock().push("A");
            });

        let job_b = JobSpec::builder("job_b")
            .priority(Priority::Medium)
            .dependencies(vec![job_a])
            .schedule(&scheduler, || {
                std::thread::sleep(std::time::Duration::from_millis(70));
                LOG.lock().push("B");
            });

        let job_c = JobSpec::builder("job_c")
            .priority(Priority::Medium)
            .dependencies(vec![job_b])
            .schedule(&scheduler, || {
                LOG.lock().push("C");
            });

        let _job_d = JobSpec::builder("job_d")
            .priority(Priority::Medium)
            .dependencies(vec![job_c])
            .schedule(&scheduler, || {
                LOG.lock().push("D");
            });

        scheduler.wait_for_all();

        let log = LOG.lock();
        assert_eq!(*log, vec!["A", "B", "C", "D"]); // Jobs should execute in order A -> B -> C
    }

    #[test]
    fn job_conditions() {
        let scheduler = Scheduler::with_workers(2);

        static LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
        static CONDITION_MET: std::sync::atomic::AtomicBool =
            std::sync::atomic::AtomicBool::new(false);
        static NUM_CONDITION_CHECKS: std::sync::atomic::AtomicUsize =
            std::sync::atomic::AtomicUsize::new(0);

        JobSpec::builder("conditional_job")
            .priority(Priority::High)
            .condition(|| {
                NUM_CONDITION_CHECKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                CONDITION_MET.load(std::sync::atomic::Ordering::SeqCst)
            })
            .schedule(&scheduler, || {
                LOG.lock().push("Conditional Job Executed");
            });

        // Schedule a job to set the condition after a delay
        JobSpec::builder("set_condition")
            .priority(Priority::Low)
            .schedule(&scheduler, || {
                std::thread::sleep(std::time::Duration::from_millis(10));
                CONDITION_MET.store(true, std::sync::atomic::Ordering::SeqCst);
            });

        scheduler.wait_for_all();

        let log = LOG.lock();
        assert_eq!(*log, vec!["Conditional Job Executed"]); // Conditional job should execute after condition is met
        // TODO(cohae): "Condition was checked 10780 times". We need to reduce the amount of times the conditions are checked (eg. move waiting jobs to a separate queue?)
        println!(
            "Condition was checked {} times",
            NUM_CONDITION_CHECKS.load(std::sync::atomic::Ordering::SeqCst)
        );
    }
}
