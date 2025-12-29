use crate::job::JobHandle;
use crate::spec::Priority;
use crate::{
    spec::{JobSpec, JobSpecBuilder},
    util::SharedString,
};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use parking_lot::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::thread::JoinHandle;

const MAX_BATCH_SIZE: usize = 32;

struct SchedulerState {
    _threads: Mutex<Vec<JoinHandle<()>>>,
    num_workers: usize,

    injectors: Injectors,
    stealers: Vec<WorkStealers>,

    /// Number of jobs currently queued or running
    num_jobs_queued: AtomicUsize,

    paused: AtomicBool,
    exiting: AtomicBool,
}

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerState>,
}

impl Scheduler {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::with_workers(gdt_cpus::num_physical_cores().unwrap_or(4))
    }

    pub fn with_workers(num_workers: usize) -> Self {
        let (mut workers, stealers): (Vec<_>, Vec<_>) = (0..num_workers)
            .map(|_| {
                let worker_id = WorkerId::new();
                let (workers, stealers) = WorkQueues::new(worker_id);
                (workers, stealers)
            })
            .unzip();

        let state = Arc::new(SchedulerState {
            _threads: Mutex::new(Vec::with_capacity(num_workers)),
            num_workers,
            injectors: Injectors::new(),
            stealers,
            num_jobs_queued: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
            exiting: AtomicBool::new(false),
        });

        let scheduler = Scheduler {
            inner: Arc::clone(&state),
        };

        for i in 0..num_workers {
            let scheduler = scheduler.clone();
            let queues = workers
                .pop()
                .expect("unreachable: number of workers == number of threads");
            let handle = std::thread::Builder::new()
                .name(format!("job-executor-{i}"))
                .spawn(move || {
                    if let Err(e) = gdt_cpus::pin_thread_to_core(i) {
                        log::error!("Failed to pin thread to core {}: {}", i, e);
                    }
                    if let Err(e) =
                        gdt_cpus::set_thread_priority(gdt_cpus::ThreadPriority::AboveNormal)
                    {
                        log::error!("Failed to set thread priority: {}", e);
                    }
                    worker_thread(WorkerContext { scheduler, queues })
                })
                .expect("Failed to spawn scheduler thread");
            state._threads.lock().push(handle);
        }

        scheduler
    }

    pub fn num_workers(&self) -> usize {
        self.inner.num_workers
    }

    pub fn spawn<'a, F>(&'a self, spec: impl Into<JobSpec<'a>>, body: F) -> JobHandle
    where
        F: FnOnce() + Send + 'static,
    {
        let (handle, push_to_global_queue) = JobHandle::new(spec.into(), body);

        // If the job has no (pending) dependencies, push it to the global injector
        // Otherwise, it will be scheduled into the local queue of the worker that completes its last dependency
        if push_to_global_queue {
            self.inner.injectors.push(handle.clone());
        }
        self.inner
            .num_jobs_queued
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        handle
    }

    pub fn job_builder(&'_ self, name: impl Into<SharedString>) -> JobSpecBuilder<'_> {
        JobSpec::builder(self, name)
    }

    pub fn wait_for_all(&self) {
        while self
            .inner
            .num_jobs_queued
            .load(std::sync::atomic::Ordering::Acquire)
            > 0
        {
            std::thread::yield_now();
        }
    }

    pub fn pause(&self) {
        self.inner
            .paused
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub fn resume(&self) {
        self.inner
            .paused
            .store(false, std::sync::atomic::Ordering::Release);
    }

    fn try_steal_job(&self, work_queues: &WorkQueues) -> Option<JobHandle> {
        for stealer in &self.inner.stealers {
            if stealer.owner == work_queues.owner {
                continue;
            }

            for priority in Priority::ALL {
                loop {
                    let stealer = match priority {
                        Priority::High => &stealer.high,
                        Priority::Medium => &stealer.medium,
                        Priority::Low => &stealer.low,
                    };

                    let worker = match priority {
                        Priority::High => &work_queues.high,
                        Priority::Medium => &work_queues.medium,
                        Priority::Low => &work_queues.low,
                    };

                    match stealer.steal_batch_with_limit_and_pop(worker, MAX_BATCH_SIZE) {
                        Steal::Success(job) => return Some(job),
                        Steal::Empty => break,
                        Steal::Retry => continue,
                    }
                }
            }
        }

        None
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.inner
            .exiting
            .store(true, std::sync::atomic::Ordering::Release);

        let threads = std::mem::take(&mut *self.inner._threads.lock());
        for handle in threads {
            let _ = handle.join();
        }
    }
}

/// Globally unique identifier for a worker thread. Unique across all schedulers.
#[derive(Debug, Copy, Clone, PartialEq)]
struct WorkerId(usize);

impl WorkerId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        WorkerId(id)
    }
}

struct Injectors {
    high: Injector<JobHandle>,
    medium: Injector<JobHandle>,
    low: Injector<JobHandle>,
}

impl Injectors {
    pub fn new() -> Self {
        Self {
            high: Injector::new(),
            medium: Injector::new(),
            low: Injector::new(),
        }
    }

    pub fn push(&self, job: JobHandle) {
        match job.priority() {
            Priority::High => self.high.push(job),
            Priority::Medium => self.medium.push(job),
            Priority::Low => self.low.push(job),
        }
    }

    fn steal_batch_and_pop(&self, work_queues: &WorkQueues) -> Option<JobHandle> {
        for priority in Priority::ALL {
            let injector = match priority {
                Priority::High => &self.high,
                Priority::Medium => &self.medium,
                Priority::Low => &self.low,
            };

            loop {
                match injector.steal_batch_with_limit_and_pop(
                    match priority {
                        Priority::High => &work_queues.high,
                        Priority::Medium => &work_queues.medium,
                        Priority::Low => &work_queues.low,
                    },
                    MAX_BATCH_SIZE,
                ) {
                    Steal::Success(job) => return Some(job),
                    Steal::Empty => break,
                    Steal::Retry => continue,
                };
            }
        }

        None
    }
}

struct WorkStealers {
    owner: WorkerId,
    high: Stealer<JobHandle>,
    medium: Stealer<JobHandle>,
    low: Stealer<JobHandle>,
}

struct WorkQueues {
    owner: WorkerId,
    high: Worker<JobHandle>,
    medium: Worker<JobHandle>,
    low: Worker<JobHandle>,
}

impl WorkQueues {
    pub fn new(worker_id: WorkerId) -> (Self, WorkStealers) {
        let [high, medium, low] = std::array::from_fn(|_| {
            if cfg!(feature = "fifo") {
                Worker::new_fifo()
            } else {
                Worker::new_lifo()
            }
        });

        let stealers = WorkStealers {
            owner: worker_id,
            high: high.stealer(),
            medium: medium.stealer(),
            low: low.stealer(),
        };

        let queues = Self {
            owner: worker_id,
            high,
            medium,
            low,
        };

        (queues, stealers)
    }
}

struct WorkerContext {
    scheduler: Scheduler,
    queues: WorkQueues,
}

impl WorkerContext {
    fn fetch_job(&self) -> Option<JobHandle> {
        // Check local queues first
        for priority in Priority::ALL {
            let job = match priority {
                Priority::High => self.queues.high.pop(),
                Priority::Medium => self.queues.medium.pop(),
                Priority::Low => self.queues.low.pop(),
            };

            if let Some(job) = job {
                return Some(job);
            }
        }

        // Try to steal from other workers and the global injector
        if let Some(job) = self.scheduler.try_steal_job(&self.queues) {
            return Some(job);
        }

        self.scheduler
            .inner
            .injectors
            .steal_batch_and_pop(&self.queues)
    }

    fn push_job(&self, job: JobHandle) {
        match job.priority() {
            Priority::High => self.queues.high.push(job),
            Priority::Medium => self.queues.medium.push(job),
            Priority::Low => self.queues.low.push(job),
        }
    }

    fn is_exiting(&self) -> bool {
        self.scheduler
            .inner
            .exiting
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn is_paused(&self) -> bool {
        self.scheduler
            .inner
            .paused
            .load(std::sync::atomic::Ordering::Acquire)
    }
}

fn worker_thread(ctx: WorkerContext) {
    loop {
        if ctx.is_exiting() {
            break;
        }

        let num_jobs_queued = ctx
            .scheduler
            .inner
            .num_jobs_queued
            .load(std::sync::atomic::Ordering::Acquire);

        if num_jobs_queued == 0 || ctx.is_paused() {
            std::thread::yield_now();
            continue;
        }

        if let Some(job) = ctx.fetch_job() {
            // Execute the job
            let Some(job_body) = (unsafe { job.take_body() }) else {
                panic!("Job body already taken for job {}", job.name());
            };

            {
                profiling::scope!(job.name());
                (job_body)();
            }

            job.set_completed();
            notify_dependents(&ctx, &job);

            // Decrement the queued job count
            ctx.scheduler
                .inner
                .num_jobs_queued
                .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
    }
}

fn notify_dependents(ctx: &WorkerContext, job: &JobHandle) {
    for dependent in job.inner.dependents.read().iter() {
        let remaining = dependent
            .inner
            .remaining_dependencies
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);

        if remaining == 1 {
            // All dependencies are complete, schedule the dependent job
            ctx.push_job(dependent.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::scheduler::Scheduler;
    use crate::spec::Priority;
    use parking_lot::Mutex;

    #[test]
    fn test_simple_jobs() {
        let scheduler = Scheduler::with_workers(4);

        static COUNTER: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

        for i in 0..3 {
            scheduler
                .job_builder("simple")
                .priority(Priority::High)
                .spawn(move || {
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
            let id = scheduler
                .job_builder("wait_single")
                .priority(Priority::Medium)
                .spawn(move || {
                    println!("Job {i} starting.");
                    std::thread::sleep(std::time::Duration::from_millis(120 * (i + 1) as u64));
                    println!("Job {i} completed.");
                    COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                });
            job_ids.push(id);
        }

        // Wait for the third job only (effectively waiting for the first and second to finish too)
        job_ids[2].wait();
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
                scheduler
                    .job_builder("priority_test")
                    .priority(priority)
                    .spawn(move || {
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

        let job_a = scheduler
            .job_builder("job_a")
            .priority(Priority::Medium)
            .spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(50));
                LOG.lock().push("A");
            });

        let job_b = scheduler
            .job_builder("job_b")
            .priority(Priority::Medium)
            .dependencies(vec![job_a])
            .spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(70));
                LOG.lock().push("B");
            });

        let job_c = scheduler
            .job_builder("job_c")
            .priority(Priority::Medium)
            .dependencies(vec![job_b])
            .spawn(|| {
                LOG.lock().push("C");
            });

        let _job_d = scheduler
            .job_builder("job_d")
            .priority(Priority::Medium)
            .dependencies(vec![job_c])
            .spawn(|| {
                LOG.lock().push("D");
            });

        scheduler.wait_for_all();

        let log = LOG.lock();
        assert_eq!(*log, vec!["A", "B", "C", "D"]); // Jobs should execute in order A -> B -> C
    }

    // #[test]
    // fn job_conditions() {
    //     let scheduler = Scheduler::with_workers(2);

    //     static LOG: Mutex<Vec<&'static str>> = Mutex::new(Vec::new());
    //     static CONDITION_MET: std::sync::atomic::AtomicBool =
    //         std::sync::atomic::AtomicBool::new(false);
    //     static NUM_CONDITION_CHECKS: std::sync::atomic::AtomicUsize =
    //         std::sync::atomic::AtomicUsize::new(0);

    //     scheduler.job_builder("conditional_job")
    //         .priority(Priority::High)
    //         .condition(|| {
    //             NUM_CONDITION_CHECKS.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    //             CONDITION_MET.load(std::sync::atomic::Ordering::SeqCst)
    //         })
    //         .spawn(|| {
    //             LOG.lock().push("Conditional Job Executed");
    //         });

    //     // Schedule a job to set the condition after a delay
    //     scheduler.job_builder("set_condition")
    //         .priority(Priority::Low)
    //         .spawn(|| {
    //             std::thread::sleep(std::time::Duration::from_millis(10));
    //             CONDITION_MET.store(true, std::sync::atomic::Ordering::SeqCst);
    //         });

    //     scheduler.wait_for_all();

    //     let log = LOG.lock();
    //     assert_eq!(*log, vec!["Conditional Job Executed"]); // Conditional job should execute after condition is met
    //     // TODO(cohae): "Condition was checked 10780 times". We need to reduce the amount of times the conditions are checked (eg. move waiting jobs to a separate queue?)
    //     println!(
    //         "Condition was checked {} times",
    //         NUM_CONDITION_CHECKS.load(std::sync::atomic::Ordering::SeqCst)
    //     );
    // }
}
