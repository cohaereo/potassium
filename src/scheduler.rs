use crate::builder::Priority;
use crate::job::JobHandle;
use crate::{builder::JobBuilder, util::SharedString};
use crossbeam_deque::{Injector, Steal, Stealer, Worker};
use parking_lot::RwLock;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::thread::JoinHandle;

const MAX_BATCH_SIZE: usize = 32;

struct SchedulerState {
    worker_threads: RwLock<Vec<JoinHandle<()>>>,
    next_thread_to_wake: AtomicUsize,

    num_workers: usize,

    injectors: Injectors,
    stealers: Vec<WorkStealers>,

    /// Number of jobs currently queued or running
    num_jobs_queued: AtomicUsize,

    paused: AtomicBool,
    exiting: AtomicBool,
}

/// Manages worker threads and job execution.
#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerState>,
}

impl Scheduler {
    /// Creates a new scheduler with a number of worker threads equal to the number of physical CPU cores (or 4 if CPU topology cannot be determined).
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::new();
    /// assert_eq!(scheduler.num_workers(), gdt_cpus::num_physical_cores().unwrap_or(4));
    /// ```
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self::with_workers(gdt_cpus::num_physical_cores().unwrap_or(4))
    }

    /// Creates a new scheduler with the specified number of worker threads.
    ///
    /// In most cases, [`Scheduler::new`] is preferred, as it automatically configures the worker threads in the most optimal way for the current system.
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::with_workers(8);
    /// assert_eq!(scheduler.num_workers(), 8);
    /// ```
    pub fn with_workers(num_workers: usize) -> Self {
        let (mut workers, stealers): (Vec<_>, Vec<_>) = (0..num_workers)
            .map(|_| {
                let worker_id = WorkerId::new();
                let (workers, stealers) = WorkQueues::new(worker_id);
                (workers, stealers)
            })
            .unzip();

        let state = Arc::new(SchedulerState {
            worker_threads: RwLock::new(Vec::with_capacity(num_workers)),
            next_thread_to_wake: AtomicUsize::new(0),
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
                    if let Err(_e) =
                        gdt_cpus::set_thread_priority(gdt_cpus::ThreadPriority::AboveNormal)
                    {
                        // cohae: gdt_cpus apparently already logs this, whyyy
                        // log::error!("Failed to set thread priority: {}", e);
                    }

                    worker_thread(WorkerContext { scheduler, queues })
                })
                .expect("Failed to spawn scheduler thread");
            state.worker_threads.write().push(handle);
        }

        scheduler
    }

    /// Returns the number of worker threads in the scheduler.
    ///
    /// The number of workers is fixed at scheduler creation, and cannot be changed.
    pub fn num_workers(&self) -> usize {
        self.inner.num_workers
    }

    /// Spawns a new job with the given specification and body.
    ///
    /// **Note:** It is recommended to use [`JobBuilder::spawn`] instead, as it provides a more ergonomic API.
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::new();
    /// let spec = scheduler.job_builder("example_job");
    /// let job_handle = scheduler.spawn(spec, || {
    ///     println!("Hello from the job!");
    /// });
    /// ```
    pub fn spawn<'a, F>(&'a self, spec: impl Into<JobBuilder<'a>>, body: F) -> JobHandle
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

        self.wake_one_worker();

        handle
    }

    /// Creates a new JobBuilder
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::new();
    /// let job_handle = scheduler.job_builder("example_job")
    ///     .spawn(|| {
    ///         println!("Hello from the job!");
    ///     });
    /// ```
    pub fn job_builder(&'_ self, name: impl Into<SharedString>) -> JobBuilder<'_> {
        JobBuilder::builder(self, name)
    }

    /// Returns the number of jobs currently queued or running.
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::new();
    /// assert_eq!(scheduler.num_jobs_queued(), 0);
    ///
    /// scheduler.pause();
    /// scheduler.job_builder("example_job")
    ///     .spawn(|| {
    ///         println!("Hello from the job!");
    ///     });
    ///
    /// assert_eq!(scheduler.num_jobs_queued(), 1);
    /// ```
    pub fn num_jobs_queued(&self) -> usize {
        self.inner
            .num_jobs_queued
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Blocks the current thread until all queued jobs have completed.
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::new();
    /// scheduler.job_builder("example_job")
    ///     .spawn(|| {
    ///         println!("Hello from the job!");
    ///     });
    ///
    /// scheduler.wait_for_all();
    /// assert_eq!(scheduler.num_jobs_queued(), 0);
    /// ```
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

    /// Pauses the scheduler, preventing worker threads from executing jobs. Call [`Scheduler::resume`] to continue processing jobs.
    ///
    /// Note that this will not stop currently executing jobs; it only prevents new jobs from starting until [`Scheduler::resume`] is called.
    ///
    /// # Examples
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::new();
    /// scheduler.pause();
    /// // Jobs scheduled while paused will not execute until `resume` is called
    /// scheduler.job_builder("example_job")
    ///     .spawn(|| {
    ///         println!("This job will not run until the scheduler is resumed.");
    ///     });
    /// scheduler.resume();
    /// ```
    pub fn pause(&self) {
        self.inner
            .paused
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Resumes the scheduler, allowing worker threads to execute jobs.
    pub fn resume(&self) {
        self.inner
            .paused
            .store(false, std::sync::atomic::Ordering::Release);

        self.wake_all_workers();
    }

    /// Shuts down the scheduler, terminating all worker threads.
    ///
    /// Note that this only waits for currently executing jobs to finish; any queued jobs that have not started will not be executed.
    ///
    /// Use [`Scheduler::shutdown_graceful`] to wait for all jobs to complete before shutting down.
    pub fn shutdown(&self) {
        self.inner
            .exiting
            .store(true, std::sync::atomic::Ordering::Release);

        self.wake_all_workers();
        let threads = std::mem::take(&mut *self.inner.worker_threads.write());
        for handle in threads {
            let _ = handle.join();
        }
    }

    /// Shuts down the scheduler gracefully, waiting for all queued and executing jobs to complete before terminating worker threads.
    pub fn shutdown_graceful(&self) {
        self.wait_for_all();
        self.shutdown();
    }

    fn wake_all_workers(&self) {
        let worker_threads = self.inner.worker_threads.read();
        for thread in worker_threads.iter() {
            thread.thread().unpark();
        }
    }

    fn wake_one_worker(&self) {
        let worker_threads = self.inner.worker_threads.read();
        if !worker_threads.is_empty() {
            let idx = self
                .inner
                .next_thread_to_wake
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % worker_threads.len();

            worker_threads[idx].thread().unpark();
        }
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

impl Drop for SchedulerState {
    fn drop(&mut self) {
        log::info!("Scheduler shut down.");
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
            .load(std::sync::atomic::Ordering::Relaxed);

        if num_jobs_queued == 0 || ctx.is_paused() {
            // Double-check the condition to avoid missing notifications
            let num_jobs_queued = ctx
                .scheduler
                .inner
                .num_jobs_queued
                .load(std::sync::atomic::Ordering::Acquire);

            if (num_jobs_queued == 0 || ctx.is_paused()) && !ctx.is_exiting() {
                std::thread::park();
            }

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
        } else {
            // No job found but count > 0, likely all jobs are running or paused
            // Park briefly to avoid spinning
            std::thread::park_timeout(std::time::Duration::from_micros(100));
        }
    }
}

fn notify_dependents(ctx: &WorkerContext, job: &JobHandle) {
    let mut any_scheduled = false;
    for dependent in job.inner.dependents.read().iter() {
        let remaining = dependent
            .inner
            .remaining_dependencies
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);

        // Sanity check. If the last value was 0, we counted a dependency too many somewhere
        debug_assert!(
            remaining > 0,
            "Job {} has negative remaining dependencies",
            dependent.name()
        );

        if remaining == 1 {
            let is_already_enqueued = dependent
                .inner
                .enqueued
                .swap(true, std::sync::atomic::Ordering::AcqRel);
            if is_already_enqueued {
                // Already enqueued. This happens if dependencies complete while the job is initially checking its dependencies)
                // In this case, it's already been pushed into the global queue
                continue;
            }

            // All dependencies are complete, schedule the dependent job
            ctx.push_job(dependent.clone());
            any_scheduled = true;
        }
    }

    // If we scheduled any dependent jobs, wake a waiting thread
    if any_scheduled {
        ctx.scheduler.wake_one_worker();
    }
}

#[cfg(test)]
mod tests {
    use crate::builder::Priority;
    use crate::scheduler::Scheduler;
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
