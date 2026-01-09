use crossbeam_channel::Receiver;

use crate::SchedulerConfiguration;
use crate::job::JobHandle;
use crate::worker::{Injectors, WorkQueues, WorkStealers, WorkerContext, WorkerId, worker_thread};
use crate::{builder::JobBuilder, util::SharedString};
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::{Arc, RwLock};
use std::thread::JoinHandle;

pub(crate) struct SchedulerState {
    worker_threads: RwLock<Vec<JoinHandle<()>>>,
    next_thread_to_wake: AtomicUsize,
    num_workers: usize,

    /// Number of jobs currently queued or running
    pub(crate) num_jobs_queued: AtomicUsize,

    pub(crate) injectors: Injectors,
    pub(crate) stealers: Vec<WorkStealers>,

    pub(crate) paused: AtomicBool,
    pub(crate) exiting: AtomicBool,

    free_queue_rx: Receiver<JobHandle>,
}

/// Manages worker threads and job execution.
#[derive(Clone)]
pub struct Scheduler {
    pub(crate) inner: Arc<SchedulerState>,
}

impl Scheduler {
    /// Creates a new scheduler with a number of worker threads equal to the number of physical CPU cores (or 4 if CPU topology cannot be determined).
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::default();
    /// assert_eq!(scheduler.num_workers(), gdt_cpus::num_physical_cores().unwrap_or(4));
    /// ```
    #[allow(clippy::new_without_default)]
    pub fn new(config: &SchedulerConfiguration) -> Self {
        let num_workers = config.workers.len();
        let (mut workers, stealers): (Vec<_>, Vec<_>) = config
            .workers
            .iter()
            .map(|_| {
                let worker_id = WorkerId::new();
                let (workers, stealers) = WorkQueues::new(worker_id);
                (workers, stealers)
            })
            .unzip();

        let (free_queue_tx, free_queue_rx) = crossbeam_channel::unbounded();

        let state = Arc::new(SchedulerState {
            worker_threads: RwLock::new(Vec::with_capacity(num_workers)),
            next_thread_to_wake: AtomicUsize::new(0),
            num_workers,
            injectors: Injectors::new(),
            stealers,
            num_jobs_queued: AtomicUsize::new(0),
            paused: AtomicBool::new(false),
            exiting: AtomicBool::new(false),
            free_queue_rx,
        });

        let scheduler = Scheduler {
            inner: Arc::clone(&state),
        };

        for (i, worker_config) in config.workers.iter().cloned().enumerate() {
            let scheduler = scheduler.clone();
            let queues = workers
                .pop()
                .expect("unreachable: number of workers == number of threads");
            let free_queue_tx_clone = free_queue_tx.clone();
            let handle = std::thread::Builder::new()
                .name(format!("job-executor-{i}"))
                .spawn(move || {
                    if let Err(e) = gdt_cpus::set_thread_affinity(
                        &gdt_cpus::AffinityMask::from_cores(&worker_config.affinity),
                    ) {
                        log::error!("Failed to pin thread to core {}: {}", i, e);
                    }
                    if let Err(_e) = gdt_cpus::set_thread_priority(worker_config.priority.into()) {
                        // cohae: gdt_cpus apparently already logs this, whyyy
                        // log::error!("Failed to set thread priority: {}", e);
                    }

                    worker_thread(WorkerContext::new(scheduler, queues, free_queue_tx_clone));
                })
                .expect("Failed to spawn scheduler thread");
            state
                .worker_threads
                .write()
                .expect("Failed to acquire worker_threads lock")
                .push(handle);
        }

        scheduler
    }

    /// Creates a new scheduler with the specified number of worker threads.
    ///
    /// In most cases, [`Scheduler::default`] is preferred, as it automatically configures the worker threads in the most optimal way for the current system.
    ///
    /// This function uses [`SchedulerConfiguration::with_cores_autopin`] to create the worker threads. If you need more control over the worker thread configuration, consider using [`Scheduler::new`] with a custom [`SchedulerConfiguration`].
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
        let config = SchedulerConfiguration::with_cores_autopin(num_workers);
        Scheduler::new(&config)
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
    /// let scheduler = Scheduler::default();
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

        const FREE_QUEUE_QUOTA: usize = 64;
        for _ in 0..FREE_QUEUE_QUOTA {
            if self.inner.free_queue_rx.try_recv().is_err() {
                break;
            }
        }

        handle
    }

    /// Creates a new JobBuilder
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::default();
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
    /// let scheduler = Scheduler::default();
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
    /// let scheduler = Scheduler::default();
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
    /// let scheduler = Scheduler::default();
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
        let threads = std::mem::take(
            &mut *self
                .inner
                .worker_threads
                .write()
                .expect("Failed to acquire worker_threads lock"),
        );
        for handle in threads {
            let _ = handle.join();
        }
    }

    /// Shuts down the scheduler gracefully, waiting for all queued and executing jobs to complete before terminating worker threads.
    pub fn shutdown_graceful(&self) {
        self.wait_for_all();
        self.shutdown();
    }
}

impl Scheduler {
    pub(crate) fn wake_all_workers(&self) {
        let worker_threads = self
            .inner
            .worker_threads
            .read()
            .expect("Failed to acquire worker_threads lock");

        for thread in worker_threads.iter() {
            thread.thread().unpark();
        }
    }

    pub(crate) fn wake_one_worker(&self) {
        let worker_threads = self
            .inner
            .worker_threads
            .read()
            .expect("Failed to acquire worker_threads lock");

        if !worker_threads.is_empty() {
            let idx = self
                .inner
                .next_thread_to_wake
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                % worker_threads.len();

            worker_threads[idx].thread().unpark();
        }
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new(&SchedulerConfiguration::default())
    }
}

impl Drop for SchedulerState {
    fn drop(&mut self) {
        log::info!("Scheduler shut down.");
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::builder::Priority;
    use crate::scheduler::Scheduler;

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
                        ORDER.lock().unwrap().push(i);
                    });
            }
        }
        scheduler.resume();

        scheduler.wait_for_all();

        let order = ORDER.lock().unwrap();
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
                LOG.lock().unwrap().push("A");
            });

        let job_b = scheduler
            .job_builder("job_b")
            .priority(Priority::Medium)
            .dependencies(vec![job_a])
            .spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(70));
                LOG.lock().unwrap().push("B");
            });

        let job_c = scheduler
            .job_builder("job_c")
            .priority(Priority::Medium)
            .dependencies(vec![job_b])
            .spawn(|| {
                LOG.lock().unwrap().push("C");
            });

        let _job_d = scheduler
            .job_builder("job_d")
            .priority(Priority::Medium)
            .dependencies(vec![job_c])
            .spawn(|| {
                LOG.lock().unwrap().push("D");
            });

        scheduler.wait_for_all();

        let log = LOG.lock().unwrap();
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
