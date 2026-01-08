use crossbeam_deque::{Injector, Steal, Stealer, Worker};

use crate::{JobHandle, Priority, Scheduler, job::JobState};

const MAX_BATCH_SIZE: usize = 32;

/// Globally unique identifier for a worker thread. Unique across all schedulers.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct WorkerId(usize);

impl WorkerId {
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicUsize, Ordering};

        static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

        let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
        WorkerId(id)
    }
}

pub struct Injectors {
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

    fn steal_batch_and_pop(
        &self,
        work_queues: &WorkQueues,
        priority: Priority,
    ) -> Option<JobHandle> {
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

        None
    }
}

pub struct WorkStealers {
    pub owner: WorkerId,
    high: Stealer<JobHandle>,
    medium: Stealer<JobHandle>,
    low: Stealer<JobHandle>,
}

pub struct WorkQueues {
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

pub struct WorkerContext {
    scheduler: Scheduler,
    queues: WorkQueues,
}

impl WorkerContext {
    pub fn new(scheduler: Scheduler, queues: WorkQueues) -> Self {
        Self { scheduler, queues }
    }

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

            // Try to steal from other workers and the global injector
            if let Some(job) = self.try_steal_job(&self.queues, priority) {
                return Some(job);
            }

            if let Some(j) = self
                .scheduler
                .inner
                .injectors
                .steal_batch_and_pop(&self.queues, priority)
            {
                return Some(j);
            }
        }

        None
    }

    fn push_job(&self, job: JobHandle) {
        match job.priority() {
            Priority::High => self.queues.high.push(job),
            Priority::Medium => self.queues.medium.push(job),
            Priority::Low => self.queues.low.push(job),
        }
    }

    fn try_steal_job(&self, work_queues: &WorkQueues, priority: Priority) -> Option<JobHandle> {
        for stealer in &self.scheduler.inner.stealers {
            if stealer.owner == work_queues.owner {
                continue;
            }

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

        None
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

pub fn worker_thread(ctx: WorkerContext) {
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
            if matches!(job.state(), JobState::Completed | JobState::Running) {
                panic!(
                    "Fetched job {} in invalid state {:?}. This indicates a bug in the scheduler, as running or completed jobs should not be re-scheduled.",
                    job.name(),
                    job.state()
                );
            }

            // Execute the job
            let Some(job_body) = (unsafe { job.take_body() }) else {
                panic!("Job body already taken for job {}", job.name());
            };

            job.set_state(JobState::Running);

            {
                profiling::scope!(job.name());
                (job_body)();
            }

            job.set_state(JobState::Completed);
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
    for dependent in job
        .inner
        .dependents
        .read()
        .expect("Failed to acquire JobHandle dependents read lock")
        .iter()
    {
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
