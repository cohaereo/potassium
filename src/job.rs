use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU32};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use fibrous::{FiberHandle, FiberStack};
use smallvec::SmallVec;

use crate::Scheduler;
use crate::builder::Priority;
use crate::fiber::FiberContext;
use crate::util::SharedString;

/// A shared handle to a scheduled job.
#[derive(Clone)]
pub struct JobHandle {
    pub(crate) inner: Arc<JobData>,
}

pub(crate) struct JobData {
    name: SharedString,
    body: UnsafeCell<Option<Box<dyn FnOnce() + Send + 'static>>>,
    pub(crate) priority: Priority,

    dependencies: SmallVec<[JobHandleWeak; 2]>,
    pub(crate) dependents: RwLock<SmallVec<[JobHandle; 2]>>,
    pub(crate) remaining_dependencies: AtomicU32,
    pub(crate) enqueued: AtomicBool,

    pub(crate) fiber_stack: UnsafeCell<Option<FiberStack>>,
    pub(crate) fiber: UnsafeCell<Option<FiberHandle>>,
    state: AtomicJobState,
}

unsafe impl Send for JobData {}
unsafe impl Sync for JobData {}

impl JobHandle {
    pub(crate) fn new<F>(spec: crate::builder::JobBuilder, body: F) -> (Self, bool)
    where
        F: FnOnce() + Send + 'static,
    {
        let j = JobHandle {
            inner: Arc::new(JobData {
                name: spec.name,
                body: UnsafeCell::new(Some(Box::new(body))),
                priority: spec.priority,
                remaining_dependencies: AtomicU32::new(spec.dependencies.len() as u32),
                dependencies: spec.dependencies.iter().map(|d| d.downgrade()).collect(),
                dependents: RwLock::new(SmallVec::new()),
                enqueued: AtomicBool::new(false),

                fiber_stack: UnsafeCell::new(None),
                fiber: UnsafeCell::new(None),
                state: AtomicJobState::new(JobState::New),
            }),
        };

        let mut push_to_global_queue = true;
        {
            for dependency in &spec.dependencies {
                // Add this job as a dependent to each dependency, subtracting from the dependency counter if the dependency is already completed
                // We keep the write lock during this check, so that we don't miss a completion that happens after pushing but before checking is_completed
                let mut dependents = dependency
                    .inner
                    .dependents
                    .write()
                    .expect("Failed to acquire JobHandle dependents write lock");

                if dependency.is_completed() {
                    j.inner
                        .remaining_dependencies
                        .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                } else {
                    push_to_global_queue = false;
                    dependents.push(j.clone());
                }
            }
        }

        // If there are no remaining dependencies, we can push this job to the global queue
        if j.inner
            .remaining_dependencies
            .load(std::sync::atomic::Ordering::Acquire)
            == 0
        {
            push_to_global_queue = true;
        }

        if push_to_global_queue {
            // Set enqueued to true, and if it was previously false, we can push to the global queue
            push_to_global_queue = !j
                .inner
                .enqueued
                .swap(true, std::sync::atomic::Ordering::AcqRel);
        }

        (j, push_to_global_queue)
    }

    /// Returns the name of the job.
    pub fn name(&self) -> &str {
        &self.inner.name
    }

    /// Returns the current state of the job.
    pub fn state(&self) -> JobState {
        self.inner.state.load()
    }

    /// Returns whether the job is currently running.
    pub fn is_running(&self) -> bool {
        self.state() == JobState::Running
    }

    /// Returns whether the job is currently yielded.
    pub fn is_yielded(&self) -> bool {
        self.state() == JobState::Yielded
    }

    /// Returns whether the job has completed.
    pub fn is_completed(&self) -> bool {
        self.state() == JobState::Completed
    }

    /// Returns the dependencies of the job.
    ///
    /// Note that these are weak references in order to avoid cyclic references internally.
    pub fn dependencies(&self) -> &[JobHandleWeak] {
        &self.inner.dependencies
    }

    /// Returns the number of dependencies that have not yet completed.
    pub fn remaining_dependencies(&self) -> u32 {
        self.inner
            .remaining_dependencies
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Returns the priority of the job.
    pub fn priority(&self) -> Priority {
        self.inner.priority
    }

    pub(crate) fn set_state(&self, new_state: JobState) {
        self.inner.state.store(new_state);
    }

    pub(crate) unsafe fn take_body(&self) -> Option<Box<dyn FnOnce() + Send + 'static>> {
        unsafe { (*self.inner.body.get()).take() }
    }

    /// Waits for the job to complete.
    ///
    /// This function will block the current thread until the job is completed, UNLESS it is called from within another job,
    /// in which case it will yield the current job back to the scheduler.
    ///
    /// If you need to poll a long running job, consider using `is_complete` or `wait_timeout` instead.
    ///
    /// # Panics
    ///
    /// Panics if a job attempts to wait on itself.
    pub fn wait(&self) {
        profiling::scope!("JobHandle::wait", &format!("name={}", self.name()));

        if let Some(current_job) = FiberContext::current_job() {
            if &raw const *current_job.inner.as_ref() == &raw const *self.inner.as_ref() {
                panic!("A job cannot wait on itself!");
            }

            self.wait_async(current_job);
        } else {
            self.wait_blocking();
        }
    }

    /// Waits for the job to complete.
    ///
    /// This function will block the current thread until the job is completed.
    ///
    /// If you need to poll a long running job, consider using `is_complete` or `wait_timeout` instead.
    pub fn wait_blocking(&self) {
        profiling::scope!("JobHandle::wait_blocking", &format!("name={}", self.name()));
        while !self.is_completed() {
            std::thread::yield_now();
        }
    }

    /// Waits for the job to complete.
    ///
    /// This function will block the current thread until the job is completed.
    ///
    /// If you need to poll a long running job, consider using `is_complete` or `wait_timeout` instead.
    fn wait_async(&self, current_job: JobHandle) {
        profiling::scope!("JobHandle::wait_async", &format!("name={}", self.name()));

        // Add current job as a dependent of this job. Once the job completes, it will re-enqueue the current (yielded) job.
        {
            let mut dependents = self.inner.dependents.write().expect(
                "Failed to acquire JobHandle dependents write lock while adding waiting job as dependent",
            );

            // Double-check completion to avoid race
            if self.is_completed() {
                // Job completed while we were setting up, don't yield
                current_job.set_state(JobState::Running);
                return;
            }

            // Increment dependency counter to prevent re-execution
            current_job
                .inner
                .remaining_dependencies
                .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

            dependents.push(current_job.clone());
        }

        assert!(
            FiberContext::yield_current(),
            "Job was not yielded even though we're yielding from a running job/fiber?"
        );
    }

    /// Creates a JobWaker that can be used to wake this job when it's yielded.
    pub fn create_waker(&self, scheduler: &Scheduler) -> JobWaker {
        self.inner
            .remaining_dependencies
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel);

        JobWaker {
            handle: self.clone(),
            scheduler: scheduler.clone(),
            waked: AtomicBool::new(false),
        }
    }

    /// Waits for the job to complete, returning a WaitResult indicating whether it completed or timed out
    pub fn wait_timeout(&self, timeout: Duration) -> WaitResult {
        profiling::scope!(
            "JobHandle::wait_timeout",
            &format!("name={}, timeout={:?}", self.name(), timeout)
        );
        let start = std::time::Instant::now();
        while !self.is_completed() {
            if start.elapsed() >= timeout {
                return WaitResult::Timeout;
            }
            std::thread::yield_now();
        }
        WaitResult::Completed
    }

    /// Downgrades the JobHandle to a weak reference.
    pub fn downgrade(&self) -> JobHandleWeak {
        JobHandleWeak {
            inner: Arc::downgrade(&self.inner),
        }
    }
}

impl JobHandleWeak {
    /// Upgrades the weak reference to a strong reference, if the job is still alive.
    ///
    /// Returns `None` if all strong references have already been dropped.
    pub fn upgrade(&self) -> Option<JobHandle> {
        self.inner.upgrade().map(|inner| JobHandle { inner })
    }
}

/// The result of a wait operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum WaitResult {
    /// The job has completed.
    Completed,
    /// The wait operation timed out.
    Timeout,
}

/// A weak reference to a scheduled job.
#[derive(Clone)]
pub struct JobHandleWeak {
    pub(crate) inner: Weak<JobData>,
}

#[repr(u8)]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum JobState {
    New = 0,
    Running = 1,
    Yielded = 2,
    Completed = 3,
}

pub struct AtomicJobState(AtomicU8);

impl AtomicJobState {
    pub fn new(state: JobState) -> Self {
        AtomicJobState(AtomicU8::new(state as u8))
    }

    pub fn load(&self) -> JobState {
        // SAFETY: AtomicJobState can only hold valid JobState values
        unsafe { std::mem::transmute(self.0.load(std::sync::atomic::Ordering::Acquire)) }
    }

    pub fn store(&self, new_value: JobState) {
        self.0
            .store(new_value as u8, std::sync::atomic::Ordering::Release);
    }
}

impl Default for AtomicJobState {
    fn default() -> Self {
        Self::new(JobState::New)
    }
}

/// A single-use signal to wake a yielded job.
///
/// Multiple signals can be created for the same job, but the job will only be resumed once all signals have been used.
///
/// When the JobWaker is dropped, it will automatically call `wake`, ensuring that the job is resumed even if the waker goes out of scope.
pub struct JobWaker {
    handle: JobHandle,
    scheduler: Scheduler,
    waked: AtomicBool,
}

impl JobWaker {
    /// Wakes the job associated with this waker, re-enqueuing it in the scheduler.
    pub fn wake(self) {
        self.wake_internal();
    }

    fn wake_internal(&self) {
        if self.waked.swap(true, std::sync::atomic::Ordering::AcqRel) {
            // Already waked
            return;
        }

        let remaining = self
            .handle
            .inner
            .remaining_dependencies
            .fetch_sub(1, std::sync::atomic::Ordering::AcqRel);

        // Sanity check. If the last value was 0, we counted a dependency too many somewhere
        debug_assert!(
            remaining > 0,
            "Job {} has negative remaining dependencies",
            self.handle.name()
        );

        if remaining == 1 {
            let is_already_enqueued = self
                .handle
                .inner
                .enqueued
                .swap(true, std::sync::atomic::Ordering::AcqRel);
            if is_already_enqueued {
                // This can happen if a job waker is used when the job is actually already enqueued
                return;
            }

            // All dependencies are complete, schedule the dependent job
            self.scheduler.push_job(self.handle.clone());
        }
    }

    pub fn into_raw(self) -> *mut Self {
        Box::into_raw(Box::new(self))
    }

    pub unsafe fn from_raw(ptr: *mut Self) -> Self {
        unsafe { *Box::from_raw(ptr) }
    }
}

impl Drop for JobWaker {
    fn drop(&mut self) {
        self.wake_internal();
    }
}
