use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicBool, AtomicU32};
use std::sync::{Arc, RwLock, Weak};
use std::time::Duration;

use smallvec::SmallVec;

use crate::builder::Priority;
use crate::util::SharedString;

/// A shared handle to a scheduled job.
#[derive(Clone)]
pub struct JobHandle {
    pub(crate) inner: Arc<JobHandleInner>,
}

pub(crate) struct JobHandleInner {
    name: SharedString,
    completed: AtomicBool,
    body: UnsafeCell<Option<Box<dyn FnOnce() + Send + 'static>>>,
    pub(crate) priority: Priority,

    dependencies: SmallVec<[JobHandleWeak; 2]>,
    pub(crate) dependents: RwLock<SmallVec<[JobHandle; 2]>>,
    pub(crate) remaining_dependencies: AtomicU32,
    pub(crate) enqueued: AtomicBool,
}

unsafe impl Sync for JobHandleInner {}

impl JobHandle {
    pub(crate) fn new<F>(spec: crate::builder::JobBuilder, body: F) -> (Self, bool)
    where
        F: FnOnce() + Send + 'static,
    {
        let j = JobHandle {
            inner: Arc::new(JobHandleInner {
                name: spec.name,
                completed: AtomicBool::new(false),
                body: UnsafeCell::new(Some(Box::new(body))),
                priority: spec.priority,
                remaining_dependencies: AtomicU32::new(spec.dependencies.len() as u32),
                dependencies: spec.dependencies.iter().map(|d| d.downgrade()).collect(),
                dependents: RwLock::new(SmallVec::new()),
                enqueued: AtomicBool::new(false),
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

    /// Returns whether the job has completed.
    pub fn is_completed(&self) -> bool {
        self.inner
            .completed
            .load(std::sync::atomic::Ordering::Acquire)
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

    pub(crate) fn set_completed(&self) {
        self.inner
            .completed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    pub(crate) unsafe fn take_body(&self) -> Option<Box<dyn FnOnce() + Send + 'static>> {
        unsafe { (*self.inner.body.get()).take() }
    }

    /// Waits for the job to complete.
    ///
    /// This function will block the current thread until the job is completed.
    ///
    /// If you need to poll a long running job, consider using `is_complete` or `wait_timeout` instead.
    pub fn wait(&self) {
        profiling::scope!("JobHandle::wait", &format!("name={}", self.name()));
        while !self.is_completed() {
            std::thread::yield_now();
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
    pub(crate) inner: Weak<JobHandleInner>,
}
