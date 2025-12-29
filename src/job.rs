use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32};

use parking_lot::RwLock;
use smallvec::SmallVec;

use crate::spec::Priority;
use crate::util::SharedString;

#[derive(Clone)]
pub struct JobHandle {
    pub(crate) inner: Arc<JobHandleInner>,
}

pub(crate) struct JobHandleInner {
    name: SharedString,
    completed: AtomicBool,
    body: UnsafeCell<Option<Box<dyn FnOnce() + Send + 'static>>>,
    pub(crate) priority: Priority,

    dependencies: SmallVec<[JobHandle; 2]>,
    pub(crate) dependents: RwLock<SmallVec<[JobHandle; 2]>>,
    pub(crate) remaining_dependencies: AtomicU32,
}

unsafe impl Sync for JobHandleInner {}

impl JobHandle {
    pub(crate) fn new<F>(spec: crate::spec::JobBuilder, body: F) -> (Self, bool)
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
                dependencies: spec.dependencies,
                dependents: RwLock::new(SmallVec::new()),
            }),
        };

        let mut push_to_global_queue = true;
        {
            for dependency in &j.inner.dependencies {
                // Add this job as a dependent to each dependency, subtracting from the dependency counter if the dependency is already completed
                // We keep the write lock during this check, so that we don't miss a completion that happens after pushing but before checking is_completed
                let mut dependents = dependency.inner.dependents.write();
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

        (j, push_to_global_queue)
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn is_completed(&self) -> bool {
        self.inner
            .completed
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn dependencies(&self) -> &[JobHandle] {
        &self.inner.dependencies
    }

    pub fn remaining_dependencies(&self) -> u32 {
        self.inner
            .remaining_dependencies
            .load(std::sync::atomic::Ordering::Acquire)
    }

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

    pub fn wait(&self) {
        while !self.is_completed() {
            std::thread::yield_now();
        }
    }
}
