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
    pub fn new<F>(spec: crate::spec::JobSpec, body: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        let num_dependencies_complete = spec
            .dependencies
            .iter()
            .map(|j| j.is_completed() as u32)
            .sum::<u32>();

        let j = JobHandle {
            inner: Arc::new(JobHandleInner {
                name: spec.name,
                completed: AtomicBool::new(false),
                body: UnsafeCell::new(Some(Box::new(body))),
                priority: spec.priority,
                remaining_dependencies: AtomicU32::new(
                    spec.dependencies.len() as u32 - num_dependencies_complete,
                ),
                dependencies: spec.dependencies,
                dependents: RwLock::new(SmallVec::new()),
            }),
        };

        for dep in &j.inner.dependencies {
            dep.push_dependent(j.clone());
        }

        j
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

    pub fn priority(&self) -> Priority {
        self.inner.priority
    }

    fn push_dependent(&self, dependent: JobHandle) {
        let mut dependents = self.inner.dependents.write();
        dependents.push(dependent);
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
