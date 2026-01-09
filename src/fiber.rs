use std::cell::RefCell;

use fibrous::{DefaultFiberApi, FiberApi, FiberHandle};

use crate::{
    JobHandle, Scheduler,
    job::{JobState, JobWaker},
};

pub(crate) struct FiberContext {
    /// The handle to the worker thread's fiber.
    pub worker_fiber: FiberHandle,

    /// The handle to the currently executing job
    pub current_job: Option<JobHandle>,

    /// Reference to the scheduler.
    pub scheduler: Scheduler,
}

thread_local! {
    /// The fiber context for the currently executing fiber.
    pub static FIBER_CONTEXT: RefCell<Option<FiberContext>> = const { RefCell::new(None) };
}

impl FiberContext {
    pub(crate) fn initialize_worker_thread(scheduler: Scheduler) {
        FIBER_CONTEXT.with(|ctx| {
            let worker_fiber = unsafe { DefaultFiberApi::convert_thread_to_fiber() }
                .expect("Failed to convert thread to fiber");

            *ctx.borrow_mut() = Some(FiberContext {
                worker_fiber,
                current_job: None,
                scheduler,
            });
        });
    }

    pub fn current_job() -> Option<JobHandle> {
        FIBER_CONTEXT.with(|ctx| ctx.borrow().as_ref().and_then(|c| c.current_job.clone()))
    }

    pub fn set_current_job(job: Option<JobHandle>) {
        FIBER_CONTEXT.with(|ctx| {
            if let Some(c) = ctx.borrow_mut().as_mut() {
                c.current_job = job;
            }
        });
    }

    pub fn worker_fiber() -> Option<FiberHandle> {
        FIBER_CONTEXT.with(|ctx| ctx.borrow().as_ref().map(|c| c.worker_fiber))
    }

    pub fn scheduler() -> Option<Scheduler> {
        FIBER_CONTEXT.with(|ctx| ctx.borrow().as_ref().map(|c| c.scheduler.clone()))
    }

    pub fn create_waker() -> Option<JobWaker> {
        Self::current_job().map(|j| {
            j.create_waker(Self::scheduler().as_ref().expect(
                "unreachable: Scheduler not found in fiber context, but current job is valid?",
            ))
        })
    }

    /// Yield the current job back to the scheduler
    /// Returns true if yielded, false if not in a fiber context
    pub fn yield_current() -> bool {
        if let Some((current_job, main_fiber)) = FIBER_CONTEXT.with(|ctx| {
            let ctx_ref = ctx.borrow();
            let c = ctx_ref.as_ref()?;
            Some((c.current_job.clone()?, c.worker_fiber))
        }) {
            if current_job.remaining_dependencies() == 0 {
                // No dependencies, no need to yield
                return false;
            }

            if let Some(current_fiber) = unsafe { *current_job.inner.fiber.get() } {
                FiberContext::set_current_job(None);
                current_job.set_state(JobState::Yielded);
                // When we yield, the job leaves the scheduler's context, so mark it as not enqueued so any dependents can re-enqueue it when ready
                current_job
                    .inner
                    .enqueued
                    .store(false, std::sync::atomic::Ordering::Release);
                unsafe {
                    DefaultFiberApi::switch_to_fiber(current_fiber, main_fiber);
                }
                current_job.set_state(JobState::Running);
                FiberContext::set_current_job(Some(current_job));

                true
            } else {
                false
            }
        } else {
            false
        }
    }
}
