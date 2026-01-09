#![doc = include_str!("../README.md")]
#![warn(missing_docs, clippy::missing_safety_doc)]

mod builder;
mod config;
mod fiber;
mod job;
mod scheduler;
mod util;
mod worker;

pub use builder::{JobBuilder, Priority};
pub use config::{SchedulerConfiguration, ThreadPriority, WorkerConfiguration};
pub use job::{JobHandle, JobHandleWeak, JobWaker, WaitResult};
pub use scheduler::Scheduler;
pub use util::SharedString;

use crate::fiber::FiberContext;

/// Yields execution of the current job, allowing other jobs to run.
///
/// Returns `true` if the job was successfully yielded, `false` otherwise.
///
/// Note: If the job doesn't have any dependencies (e.g. through a JobWaker), then execution will not yield.
pub fn yield_job() -> bool {
    FiberContext::yield_current()
}

/// Creates a `JobWaker` for the current job, if running in a yieldable job context,  None otherwise
pub fn create_waker() -> Option<job::JobWaker> {
    FiberContext::create_waker()
}

/// Returns the `JobHandle` of the currently executing job, if any.
pub fn current_job() -> Option<JobHandle> {
    FiberContext::current_job()
}

/// Returns the `Scheduler` associated with the current job, if any.
pub fn current_scheduler() -> Option<Scheduler> {
    FiberContext::scheduler()
}
