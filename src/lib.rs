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

pub fn yield_job() -> bool {
    FiberContext::yield_current()
}

pub fn create_waker() -> Option<job::JobWaker> {
    FiberContext::create_waker()
}
