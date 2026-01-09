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
pub use job::{JobHandle, JobHandleWeak, WaitResult};
pub use scheduler::Scheduler;
pub use util::SharedString;
