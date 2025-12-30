#![doc = include_str!("../README.md")]
#![warn(missing_docs, clippy::missing_safety_doc)]

mod builder;
mod job;
mod scheduler;
mod util;
mod worker;

pub use builder::{JobBuilder, Priority};
pub use job::{JobHandle, JobHandleWeak};
pub use scheduler::Scheduler;
pub use util::SharedString;
