use crate::job::JobHandle;
use crate::scheduler::Scheduler;
use crate::util::SharedString;
use smallvec::SmallVec;

pub struct JobBuilder<'a> {
    _scheduler: &'a Scheduler,

    pub name: SharedString,
    pub priority: Priority,

    pub dependencies: SmallVec<[JobHandle; 2]>,
}

impl<'a> JobBuilder<'a> {
    pub fn builder(scheduler: &'a Scheduler, name: impl Into<SharedString>) -> Self {
        JobBuilder {
            _scheduler: scheduler,
            name: name.into(),
            priority: Priority::Medium,
            dependencies: SmallVec::new(),
        }
    }

    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    pub fn dependencies<I>(mut self, dependencies: I) -> Self
    where
        I: IntoIterator<Item = JobHandle>,
    {
        self.dependencies.extend(dependencies);
        self
    }

    pub fn spawn<F>(self, body: F) -> JobHandle
    where
        F: FnOnce() + Send + 'static,
    {
        self._scheduler.spawn(self, body)
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub enum Priority {
    Low,
    Medium,
    High,
}

impl Priority {
    pub const ALL: [Priority; 3] = [Priority::High, Priority::Medium, Priority::Low];
    pub const COUNT: usize = 3;
}
