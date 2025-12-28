use crate::JobId;
use crate::scheduler::Scheduler;
use crate::spec::job_spec_builder::{IsComplete, State};
use crate::util::SharedString;
use smallvec::SmallVec;

#[derive(bon::Builder)]
pub struct JobSpec<'a> {
    #[builder(start_fn)]
    _scheduler: &'a Scheduler,

    #[builder(start_fn, into)]
    pub name: SharedString,
    pub priority: Priority,

    #[builder(default, into)]
    pub dependencies: SmallVec<[JobId; 4]>,
}

impl<'a, S: State> JobSpecBuilder<'a, S> {
    pub fn spawn<F>(self, body: F) -> JobId
    where
        S: IsComplete,
        F: FnOnce() + Send + 'static,
    {
        self._scheduler.spawn(self, body)
    }
}

impl<'a, S: State> From<JobSpecBuilder<'a, S>> for JobSpec<'a>
where
    S: IsComplete,
{
    fn from(val: JobSpecBuilder<'a, S>) -> Self {
        val.build()
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
