use crate::scheduler::{JobId, Scheduler};
use crate::spec::job_spec_builder::{IsComplete, IsUnset, SetCondition, State};
use crate::util::SharedString;
use smallvec::SmallVec;

#[derive(bon::Builder)]
pub struct JobSpec {
    #[builder(start_fn, into)]
    pub name: SharedString,
    pub priority: Priority,

    #[builder(default, into)]
    pub dependencies: SmallVec<[JobId; 4]>,

    #[builder(setters(vis = "", name = condition_internal))]
    pub condition: Option<Box<dyn JobCondition>>,
}

impl<S: State> JobSpecBuilder<S> {
    pub fn schedule<F>(self, scheduler: &Scheduler, body: F) -> JobId
    where
        S: IsComplete,
        F: FnOnce() + Send + 'static,
    {
        let spec = self.build();
        scheduler.schedule_job(spec, body)
    }

    pub fn condition(
        self,
        condition: impl JobCondition + 'static,
    ) -> JobSpecBuilder<SetCondition<S>>
    where
        S::Condition: IsUnset,
    {
        self.condition_internal(Box::new(condition))
    }
}

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub enum Priority {
    Low,
    Medium,
    High,
}

impl Priority {
    pub const COUNT: usize = 3;
}

pub trait JobCondition: Send {
    fn is_met(&self) -> bool;
}

impl<F> JobCondition for F
where
    F: Fn() -> bool + Send,
{
    fn is_met(&self) -> bool {
        self()
    }
}
