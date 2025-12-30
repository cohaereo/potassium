use crate::job::JobHandle;
use crate::scheduler::Scheduler;
use crate::util::SharedString;
use smallvec::SmallVec;

/// Builder for creating and configuring jobs to be scheduled.
pub struct JobBuilder<'a> {
    _scheduler: &'a Scheduler,

    /// Name of the job. Used for debugging and profiling.
    pub name: SharedString,
    /// Priority of the job. Defaults to `Priority::Medium`.
    pub priority: Priority,

    /// Dependencies that must complete before this job can run.
    pub dependencies: SmallVec<[JobHandle; 2]>,
}

impl<'a> JobBuilder<'a> {
    /// Create a new JobBuilder with the given scheduler and job name.
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::{Scheduler, Priority};
    ///
    /// let scheduler = Scheduler::new();
    /// let job_builder = scheduler.job_builder("example_job");
    ///
    /// assert_eq!(&*job_builder.name, "example_job");
    /// assert_eq!(job_builder.priority, Priority::Medium);
    /// assert!(job_builder.dependencies.is_empty());
    /// ```
    pub fn builder(scheduler: &'a Scheduler, name: impl Into<SharedString>) -> Self {
        JobBuilder {
            _scheduler: scheduler,
            name: name.into(),
            priority: Priority::Medium,
            dependencies: SmallVec::new(),
        }
    }

    /// Set the priority of the job.
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::{Scheduler, Priority};
    ///
    /// let scheduler = Scheduler::new();
    /// let job_builder = scheduler
    ///     .job_builder("example_job")
    ///     .priority(Priority::High);
    ///
    /// assert_eq!(job_builder.priority, Priority::High);
    /// ```
    pub fn priority(mut self, priority: Priority) -> Self {
        self.priority = priority;
        self
    }

    /// Add dependencies that must complete before this job can run.
    ///
    /// # Examples
    ///
    /// ```
    /// use potassium::{Scheduler, Priority};
    ///
    /// let scheduler = Scheduler::new();
    /// let dep_job = scheduler
    ///     .job_builder("dependency_job")
    ///     .spawn(|| {});
    ///
    /// let job_builder = scheduler
    ///     .job_builder("example_job")
    ///     .dependencies(vec![dep_job]);
    ///
    /// assert_eq!(job_builder.dependencies.len(), 1);
    /// ```
    pub fn dependencies<I>(mut self, dependencies: I) -> Self
    where
        I: IntoIterator<Item = JobHandle>,
    {
        self.dependencies.extend(dependencies);
        self
    }

    /// Spawns the job with the given body.
    ////
    /// # Examples
    ///
    /// ```
    /// use potassium::Scheduler;
    ///
    /// let scheduler = Scheduler::new();
    /// let job_handle = scheduler
    ///     .job_builder("example_job")
    ///     .spawn(|| {
    ///         println!("Job is running!");
    ///     });
    /// ```
    pub fn spawn<F>(self, body: F) -> JobHandle
    where
        F: FnOnce() + Send + 'static,
    {
        self._scheduler.spawn(self, body)
    }
}

/// Priority levels for jobs
#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub enum Priority {
    /// Low priority. Executed after Medium and High priority jobs.
    Low,
    /// Medium priority. Executed after High priority jobs but before Low priority jobs.
    Medium,
    /// High priority. Executed before Medium and Low priority jobs.
    High,
}

impl Priority {
    /// An array of all priority levels, sorted from highest to lowest.
    pub const ALL: [Priority; 3] = [Priority::High, Priority::Medium, Priority::Low];
    /// The number of priority levels.
    pub const COUNT: usize = 3;
}
