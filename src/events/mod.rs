pub mod chrome;
pub mod mermaid;

use crate::JobHandle;

/// Represents an event that occurs during the execution of a job.
#[allow(missing_docs)]
#[derive(Debug, Clone)]
pub enum Event {
    /// The job has started execution.
    JobStarted { worker_id: usize, handle: JobHandle },
    /// The job has yielded execution.
    JobYielded { worker_id: usize, handle: JobHandle },
    /// The job has resumed execution.
    JobResumed { worker_id: usize, handle: JobHandle },
    /// The job has finished execution.
    JobCompleted { worker_id: usize, handle: JobHandle },
}

impl Event {
    /// Returns the worker ID associated with this event.
    pub fn worker_id(&self) -> usize {
        match self {
            Event::JobStarted { worker_id, .. }
            | Event::JobYielded { worker_id, .. }
            | Event::JobResumed { worker_id, .. }
            | Event::JobCompleted { worker_id, .. } => *worker_id,
        }
    }
}

/// Handles events emitted by job workers when they start/finish working on jobs.
pub trait EventHandler: Send + Sync {
    /// Handles an event emitted by a job worker.
    ///
    /// This function is called from within the worker threads, so it's recommended to defer or offload expensive operations.
    fn handle_event(&self, event: Event);
}
