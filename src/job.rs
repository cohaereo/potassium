use crate::spec::Priority;
use crate::util::SharedString;
use slotmap::new_key_type;

new_key_type! {
    pub struct JobId;
}

pub(crate) struct Job {
    pub _name: SharedString,
    pub priority: Priority,
    pub body: Box<dyn FnOnce() + Send + 'static>,
}

impl Job {
    pub fn new<F>(spec: crate::spec::JobSpec, body: F) -> Self
    where
        F: FnOnce() + Send + 'static,
    {
        Job {
            _name: spec.name,
            priority: spec.priority,
            body: Box::new(body),
        }
    }
}
