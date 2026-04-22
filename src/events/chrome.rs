//! Renders events to Chrome's trace JSON format.

use std::{fmt::Write, sync::Mutex, time::Instant};

use crate::{Event, JobHandle, events::EventHandler, util::MutexExt};

/// Renders job execution timelines to Chrome's trace JSON format.
pub struct ChromeTraceRenderer {
    start_time: Instant,
    events_by_worker: Mutex<Vec<Vec<(Instant, Event)>>>,
}

impl EventHandler for ChromeTraceRenderer {
    fn handle_event(&self, event: Event) {
        let mut events = self.events_by_worker.lock2();
        let worker_id = event.worker_id();
        if worker_id >= events.len() {
            events.resize(worker_id + 1, Vec::new());
        }
        events[worker_id].push((Instant::now(), event));
    }
}

impl ChromeTraceRenderer {
    /// Render the collected events to Chrome's trace JSON format.
    pub fn render(&self) -> String {
        let events_by_worker = std::mem::take(&mut *self.events_by_worker.lock2());

        struct JobBlock {
            worker_id: usize,
            start: Instant,
            end: Instant,
            job: JobHandle,
        }

        let mut blocks = Vec::new();
        for (worker_id, events) in events_by_worker.iter().enumerate() {
            let mut current_block = None;
            for (time, event) in events {
                match event {
                    Event::JobStarted { handle, .. } => {
                        if current_block.is_some() {
                            panic!("JobStarted event while another job is running");
                        }

                        current_block = Some(JobBlock {
                            worker_id,
                            start: *time,
                            end: *time,
                            job: handle.clone(),
                        });
                    }
                    Event::JobYielded { .. } => {
                        let Some(mut block) = current_block.take() else {
                            continue;
                        };

                        block.end = *time;
                        blocks.push(block);
                    }
                    Event::JobResumed { handle, .. } => {
                        if current_block.is_some() {
                            panic!("JobResumed event while another job is running");
                        }

                        current_block = Some(JobBlock {
                            worker_id,
                            start: *time,
                            end: *time,
                            job: handle.clone(),
                        });
                    }
                    Event::JobCompleted { .. } => {
                        let Some(mut block) = current_block.take() else {
                            continue;
                        };
                        block.end = *time;
                        blocks.push(block);
                    }
                }
            }
        }

        let mut s = String::new();
        writeln!(&mut s, r#"{{"#).ok();
        writeln!(&mut s, r#"  "traceEvents": ["#).ok();
        for (i, block) in blocks.iter().enumerate() {
            writeln!(
                &mut s,
                r#"    {{"name": {:?}, "cat": "TASK", "ph": "B", "pid": 0, "tid": {}, "ts": {}}},"#,
                block.job.name(),
                block.worker_id,
                block.start.duration_since(self.start_time).as_micros()
            )
            .ok();
            write!(
                &mut s,
                r#"    {{"name": {:?}, "cat": "TASK", "ph": "E", "pid": 0, "tid": {}, "ts": {}}}"#,
                block.job.name(),
                block.worker_id,
                block.end.duration_since(self.start_time).as_micros()
            )
            .ok();
            if i < blocks.len() - 1 {
                writeln!(&mut s, ",").ok();
            } else {
                writeln!(&mut s).ok();
            }
        }

        writeln!(&mut s, r#"  ],"#).ok();
        writeln!(&mut s, r#"  "displayTimeUnit": "ns","#).ok();
        writeln!(&mut s, r#"  "otherData": {{"#).ok();
        writeln!(
            &mut s,
            r#"    "version": "Potassium v{}""#,
            env!("CARGO_PKG_VERSION")
        )
        .ok();
        writeln!(&mut s, r#"  }}"#).ok();
        writeln!(&mut s, r#"}}"#).ok();

        s
    }
}

impl Default for ChromeTraceRenderer {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
            events_by_worker: Mutex::new(Vec::new()),
        }
    }
}
