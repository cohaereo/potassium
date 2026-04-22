//! Render Mermaid Gantt diagrams from event logs.

use std::{fmt::Write, sync::Mutex, time::Instant};

use crate::{Event, JobHandle, events::EventHandler, util::MutexExt};

/// Renders job execution timelines as Mermaid Gantt diagrams.
pub struct MermaidRenderer {
    start_time: Instant,
    events_by_worker: Mutex<Vec<Vec<(Instant, Event)>>>,
}

impl EventHandler for MermaidRenderer {
    fn handle_event(&self, event: Event) {
        let mut events = self.events_by_worker.lock2();
        let worker_id = event.worker_id();
        if worker_id >= events.len() {
            events.resize(worker_id + 1, Vec::new());
        }
        events[worker_id].push((Instant::now(), event));
    }
}

impl MermaidRenderer {
    /// Render the collected events as a Mermaid Gantt diagram.
    pub fn render(&self) -> String {
        let mut s = String::new();

        writeln!(&mut s, "gantt").ok();
        writeln!(&mut s, "\ttitle Potassium Job Timeline").ok();
        writeln!(&mut s, "\tdateFormat x").ok();
        writeln!(&mut s, "\taxisFormat %Lus").ok();

        writeln!(&mut s).ok();

        let events_by_worker = std::mem::take(&mut *self.events_by_worker.lock2());

        struct JobBlock {
            start: Instant,
            end: Instant,
            job: JobHandle,
        }

        for (worker_id, events) in events_by_worker.iter().enumerate() {
            writeln!(&mut s, "\tsection Worker {}", worker_id).ok();
            let mut blocks = Vec::new();
            let mut current_block = None;
            for (time, event) in events {
                match event {
                    Event::JobStarted { handle, .. } => {
                        if current_block.is_some() {
                            panic!("JobStarted event while another job is running");
                        }

                        current_block = Some(JobBlock {
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

            for block in blocks {
                let start = block.start.duration_since(self.start_time);
                let end = block.end.duration_since(self.start_time);
                writeln!(
                    &mut s,
                    "\t{}: {}, {}",
                    block.job.name(),
                    start.as_micros(),
                    end.as_micros()
                )
                .ok();
            }
        }

        s
    }
}

impl Default for MermaidRenderer {
    fn default() -> Self {
        Self {
            start_time: Instant::now(),
            events_by_worker: Mutex::new(Vec::new()),
        }
    }
}
