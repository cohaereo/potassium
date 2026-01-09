# 🍌 Potassium - A lean mean job 'n task machine

Potassium is a job scheduler for modern multi-threaded game engines.

It is designed so that the entire engine can be built around it in order to maximize CPU utilization.

## Features

- Job dependencies
- Priority scheduling
- Configurable worker threads
- [Fiber](https://crates.io/crates/fibrous) support (resumable jobs)
- Jobs with return values

### Planned Features

- 🕑 Job graphs (DAGs) - coming soon

## Example Usage

```rust
use potassium::{Scheduler, Priority};
let scheduler = Scheduler::default();
println!("Running with {} workers", scheduler.num_workers());

let long_job = scheduler
    .job_builder("long_job")
    .priority(Priority::Low)
    .spawn_with_result(|| {
        let start = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_secs(2));
        let duration = start.elapsed();
        println!("Long job completed.");
        duration
    });

let job1 = scheduler
    .job_builder("job1")
    .priority(Priority::High)
    .spawn(|| {
        // Doing some hard work
        std::thread::sleep(std::time::Duration::from_millis(200));
    });

let job2 = scheduler
    .job_builder("job2")
    .priority(Priority::Medium)
    .spawn(|| {
        // Doing some medium work
        std::thread::sleep(std::time::Duration::from_millis(300));
    });

let job_sync = scheduler
    .job_builder("sync_job")
    .priority(Priority::Medium)
    .dependencies(vec![job1, job2])
    .spawn(|| {
        println!("All small jobs completed. Running sync job.");
    });

// Jobs can be awaited using the returned JobHandle
job_sync.wait();
println!("Small job sync completed. Big job should finish shortly.");
let long_job_duration = long_job.wait();
println!(
    "Long job took {:.2?} seconds",
    long_job_duration.as_secs_f32()
);
scheduler.wait_for_all();
println!("All jobs completed.");
```
