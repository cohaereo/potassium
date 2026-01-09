use potassium::{Priority, Scheduler};

fn main() {
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

    scheduler.pause(); // Pause the scheduler in order to demonstrate priority ordering
    let small_jobs: Vec<_> = (0..scheduler.num_workers() * 2)
        .map(|i| {
            let priority = if i % 2 == 0 {
                Priority::High
            } else {
                Priority::Medium
            };

            scheduler
                .job_builder(format!("small_job_{}", i))
                .priority(priority)
                .spawn(move || {
                    std::thread::sleep(std::time::Duration::from_millis(100 + i as u64 * 10));
                    println!("Small job {i} completed (priority {priority:?})");
                })
        })
        .collect();
    scheduler.resume();

    let job_sync = scheduler
        .job_builder("sync_job")
        .priority(Priority::Medium)
        .dependencies(small_jobs)
        .spawn(|| {
            println!("All small jobs completed. Running sync job.");
        });

    job_sync.wait();
    println!("Small job sync completed. Big job should finish shortly.");
    let long_job_duration = long_job.wait();
    println!(
        "Long job took {:.2?} seconds",
        long_job_duration.as_secs_f32()
    );

    scheduler.wait_for_all();
    println!("All jobs completed.");
    scheduler.shutdown();
}
