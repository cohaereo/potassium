use potassium::scheduler::Scheduler;
use potassium::spec::Priority;

fn main() {
    let scheduler = Scheduler::new();
    println!("Running with {} workers", scheduler.num_workers());

    let _big_job = scheduler
        .job_builder("big_job")
        .priority(Priority::Low)
        .spawn(|| {
            std::thread::sleep(std::time::Duration::from_secs(2));
            println!("Big job completed.");
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
    scheduler.wait_for_all();
    println!("All jobs completed.");
    scheduler.shutdown();
}
