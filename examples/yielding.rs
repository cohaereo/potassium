use potassium::Priority;

fn main() {
    let scheduler = potassium::Scheduler::with_workers(1);

    // This would run fine on 3 workers, but with only 1 worker it will deadlock, *unless* the scheduler yields jobs that are waiting.
    // With job yielding, JobHandle::wait() will yield if called from within a job, preventing this deadlock.
    println!("Starting job 0");
    let scheduler_clone = scheduler.clone();
    let job0 = scheduler
        .job_builder("job_0")
        .priority(Priority::Medium)
        .spawn(move || {
            println!("  Starting job 1");
            let scheduler_clone2 = scheduler_clone.clone();
            let job1 = scheduler_clone
                .job_builder("job_1")
                .priority(Priority::Medium)
                .spawn(move || {
                    println!("    Starting job 2");
                    let job2 = scheduler_clone2
                        .job_builder("job_2")
                        .priority(Priority::Medium)
                        .spawn(|| {
                            println!("      Hello from job 3!");
                        });
                    job2.wait();
                    println!("    Job 2 has finished");
                });
            job1.wait();
            println!("  Job 1 has finished");
        });

    job0.wait();
    println!("Job 0 has finished");

    scheduler.shutdown();
}
