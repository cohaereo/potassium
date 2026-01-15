use potassium::{JobWaker, Priority};

fn main() {
    let scheduler = potassium::Scheduler::with_workers(1);

    // This would run fine on 3 workers, but with only 1 worker it will deadlock, *unless* the scheduler yields jobs that are waiting.
    // With job yielding, JobHandle::wait() will yield if called from within a job, preventing this deadlock.
    println!("Starting job 0");
    let scheduler_clone = scheduler.clone();
    let job1 = scheduler
        .job_builder("job_1")
        .priority(Priority::Medium)
        .spawn(move || {
            println!("  Starting job 1");
            let scheduler_clone2 = scheduler_clone.clone();
            let job2 = scheduler_clone
                .job_builder("job_2")
                .priority(Priority::Medium)
                .spawn(move || {
                    println!("    Starting job 2");
                    let job3 = scheduler_clone2
                        .job_builder("job_3")
                        .priority(Priority::Medium)
                        .spawn(|| {
                            println!("      Hello from job 3!");
                        });
                    job3.wait();
                    println!("    Job 2 has finished");
                });
            job2.wait();
            println!("  Job 1 has finished");
        });

    job1.wait();
    println!("Job 0 has finished");

    scheduler
        .job_builder("job_middleware_async")
        .priority(Priority::Medium)
        .spawn(move || {
            println!("job_middleware_async: Starting middleware task thread");
            let waker = potassium::create_waker().unwrap();
            // Turn the waker into a raw pointer, this can be passed as user data to eg. a C callback
            // Avoid doing this in native (safe) Rust code, as this is intended for FFI scenarios
            let user_data = waker.into_raw() as usize;
            std::thread::Builder::new()
                .name("middleware-thread".into())
                .spawn(move || {
                    let waker = unsafe { JobWaker::from_raw(user_data as _) };
                    println!("  Middleware task starting expensive work");
                    for i in 0..5 {
                        std::thread::sleep(std::time::Duration::from_millis(100));
                        println!("  Middleware task working... {}/5", i + 1);
                    }
                    println!("  Middleware task finished work, waking job");
                    waker.wake();
                })
                .unwrap();

            println!("job_middleware_async: yielding to wait for async work");
            potassium::yield_job();
            println!("job_middleware_async: resumed after yield");
        });

    // Spawn some low priority jobs that can run while the middleware job is running, despite only having 1 worker
    for i in 0..4 {
        scheduler
            .job_builder(format!("misc_job_{i}"))
            .priority(Priority::Low)
            .spawn(move || {
                println!("misc_job_{i}: Doing some low priority work while waiting for middleware");
            });
    }

    scheduler.wait_for_all();

    scheduler.shutdown();
}
