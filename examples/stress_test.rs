//! This example schedules as many empty jobs as possible within one second to measure the overhead of job scheduling.

use potassium::{Priority, Scheduler};

fn main() {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let mut worker_counts = vec![1, 2, cpus, cpus * 2];
    if !worker_counts.contains(&(cpus / 2)) && cpus > 2 {
        worker_counts.push(cpus / 2);
        worker_counts.sort_unstable();
    }

    for with_dependencies in [false, true] {
        for &num_workers in &worker_counts {
            println!(
                "Running stress test with {} workers (dependencies={with_dependencies}):",
                num_workers
            );
            run_stress_test(num_workers, with_dependencies);
            println!();
        }
    }
}

fn run_stress_test(num_workers: usize, dependencies: bool) {
    let scheduler = Scheduler::with_workers(num_workers);
    scheduler.pause();

    let start = std::time::Instant::now();
    while start.elapsed().as_secs_f32() < 1.0 {
        let job1 = scheduler
            .job_builder("empty")
            .priority(Priority::Low)
            .spawn(|| {});

        if dependencies {
            let _job2 = scheduler
                .job_builder("empty_dep")
                .priority(Priority::Low)
                .dependencies(vec![job1])
                .spawn(|| {});
        }
    }

    let num_jobs = scheduler.num_jobs_queued();
    println!("  Scheduled {} jobs", num_jobs);

    let start = std::time::Instant::now();
    scheduler.resume();
    scheduler.wait_for_all();
    let duration = start.elapsed();
    println!(
        "  All jobs completed in {:?} (mean {:?} per job)",
        duration,
        duration / num_jobs as u32
    );
}
