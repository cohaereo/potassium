//! This example schedules as many empty jobs as possible within one second to measure the overhead of job scheduling.

use potassium::{Priority, Scheduler, SchedulerConfiguration};

fn main() {
    #[rustfmt::skip]
    let configurations = [
        (SchedulerConfiguration::single_core(), "single core"),
        (SchedulerConfiguration::with_cores_autopin(2), "2 cores pinned"),
        (SchedulerConfiguration::all_cores_unpinned(), "all cores unpinned"),
        (SchedulerConfiguration::all_cores_pinned(), "all cores pinned"),
        (SchedulerConfiguration::all_logical_cores_pinned(), "all logical cores pinned"),
    ];

    for with_dependencies in [false, true] {
        for (config, config_name) in &configurations {
            println!(
                "Running stress test with config '{config_name} ({})' (dependencies={with_dependencies}):",
                config.workers.len()
            );
            run_stress_test(config, with_dependencies);
            println!();
        }
    }
}

fn run_stress_test(config: &SchedulerConfiguration, dependencies: bool) {
    let scheduler = Scheduler::new(config);
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
    scheduler.shutdown();
}
