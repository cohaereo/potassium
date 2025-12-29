use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use potassium::scheduler::Scheduler;

fn main() {
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let mut worker_count = vec![1, 2, cpus, cpus * 2];
    if !worker_count.contains(&(cpus / 2)) && cpus > 2 {
        worker_count.push(cpus / 2);
        worker_count.sort_unstable();
    }

    for &worker_count in &worker_count {
        let scheduler = Scheduler::with_workers(worker_count);
        for (kind_name, num_operations) in [
            // ("tiny_jobs", 1_000),
            ("small_job", 10_000),
            ("job", 1_000_000),
            ("big_job", 10_000_000),
        ] {
            for job_count in [100, 1_000] {
                let baseline_start = std::time::Instant::now();
                let baseline_counter = Arc::new(AtomicUsize::new(0));
                for _ in 0..job_count {
                    let c = Arc::clone(&baseline_counter);
                    // Small amount of work
                    let mut sum = 0u64;
                    for i in 0..num_operations {
                        sum = std::hint::black_box(sum.wrapping_add(i));
                    }
                    c.fetch_add(sum as usize, Ordering::Relaxed);
                }
                let baseline_duration = baseline_start.elapsed();
                let _ = std::hint::black_box(baseline_counter.load(Ordering::Relaxed));
                println!(
                    "many_{kind_name:12} with {:4} jobs,   baseline, completed in {:14?}, avg time per job: {:?}",
                    job_count,
                    baseline_duration,
                    baseline_duration / job_count as u32
                );

                let counter = Arc::new(AtomicUsize::new(0));

                let start = std::time::Instant::now();
                scheduler.pause();
                for _ in 0..job_count {
                    let c = Arc::clone(&counter);

                    scheduler
                        .job_builder(kind_name)
                        .priority(potassium::spec::Priority::Medium)
                        .spawn(move || {
                            // Small amount of work
                            let mut sum = 0u64;
                            for i in 0..num_operations {
                                sum = std::hint::black_box(sum.wrapping_add(i));
                            }
                            c.fetch_add(sum as usize, Ordering::Relaxed);
                        });
                }
                scheduler.resume();

                scheduler.wait_for_all();
                let duration = start.elapsed();
                let count = std::hint::black_box(counter.load(Ordering::Relaxed));
                let expected =
                    (0..num_operations).fold(0u64, |acc, x| acc.wrapping_add(x)) * job_count as u64;
                assert_eq!(count as u64, expected);

                let speed_relative = baseline_duration.as_secs_f64() / duration.as_secs_f64();
                println!(
                    "many_{kind_name:12} with {:4} jobs, {:2} workers, completed in {:14?}, avg time per job: {:14?} ({:5.1}% relative to baseline)",
                    job_count,
                    worker_count,
                    duration,
                    duration / job_count as u32,
                    speed_relative * 100.0
                );
            }
        }

        scheduler.shutdown();
    }
}
