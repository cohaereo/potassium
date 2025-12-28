use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use potassium::scheduler::Scheduler;
use potassium::spec::Priority;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

fn scheduler_benchmark(c: &mut Criterion) {
    // Benchmark 1: Many small jobs with varying worker counts
    let cpus = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);

    let mut worker_count = vec![1, 2, cpus, cpus * 2];
    if !worker_count.contains(&(cpus / 2)) && cpus > 2 {
        worker_count.push(cpus / 2);
        worker_count.sort_unstable();
    }

    for (kind_name, num_operations) in [
        // ("tiny_job", 100),
        ("small_job", 1_000),
        ("job", 100_000),
    ] {
        for job_count in [100, 1_000] {
            let mut group = c.benchmark_group(format!("many_{kind_name}s_j{job_count}"));

            group.bench_with_input(BenchmarkId::from_parameter("baseline"), &(), |b, &_| {
                b.iter(|| {
                    let counter = Arc::new(AtomicUsize::new(0));

                    for _ in 0..job_count {
                        let c = Arc::clone(&counter);
                        // Small amount of work
                        let mut sum = 0u64;
                        for i in 0..num_operations {
                            sum = std::hint::black_box(sum.wrapping_add(i));
                        }
                        c.fetch_add(sum as usize, Ordering::Relaxed);
                    }
                });
            });

            for &worker_count in &worker_count {
                let scheduler = Scheduler::with_workers(worker_count);
                group.bench_with_input(
                    BenchmarkId::from_parameter(format!("w{:02}", worker_count)),
                    &(worker_count, job_count),
                    |b, &(workers, jobs)| {
                        b.iter(|| {
                            let counter = Arc::new(AtomicUsize::new(0));

                            for _ in 0..jobs {
                                let c = Arc::clone(&counter);
                                scheduler
                                    .job_builder(kind_name)
                                    .priority(Priority::Medium)
                                    .spawn(move || {
                                        // Small amount of work
                                        let mut sum = 0u64;
                                        for i in 0..num_operations {
                                            sum = std::hint::black_box(sum.wrapping_add(i));
                                        }
                                        c.fetch_add(sum as usize, Ordering::Relaxed);
                                    });
                            }

                            scheduler.wait_for_all();
                        });
                    },
                );
            }
            group.finish();
        }
    }

    // Benchmark 2: Priority scheduling stress test
    let mut group = c.benchmark_group("priority_scheduling");
    for job_count in [500, 2_000, 5_000] {
        group.bench_with_input(
            BenchmarkId::from_parameter(job_count),
            &job_count,
            |b, &jobs| {
                let scheduler = Scheduler::with_workers(cpus);
                b.iter(|| {
                    // Mix of priorities
                    for i in 0..jobs {
                        let priority = match i % 3 {
                            0 => Priority::Low,
                            1 => Priority::Medium,
                            _ => Priority::High,
                        };
                        scheduler
                            .job_builder("priority_job")
                            .priority(priority)
                            .spawn(|| {
                                let mut sum = 0u64;
                                for i in 0..30 {
                                    sum = sum.wrapping_add(i);
                                }
                                let _ = sum;
                            });
                    }

                    scheduler.wait_for_all();
                });
            },
        );
    }
    group.finish();

    // Benchmark 3: Dependency chain scheduling
    let mut group = c.benchmark_group("dependency_chains");
    for chain_length in [10, 50, 100] {
        group.bench_with_input(
            BenchmarkId::from_parameter(chain_length),
            &chain_length,
            |b, &length| {
                let scheduler = Scheduler::with_workers(cpus);
                b.iter(|| {
                    let mut prev_job = None;
                    for _ in 0..length {
                        let builder = scheduler
                            .job_builder("chain_job")
                            .priority(Priority::Medium);

                        let builder = if let Some(dep) = prev_job {
                            builder.dependencies(vec![dep])
                        } else {
                            builder.dependencies(vec![])
                        };

                        prev_job = Some(builder.spawn(|| {
                            let mut sum = 0u64;
                            for i in 0..20 {
                                sum = sum.wrapping_add(i);
                            }
                            let _ = sum;
                        }));
                    }

                    scheduler.wait_for_all();
                });
            },
        );
    }
    group.finish();

    // Benchmark 4: Parallel chains (multiple independent chains)
    let mut group = c.benchmark_group("parallel_chains");
    for num_chains in [4, 8, 16] {
        group.bench_with_input(
            BenchmarkId::from_parameter(num_chains),
            &num_chains,
            |b, &chains| {
                let scheduler = Scheduler::with_workers(cpus);
                b.iter(|| {
                    for _ in 0..chains {
                        let mut prev = None;
                        for _ in 0..20 {
                            let builder = scheduler
                                .job_builder("parallel_chain")
                                .priority(Priority::Medium);

                            let builder = if let Some(dep) = prev {
                                builder.dependencies(vec![dep])
                            } else {
                                builder.dependencies(vec![])
                            };

                            prev = Some(builder.spawn(|| {
                                let mut sum = 0u64;
                                for i in 0..15 {
                                    sum = sum.wrapping_add(i);
                                }
                                let _ = sum;
                            }));
                        }
                    }

                    scheduler.wait_for_all();
                });
            },
        );
    }
    group.finish();

    // Benchmark 5: Bursty workload (submit jobs in waves)
    let mut group = c.benchmark_group("bursty_workload");
    for wave_count in [5, 10, 20] {
        group.bench_with_input(
            BenchmarkId::from_parameter(wave_count),
            &wave_count,
            |b, &waves| {
                let scheduler = Scheduler::with_workers(cpus);
                b.iter(|| {
                    for _ in 0..waves {
                        for _ in 0..100 {
                            scheduler
                                .job_builder("burst_job")
                                .priority(Priority::Medium)
                                .spawn(|| {
                                    let mut sum = 0u64;
                                    for i in 0..40 {
                                        sum = sum.wrapping_add(i);
                                    }
                                    let _ = sum;
                                });
                        }
                        // Small pause between waves
                        std::thread::sleep(std::time::Duration::from_micros(100));
                    }

                    scheduler.wait_for_all();
                });
            },
        );
    }
    group.finish();

    // Benchmark 6: Mixed workload (different job sizes)
    c.bench_function("mixed_workload", |b| {
        let scheduler = Scheduler::with_workers(cpus);
        b.iter(|| {
            // 80% small jobs, 15% medium, 5% large
            for i in 0..1000 {
                let work_size = if i < 800 {
                    20 // small
                } else if i < 950 {
                    100 // medium
                } else {
                    500 // large
                };

                scheduler
                    .job_builder("mixed_job")
                    .priority(Priority::Medium)
                    .spawn(move || {
                        let mut sum = 0u64;
                        for i in 0..work_size {
                            sum = sum.wrapping_add(i);
                        }
                        let _ = sum;
                    });
            }

            scheduler.wait_for_all();
        });
    });
}

criterion_group!(benches, scheduler_benchmark);
criterion_main!(benches);
