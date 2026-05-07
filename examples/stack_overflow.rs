use std::time::Duration;

use potassium::*;

fn main() {
    let scheduler = Scheduler::default();
    println!("Running with {} workers", scheduler.num_workers());

    let mut last_job = None;
    for i in 0..4 {
        let job = scheduler
            .job_builder("test_0")
            .dependencies(last_job.clone())
            .spawn(move || {
                big_stack();
            });
        last_job = Some(job);
    }

    if let Some(job) = last_job {
        job.wait();
    }

    println!("Done");
}

fn big_stack() {
    let mut big_stack_array = [0u8; 64 * 1024];
    println!("Allocated big stack array");
    big_stack_array[big_stack_array.len() - 1] = 1;
    std::hint::black_box(big_stack_array[big_stack_array.len() - 1]);
    println!("Accessed big stack");
}
