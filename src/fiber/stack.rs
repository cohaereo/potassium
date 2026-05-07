use std::sync::OnceLock;

use crossbeam_queue::ArrayQueue;
use fibrous::FiberStack;

pub(crate) static FIBER_GUARD_PAGES: OnceLock<Box<[(usize, usize)]>> = OnceLock::new();

/// Reusable stack manager for job fibers.
pub struct FiberStackPool {
    stacks: ArrayQueue<FiberStack>,
}

impl FiberStackPool {
    pub fn new(capacity: usize, stack_size: usize) -> Self {
        let stacks = ArrayQueue::new(capacity);
        let mut guard_pages = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            let stack = FiberStack::new(stack_size);
            // TODO(cohae): The guard page needs to be bigger on Windows, 1 page isn't enough to catch all stack overflows
            guard_pages.push((
                stack.guard_page_start() as usize,
                stack.guard_page_end() as usize,
            ));
            _ = stacks.push(stack);
        }

        FIBER_GUARD_PAGES
            .set(guard_pages.into_boxed_slice())
            .expect("FiberStackPool is already initialized!");

        FiberStackPool { stacks }
    }

    pub fn acquire_stack(&self) -> Option<FiberStack> {
        self.stacks.pop()
    }

    pub fn release_stack(&self, stack: FiberStack) {
        let _ = self.stacks.push(stack);
    }
}

unsafe impl Send for FiberStackPool {}
unsafe impl Sync for FiberStackPool {}
