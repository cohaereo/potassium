use crossbeam_queue::ArrayQueue;
use fibrous::FiberStack;

/// Reusable stack manager for job fibers.
pub struct FiberStackPool {
    stacks: ArrayQueue<FiberStack>,
}

impl FiberStackPool {
    pub fn new(capacity: usize, stack_size: usize) -> Self {
        let stacks = ArrayQueue::new(capacity);
        for _ in 0..capacity {
            _ = stacks.push(FiberStack::new(stack_size));
        }
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
