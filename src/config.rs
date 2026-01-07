use gdt_cpus::CoreType;
use smallvec::{SmallVec, smallvec};

/// Configuration for an individual worker thread.
#[derive(Debug, Default, Clone)]
pub struct WorkerConfiguration {
    /// List of logical CPU cores this worker is allowed to run on.
    ///
    /// If empty, the worker isn't pinned to any specific cores.
    pub affinity: SmallVec<[usize; 2]>,
    /// Priority level for this worker thread.
    pub priority: ThreadPriority,
}

/// Configuration for the scheduler and its worker threads.
#[derive(Debug, Clone)]
pub struct SchedulerConfiguration {
    /// List of workers to create.
    pub workers: Vec<WorkerConfiguration>,
}

impl SchedulerConfiguration {
    /// Spawns a single worker thread, not pinned to any specific core.
    pub fn single_core() -> Self {
        Self {
            workers: vec![WorkerConfiguration {
                affinity: SmallVec::new(),
                ..Default::default()
            }],
        }
    }

    /// Spawns the specified number of worker threads, not pinned to any specific cores.
    pub fn with_cores_unpinned(count: usize) -> Self {
        let workers = (0..count)
            .map(|_i| WorkerConfiguration {
                affinity: SmallVec::new(),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Self { workers }
    }

    /// Spawns the specified number of worker threads, pinning each to a specific logical core.
    pub fn with_cores_pinned(core_ids: &[usize]) -> Self {
        let workers = core_ids
            .iter()
            .map(|&core_id| WorkerConfiguration {
                affinity: smallvec![core_id],
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Self { workers }
    }

    /// Spawns the specified number of worker threads, pinning each to the first logical processor of the physical core corresponding to its index.
    pub fn with_cores_autopin(count: usize) -> Self {
        let cpu = gdt_cpus::cpu_info().expect("Failed to get CPU info");
        let socket = cpu.sockets.first().expect("No CPU sockets found?");
        let mut workers = Vec::with_capacity(count);
        for i in 0..count {
            let core = &socket.cores[i % socket.cores.len()];
            workers.push(WorkerConfiguration {
                affinity: smallvec![core.logical_processor_ids[0]],
                ..Default::default()
            });
        }

        Self { workers }
    }

    /// Spawns one worker per physical CPU core, but does not pin them to any specific cores.
    pub fn all_cores_unpinned() -> Self {
        let cpu = gdt_cpus::cpu_info().expect("Failed to get CPU info");
        let socket = cpu.sockets.first().expect("No CPU sockets found?");
        let workers = socket
            .cores
            .iter()
            .filter(|c| c.core_type == CoreType::Performance)
            .map(|_core| WorkerConfiguration {
                affinity: SmallVec::new(),
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Self { workers }
    }

    /// Spawns one worker per physical CPU core, pinning each to it's first logical processor.
    pub fn all_cores_pinned() -> Self {
        let cpu = gdt_cpus::cpu_info().expect("Failed to get CPU info");
        let socket = cpu.sockets.first().expect("No CPU sockets found?");
        let workers = socket
            .cores
            .iter()
            .filter(|c| c.core_type == CoreType::Performance)
            .map(|core| WorkerConfiguration {
                affinity: smallvec![core.logical_processor_ids[0]],
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Self { workers }
    }

    /// Spawns one worker per logical CPU core, pinning each to that core.
    pub fn all_logical_cores_pinned() -> Self {
        let cpu = gdt_cpus::cpu_info().expect("Failed to get CPU info");
        let socket = cpu.sockets.first().expect("No CPU sockets found?");
        let workers = socket
            .cores
            .iter()
            .filter(|c| c.core_type == CoreType::Performance)
            .flat_map(|core| core.logical_processor_ids.iter().cloned())
            .map(|lp_id| WorkerConfiguration {
                affinity: smallvec![lp_id],
                ..Default::default()
            })
            .collect::<Vec<_>>();

        Self { workers }
    }
}

impl Default for SchedulerConfiguration {
    fn default() -> Self {
        Self::all_cores_pinned()
    }
}

/// Priority levels for worker threads.
#[derive(Debug, Default, Clone)]
pub enum ThreadPriority {
    /// Below normal priority: For tasks that are less critical than normal operations.
    BelowNormal,
    /// Normal priority: The default priority for most threads.
    Normal,
    /// Above normal priority: For tasks that require more immediate attention.
    #[default]
    AboveNormal,
    /// Highest priority: For time-critical tasks that must be executed promptly.
    Highest,
}

impl From<ThreadPriority> for gdt_cpus::ThreadPriority {
    fn from(priority: ThreadPriority) -> Self {
        match priority {
            ThreadPriority::BelowNormal => gdt_cpus::ThreadPriority::BelowNormal,
            ThreadPriority::Normal => gdt_cpus::ThreadPriority::Normal,
            ThreadPriority::AboveNormal => gdt_cpus::ThreadPriority::AboveNormal,
            ThreadPriority::Highest => gdt_cpus::ThreadPriority::Highest,
        }
    }
}
