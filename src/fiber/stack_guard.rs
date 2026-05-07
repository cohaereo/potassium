#[cfg(windows)]
mod windows_handler {
    use crate::fiber::stack::FIBER_GUARD_PAGES;
    use crate::util::MutexExt;

    use windows_sys::Win32::Foundation::{
        EXCEPTION_ACCESS_VIOLATION, EXCEPTION_STACK_OVERFLOW, STATUS_GUARD_PAGE_VIOLATION,
    };
    use windows_sys::Win32::System::Console::{GetStdHandle, STD_ERROR_HANDLE};
    use windows_sys::Win32::System::Diagnostics::Debug::{
        AddVectoredExceptionHandler, EXCEPTION_CONTINUE_SEARCH, EXCEPTION_POINTERS,
    };

    unsafe extern "system" fn veh_handler(info: *mut EXCEPTION_POINTERS) -> i32 {
        let record = unsafe { &*(*info).ExceptionRecord };

        let fault_addr: usize = match record.ExceptionCode {
            STATUS_GUARD_PAGE_VIOLATION if record.NumberParameters >= 2 => {
                record.ExceptionInformation[1]
            }
            EXCEPTION_ACCESS_VIOLATION if record.NumberParameters >= 2 => {
                record.ExceptionInformation[1]
            }
            EXCEPTION_STACK_OVERFLOW if record.NumberParameters >= 2 => {
                record.ExceptionInformation[1]
            }
            _ => return EXCEPTION_CONTINUE_SEARCH,
        };

        for &(start, end) in FIBER_GUARD_PAGES.lock2().iter() {
            if fault_addr >= start && fault_addr < end {
                const MSG: &[u8] = b"fatal: fiber stack overflow\n\0";
                unsafe {
                    windows_sys::Win32::Storage::FileSystem::WriteFile(
                        GetStdHandle(STD_ERROR_HANDLE),
                        MSG.as_ptr() as *const _,
                        MSG.len() as u32,
                        std::ptr::null_mut(),
                        std::ptr::null_mut(),
                    );
                    windows_sys::Win32::System::Threading::ExitProcess(1);
                }
            }
        }

        EXCEPTION_CONTINUE_SEARCH
    }

    pub fn install() {
        unsafe {
            AddVectoredExceptionHandler(1, Some(veh_handler));
        }
    }
}

#[cfg(unix)]
mod unix_handler {
    use std::os::raw::c_void;

    use libc::{
        SA_ONSTACK, SA_SIGINFO, SIGSEGV, STDERR_FILENO, c_int, sigaction, sigaltstack, siginfo_t,
        stack_t, write,
    };

    use crate::fiber::stack::FIBER_GUARD_PAGES;
    use crate::util::MutexExt;

    extern "C" fn sigsegv_handler(_sig: c_int, info: *mut siginfo_t, _ctx: *mut c_void) {
        let fault_addr = unsafe { (*info).si_addr() } as usize;

        for &(start, end) in FIBER_GUARD_PAGES.lock2().iter() {
            if fault_addr >= start && fault_addr < end {
                const MSG: &[u8] = b"fatal: fiber stack overflow\n";
                unsafe {
                    write(STDERR_FILENO, MSG.as_ptr() as *const c_void, MSG.len());
                    libc::abort();
                }
            }
        }

        unsafe {
            libc::signal(libc::SIGSEGV, libc::SIG_DFL);
            libc::raise(libc::SIGSEGV);
        }
    }

    const ALT_STACK_SIZE: usize = 64 * 1024;

    pub fn install() {
        let alt_stack_mem: &'static mut [u8] =
            Box::leak(vec![0u8; ALT_STACK_SIZE].into_boxed_slice());

        let ss = stack_t {
            ss_sp: alt_stack_mem.as_mut_ptr() as *mut c_void,
            ss_flags: 0,
            ss_size: ALT_STACK_SIZE,
        };
        let ret = unsafe { sigaltstack(&ss, std::ptr::null_mut()) };
        assert_eq!(ret, 0, "sigaltstack failed");

        let mut sa: sigaction = unsafe { std::mem::zeroed() };
        sa.sa_sigaction = sigsegv_handler as *const () as usize;
        sa.sa_flags = SA_SIGINFO | SA_ONSTACK;

        let ret = unsafe { sigaction(SIGSEGV, &sa, std::ptr::null_mut()) };
        assert_eq!(ret, 0, "sigaction failed");
    }
}

pub fn install_stack_overflow_handler() {
    #[cfg(unix)]
    unix_handler::install();

    #[cfg(windows)]
    windows_handler::install();
}
