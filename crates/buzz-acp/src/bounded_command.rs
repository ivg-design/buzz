//! Cross-platform bounded subprocess execution.
//!
//! Output is continuously drained into capped in-memory sinks. Unix children
//! lead a process group; Windows children are suspended until assigned to a
//! kill-on-close Job Object. Teardown runs on success as well as failure so a
//! background descendant cannot outlive a read-only inspection.

use std::io::{ErrorKind, Read};
use std::process::{ChildStderr, ChildStdout, Command, ExitStatus, Output, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const POLL_INTERVAL: Duration = Duration::from_millis(10);

#[cfg(unix)]
const DRAIN_IDLE_POLL: Duration = Duration::from_millis(5);

#[cfg(unix)]
const KILL_GRACE: Duration = Duration::from_millis(100);

#[cfg(windows)]
const CREATE_SUSPENDED: u32 = 0x0000_0004;

#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;

#[cfg(windows)]
const BOUNDED_CREATION_FLAGS: u32 = CREATE_SUSPENDED | CREATE_NO_WINDOW;

#[cfg(windows)]
const _: () = {
    assert!(BOUNDED_CREATION_FLAGS & CREATE_SUSPENDED == CREATE_SUSPENDED);
    assert!(BOUNDED_CREATION_FLAGS & CREATE_NO_WINDOW == CREATE_NO_WINDOW);
};

#[derive(Debug, Clone, Copy)]
pub(crate) struct Limits {
    pub timeout: Duration,
    pub stdout_bytes: u64,
    pub stderr_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Error {
    Spawn,
    Setup,
    Timeout,
    OutputLimit,
    Wait,
    Read,
}

struct BoundedChild {
    child: std::process::Child,
    #[cfg(windows)]
    job: Option<JobHandle>,
}

impl BoundedChild {
    fn spawn(mut command: Command) -> Option<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt as _;
            command.process_group(0);
        }
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt as _;
            command.creation_flags(BOUNDED_CREATION_FLAGS);
        }

        #[cfg_attr(not(windows), allow(unused_mut))]
        let mut child = command.spawn().ok()?;
        #[cfg(windows)]
        let job = {
            let Some(job) = create_job_for_child(child.id()) else {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            };
            if !resume_process(child.id()) {
                drop(job);
                let _ = child.wait();
                return None;
            }
            job
        };

        Some(Self {
            child,
            #[cfg(windows)]
            job: Some(job),
        })
    }

    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.child.try_wait()
    }

    fn terminate_timed_out(&mut self) {
        #[cfg(unix)]
        {
            use nix::sys::signal::{killpg, Signal};
            use nix::unistd::Pid;
            let _ = killpg(Pid::from_raw(self.child.id() as i32), Signal::SIGTERM);
            std::thread::sleep(KILL_GRACE);
        }
        self.kill_tree();
    }

    fn kill_tree(&mut self) {
        #[cfg(unix)]
        {
            use nix::sys::signal::{killpg, Signal};
            use nix::unistd::Pid;
            let _ = killpg(Pid::from_raw(self.child.id() as i32), Signal::SIGKILL);
        }
        #[cfg(windows)]
        if let Some(job) = self.job.take() {
            drop(job);
        }
        let _ = self.child.kill();
    }

    fn reap(&mut self) {
        let _ = self.child.wait();
    }

    fn take_stdout(&mut self) -> Option<ChildStdout> {
        self.child.stdout.take()
    }

    fn take_stderr(&mut self) -> Option<ChildStderr> {
        self.child.stderr.take()
    }
}

#[cfg(unix)]
fn set_nonblocking<F: std::os::fd::AsFd>(file: &F) -> bool {
    use nix::fcntl::{fcntl, FcntlArg, OFlag};

    let Ok(raw_flags) = fcntl(file, FcntlArg::F_GETFL) else {
        return false;
    };
    let flags = OFlag::from_bits_truncate(raw_flags);
    fcntl(file, FcntlArg::F_SETFL(flags | OFlag::O_NONBLOCK)).is_ok()
}

fn spawn_drain<R: Read + Send + 'static>(
    mut reader: R,
    limit: u64,
    overflow: Arc<AtomicBool>,
    stop: Arc<AtomicBool>,
) -> JoinHandle<std::io::Result<Vec<u8>>> {
    #[cfg(windows)]
    let _ = &stop;
    std::thread::spawn(move || {
        let mut output = Vec::new();
        let mut chunk = [0_u8; 8192];
        loop {
            match reader.read(&mut chunk) {
                Ok(0) => return Ok(output),
                Ok(read) => {
                    let remaining = limit.saturating_sub(output.len() as u64) as usize;
                    let keep = remaining.min(read);
                    output.extend_from_slice(&chunk[..keep]);
                    if keep < read {
                        overflow.store(true, Ordering::Relaxed);
                        return Ok(output);
                    }
                }
                Err(error) if error.kind() == ErrorKind::Interrupted => continue,
                #[cfg(unix)]
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
                    if stop.load(Ordering::Relaxed) {
                        return Ok(output);
                    }
                    std::thread::sleep(DRAIN_IDLE_POLL);
                }
                Err(error) => return Err(error),
            }
        }
    })
}

fn join_drain(drain: Option<JoinHandle<std::io::Result<Vec<u8>>>>) -> Option<Vec<u8>> {
    match drain {
        Some(handle) => handle.join().ok()?.ok(),
        None => Some(Vec::new()),
    }
}

pub(crate) fn output_with_limits(mut command: Command, limits: Limits) -> Result<Output, Error> {
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = BoundedChild::spawn(command).ok_or(Error::Spawn)?;
    let stdout = child.take_stdout();
    let stderr = child.take_stderr();

    #[cfg(unix)]
    if !stdout.as_ref().is_none_or(set_nonblocking) || !stderr.as_ref().is_none_or(set_nonblocking)
    {
        child.kill_tree();
        child.reap();
        return Err(Error::Setup);
    }

    let overflow = Arc::new(AtomicBool::new(false));
    let stop = Arc::new(AtomicBool::new(false));
    let stdout_drain = stdout.map(|pipe| {
        spawn_drain(
            pipe,
            limits.stdout_bytes,
            Arc::clone(&overflow),
            Arc::clone(&stop),
        )
    });
    let stderr_drain = stderr.map(|pipe| {
        spawn_drain(
            pipe,
            limits.stderr_bytes,
            Arc::clone(&overflow),
            Arc::clone(&stop),
        )
    });

    let deadline = Instant::now() + limits.timeout;
    let mut failure = None;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Some(status),
            Ok(None) if overflow.load(Ordering::Relaxed) => {
                child.kill_tree();
                failure = Some(Error::OutputLimit);
                break None;
            }
            Ok(None) if Instant::now() >= deadline => {
                child.terminate_timed_out();
                failure = Some(Error::Timeout);
                break None;
            }
            Ok(None) => std::thread::sleep(POLL_INTERVAL),
            Err(_) => {
                child.kill_tree();
                failure = Some(Error::Wait);
                break None;
            }
        }
    };

    child.kill_tree();
    child.reap();
    stop.store(true, Ordering::Relaxed);
    let stdout = join_drain(stdout_drain).ok_or(Error::Read)?;
    let stderr = join_drain(stderr_drain).ok_or(Error::Read)?;
    if let Some(error) = failure {
        return Err(error);
    }
    let status = status.ok_or(Error::Wait)?;
    if overflow.load(Ordering::Relaxed) {
        return Err(Error::OutputLimit);
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}

#[cfg(windows)]
struct JobHandle(windows_sys::Win32::Foundation::HANDLE);

#[cfg(windows)]
unsafe impl Send for JobHandle {}

#[cfg(windows)]
impl Drop for JobHandle {
    fn drop(&mut self) {
        unsafe { windows_sys::Win32::Foundation::CloseHandle(self.0) };
    }
}

#[cfg(windows)]
fn create_job_for_child(pid: u32) -> Option<JobHandle> {
    use std::ptr::null;
    use windows_sys::Win32::Foundation::{CloseHandle, FALSE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JobObjectExtendedLimitInformation,
        SetInformationJobObject, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
    };
    use windows_sys::Win32::System::Threading::{
        OpenProcess, PROCESS_SET_QUOTA, PROCESS_TERMINATE,
    };

    unsafe {
        let job = CreateJobObjectW(null(), null());
        if job.is_null() {
            return None;
        }
        let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
        info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
        if SetInformationJobObject(
            job,
            JobObjectExtendedLimitInformation,
            &raw const info as *const _,
            std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
        ) == FALSE
        {
            CloseHandle(job);
            return None;
        }
        let process = OpenProcess(PROCESS_SET_QUOTA | PROCESS_TERMINATE, FALSE, pid);
        if process.is_null() {
            CloseHandle(job);
            return None;
        }
        let assigned = AssignProcessToJobObject(job, process);
        CloseHandle(process);
        if assigned == FALSE {
            CloseHandle(job);
            return None;
        }
        Some(JobHandle(job))
    }
}

#[cfg(windows)]
fn resume_process(pid: u32) -> bool {
    use windows_sys::Win32::Foundation::{CloseHandle, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::Diagnostics::ToolHelp::{
        CreateToolhelp32Snapshot, Thread32First, Thread32Next, TH32CS_SNAPTHREAD, THREADENTRY32,
    };
    use windows_sys::Win32::System::Threading::{OpenThread, ResumeThread, THREAD_SUSPEND_RESUME};

    unsafe {
        let snapshot = CreateToolhelp32Snapshot(TH32CS_SNAPTHREAD, 0);
        if snapshot == INVALID_HANDLE_VALUE {
            return false;
        }
        let mut entry: THREADENTRY32 = std::mem::zeroed();
        entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
        let mut resumed = false;
        let mut has_entry = Thread32First(snapshot, &mut entry);
        while has_entry != 0 {
            if entry.th32OwnerProcessID == pid {
                let thread = OpenThread(THREAD_SUSPEND_RESUME, 0, entry.th32ThreadID);
                if !thread.is_null() {
                    if ResumeThread(thread) != u32::MAX {
                        resumed = true;
                    }
                    CloseHandle(thread);
                }
            }
            entry.dwSize = std::mem::size_of::<THREADENTRY32>() as u32;
            has_entry = Thread32Next(snapshot, &mut entry);
        }
        CloseHandle(snapshot);
        resumed
    }
}
