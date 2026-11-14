//! Real-time set-up: the process steps that run on the main thread before
//! any other thread exists, and the steps the RT thread runs on itself.
//! Every step reports its outcome; none aborts start-up.

use std::ffi::c_int;
use std::fmt;

use crate::config::{Rt, RtCpu};

/// Default stack size of threads created after [`setup_process`],
/// libmxl's domain watchers among them, and of the engine's own threads.
pub const STACK_SIZE: usize = 512 * 1024;

/// The downward-API environment variables carrying the container's CPU
/// request and limit in millicores (`resourceFieldRef`, `divisor: 1m`).
pub const CPU_REQUEST_ENV: &str = "SAMPLEZ_CPU_REQUEST_MILLICORES";
pub const CPU_LIMIT_ENV: &str = "SAMPLEZ_CPU_LIMIT_MILLICORES";

const CAP_IPC_LOCK: u32 = 14;
const CAP_SYS_NICE: u32 = 23;

/// The outcome of one set-up step.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step {
    /// Done as configured.
    Active(String),
    /// Not done or done partly; the engine runs without it.
    Degraded(String),
    /// Not configured, or not applicable.
    Off(String),
}

impl Step {
    pub fn is_degraded(&self) -> bool {
        matches!(self, Step::Degraded(_))
    }
}

impl fmt::Display for Step {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Step::Active(m) => write!(f, "active: {m}"),
            Step::Degraded(m) => write!(f, "degraded: {m}"),
            Step::Off(m) => write!(f, "off: {m}"),
        }
    }
}

/// The process steps, from [`setup_process`].
#[derive(Clone, Debug)]
pub struct ProcessStatus {
    /// Effective capabilities raised from the permitted set.
    pub capabilities: Step,
    /// `PR_SET_DUMPABLE` back to 1 after a secure-exec start.
    pub dumpable: Step,
    /// `mlockall`: `MCL_CURRENT | MCL_FUTURE`, or `MCL_CURRENT` only.
    pub mlock: Step,
    /// The default thread stack size cap.
    pub stack: Step,
}

/// The RT thread's own steps.
#[derive(Clone, Debug)]
pub struct ThreadStatus {
    /// The RT thread's kernel thread id.
    pub tid: i32,
    pub sched: Step,
    pub affinity: Step,
    pub ftz_daz: Step,
    pub signals: Step,
}

#[derive(Clone, Debug)]
pub struct RtStatus {
    pub process: ProcessStatus,
    pub thread: ThreadStatus,
}

impl RtStatus {
    pub fn degraded(&self) -> bool {
        let p = &self.process;
        let t = &self.thread;
        [
            &p.capabilities,
            &p.dumpable,
            &p.mlock,
            &p.stack,
            &t.sched,
            &t.affinity,
            &t.ftz_daz,
            &t.signals,
        ]
        .iter()
        .any(|s| s.is_degraded())
    }
}

fn errno() -> std::io::Error {
    std::io::Error::last_os_error()
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CapHeader {
    version: u32,
    pid: c_int,
}

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CapData {
    effective: u32,
    permitted: u32,
    inheritable: u32,
}

const CAP_V3: u32 = 0x2008_0522;

/// `capget` of the calling thread, v3: two data structs, the low and high
/// 32 capabilities.
fn capget() -> std::io::Result<(CapHeader, [CapData; 2])> {
    let mut h = CapHeader {
        version: CAP_V3,
        pid: 0,
    };
    let mut d = [CapData::default(); 2];
    // SAFETY: a v3 header and two data structs, as capget(2) requires.
    if unsafe { libc::syscall(libc::SYS_capget, &mut h, d.as_mut_ptr()) } != 0 {
        return Err(errno());
    }
    Ok((h, d))
}

fn joined(d: &[CapData; 2], f: fn(&CapData) -> u32) -> u64 {
    u64::from(f(&d[0])) | (u64::from(f(&d[1])) << 32)
}

/// Raises the effective set to the permitted set.
fn raise_permitted() -> Step {
    let (mut h, mut d) = match capget() {
        Ok(v) => v,
        Err(e) => return Step::Degraded(format!("capget failed: {e}")),
    };
    let perm = joined(&d, |x| x.permitted);
    if perm == 0 {
        return Step::Degraded("no capabilities permitted".to_owned());
    }
    for x in &mut d {
        x.effective = x.permitted;
    }
    // SAFETY: a v3 header and two data structs read back from capget with
    // the effective sets set to the permitted ones, which capset(2) allows.
    if unsafe { libc::syscall(libc::SYS_capset, &mut h, d.as_ptr()) } != 0 {
        return Step::Degraded(format!("capset to permitted {perm:#x} failed: {}", errno()));
    }
    Step::Active(format!("effective = permitted = {perm:#x}"))
}

fn effective_has(cap: u32) -> bool {
    capget().is_ok_and(|(_, d)| joined(&d, |x| x.effective) & (1 << cap) != 0)
}

/// The permitted capability set of the calling thread.
pub fn permitted() -> std::io::Result<u64> {
    capget().map(|(_, d)| joined(&d, |x| x.permitted))
}

fn memlock_unlimited() -> bool {
    let mut r = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: `r` is writable.
    unsafe {
        libc::getrlimit(libc::RLIMIT_MEMLOCK, &mut r) == 0 && r.rlim_cur == libc::RLIM_INFINITY
    }
}

/// `mlockall(MCL_CURRENT | MCL_FUTURE)` when `CAP_IPC_LOCK` is effective
/// or `RLIMIT_MEMLOCK` is unlimited. With a finite limit `MCL_FUTURE` makes
/// later mappings of flows, stacks and arenas fail, so only `MCL_CURRENT`
/// is locked then, which is degraded.
fn lock_memory() -> Step {
    if effective_has(CAP_IPC_LOCK) || memlock_unlimited() {
        // SAFETY: mlockall takes flags only.
        if unsafe { libc::mlockall(libc::MCL_CURRENT | libc::MCL_FUTURE) } == 0 {
            return Step::Active("MCL_CURRENT | MCL_FUTURE".to_owned());
        }
        return Step::Degraded(format!(
            "mlockall(MCL_CURRENT | MCL_FUTURE) failed: {}",
            errno()
        ));
    }
    // SAFETY: mlockall takes flags only.
    if unsafe { libc::mlockall(libc::MCL_CURRENT) } == 0 {
        Step::Degraded("MCL_CURRENT only: no CAP_IPC_LOCK and RLIMIT_MEMLOCK is finite".to_owned())
    } else {
        Step::Degraded(format!(
            "mlockall(MCL_CURRENT) failed: {}; no CAP_IPC_LOCK and RLIMIT_MEMLOCK is finite",
            errno()
        ))
    }
}

unsafe extern "C" {
    /// glibc 2.18+.
    fn pthread_setattr_default_np(attr: *const libc::pthread_attr_t) -> c_int;
}

fn cap_default_stack() -> Step {
    let mut attr = std::mem::MaybeUninit::<libc::pthread_attr_t>::uninit();
    // SAFETY: `attr` is initialised by pthread_attr_init before any other
    // use and destroyed after.
    let rc = unsafe {
        if libc::pthread_attr_init(attr.as_mut_ptr()) != 0 {
            return Step::Degraded("pthread_attr_init failed".to_owned());
        }
        let mut rc = libc::pthread_attr_setstacksize(attr.as_mut_ptr(), STACK_SIZE);
        if rc == 0 {
            rc = pthread_setattr_default_np(attr.as_ptr());
        }
        libc::pthread_attr_destroy(attr.as_mut_ptr());
        rc
    };
    if rc == 0 {
        Step::Active(format!("{STACK_SIZE} bytes"))
    } else {
        Step::Degraded(format!(
            "pthread_setattr_default_np failed: {}",
            std::io::Error::from_raw_os_error(rc)
        ))
    }
}

/// The process steps. Runs on the main thread before any other thread is
/// created: capabilities, then memory locking, then the default stack cap.
pub fn setup_process(rt: &Rt) -> ProcessStatus {
    let stack = cap_default_stack();
    if !rt.enabled {
        let off = || Step::Off("rt.enabled is false".to_owned());
        return ProcessStatus {
            capabilities: off(),
            dumpable: off(),
            mlock: off(),
            stack,
        };
    }
    let capabilities = raise_permitted();
    // A process started with file capabilities is not dumpable.
    // SAFETY: prctl with PR_SET_DUMPABLE takes an integer argument.
    let dumpable = if unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 1) } == 0 {
        Step::Active("1".to_owned())
    } else {
        Step::Degraded(format!("PR_SET_DUMPABLE failed: {}", errno()))
    };
    ProcessStatus {
        capabilities,
        dumpable,
        mlock: lock_memory(),
        stack,
    }
}

/// The calling thread's allowed CPUs, ascending.
fn allowed_cpus() -> std::io::Result<Vec<usize>> {
    // SAFETY: an all-zero cpu_set_t is an empty set.
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    // SAFETY: `set` is writable and its size is passed.
    if unsafe { libc::sched_getaffinity(0, size_of::<libc::cpu_set_t>(), &mut set) } != 0 {
        return Err(errno());
    }
    Ok((0..libc::CPU_SETSIZE as usize)
        // SAFETY: `cpu` is below CPU_SETSIZE.
        .filter(|&cpu| unsafe { libc::CPU_ISSET(cpu, &set) })
        .collect())
}

fn pin(cpu: usize) -> std::io::Result<()> {
    // SAFETY: an all-zero cpu_set_t is an empty set; `cpu` comes from the
    // allowed set, so it is below CPU_SETSIZE.
    let rc = unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set)
    };
    if rc == 0 { Ok(()) } else { Err(errno()) }
}

/// The CFS quota of the process's cgroup: `None` when unlimited, else
/// (quota, period) in microseconds. cgroup v2 `cpu.max`, else v1
/// `cpu.cfs_quota_us` and `cpu.cfs_period_us`.
pub fn cgroup_quota() -> Result<Option<(u64, u64)>, String> {
    let cgroups = std::fs::read_to_string("/proc/self/cgroup")
        .map_err(|e| format!("/proc/self/cgroup: {e}"))?;
    for line in cgroups.lines() {
        let mut parts = line.splitn(3, ':');
        let (Some(id), Some(controllers), Some(path)) = (parts.next(), parts.next(), parts.next())
        else {
            continue;
        };
        if id == "0" && controllers.is_empty() {
            let file = format!("/sys/fs/cgroup{}/cpu.max", path.trim_end_matches('/'));
            let text = std::fs::read_to_string(&file).map_err(|e| format!("{file}: {e}"))?;
            return parse_cpu_max(&text).ok_or_else(|| format!("{file}: unreadable {text:?}"));
        }
        if controllers.split(',').any(|c| c == "cpu") {
            let dir = format!("/sys/fs/cgroup/{controllers}{}", path.trim_end_matches('/'));
            let read = |name: &str| -> Result<i64, String> {
                let file = format!("{dir}/{name}");
                let text = std::fs::read_to_string(&file).map_err(|e| format!("{file}: {e}"))?;
                text.trim()
                    .parse()
                    .map_err(|_| format!("{file}: unreadable {text:?}"))
            };
            let quota = read("cpu.cfs_quota_us")?;
            if quota < 0 {
                return Ok(None);
            }
            return Ok(Some((quota as u64, read("cpu.cfs_period_us")? as u64)));
        }
    }
    Err("no cpu cgroup in /proc/self/cgroup".to_owned())
}

fn parse_cpu_max(text: &str) -> Option<Option<(u64, u64)>> {
    let mut it = text.split_whitespace();
    let quota = it.next()?;
    let period = it.next()?.parse().ok()?;
    if quota == "max" {
        Some(None)
    } else {
        Some(Some((quota.parse().ok()?, period)))
    }
}

/// Whether `auto` pins: the CPU request and limit (millicores) are equal
/// multiples of 1000, the allowed set has that many CPUs, and the cgroup
/// quota is unlimited or equals the set size. That is the signature of an
/// exclusive allocation under the static CPU manager. `Err` says why not.
pub fn auto_pins(
    request: Option<&str>,
    limit: Option<&str>,
    set_size: usize,
    quota: Result<Option<(u64, u64)>, String>,
) -> Result<(), String> {
    let millicores = |v: Option<&str>, name: &str| -> Result<u64, String> {
        let v = v.ok_or_else(|| format!("{name} is not set"))?;
        v.trim()
            .parse()
            .map_err(|_| format!("{name}={v:?} is not a millicore count"))
    };
    let r = millicores(request, CPU_REQUEST_ENV)?;
    let l = millicores(limit, CPU_LIMIT_ENV)?;
    if r != l {
        return Err(format!("CPU request {r}m and limit {l}m differ"));
    }
    if r == 0 || r % 1000 != 0 {
        return Err(format!("CPU request {r}m is not a whole number of CPUs"));
    }
    let cpus = (r / 1000) as usize;
    if set_size != cpus {
        return Err(format!(
            "the allowed set has {set_size} CPUs, the CPU request is {cpus}"
        ));
    }
    match quota? {
        None => Ok(()),
        Some((q, p)) if q == cpus as u64 * p => Ok(()),
        Some((q, p)) => Err(format!(
            "cgroup CPU quota {q}/{p} is neither unlimited nor {cpus} CPUs"
        )),
    }
}

fn affinity(rt_cpu: RtCpu) -> Step {
    let set = match allowed_cpus() {
        Ok(s) if !s.is_empty() => s,
        Ok(_) => return Step::Degraded("sched_getaffinity returned no CPU".to_owned()),
        Err(e) => return Step::Degraded(format!("sched_getaffinity failed: {e}")),
    };
    let cpu = match rt_cpu {
        RtCpu::None => return Step::Off("rtCpu none".to_owned()),
        RtCpu::First => set[0],
        RtCpu::Last => set[set.len() - 1],
        RtCpu::Index(i) => match set.get(i) {
            Some(&c) => c,
            None => {
                return Step::Degraded(format!(
                    "rtCpu {i} is outside the allowed set of {} CPUs",
                    set.len()
                ));
            }
        },
        RtCpu::Auto => {
            let env = |n| std::env::var(n).ok();
            let (request, limit) = (env(CPU_REQUEST_ENV), env(CPU_LIMIT_ENV));
            if let Err(why) = auto_pins(
                request.as_deref(),
                limit.as_deref(),
                set.len(),
                cgroup_quota(),
            ) {
                return Step::Off(format!("auto, not pinned: {why}"));
            }
            set[set.len() - 1]
        }
    };
    match pin(cpu) {
        Ok(()) => Step::Active(format!("CPU {cpu}")),
        Err(e) => Step::Degraded(format!("sched_setaffinity to CPU {cpu} failed: {e}")),
    }
}

fn sched_fifo(priority: i32) -> Step {
    let param = libc::sched_param {
        sched_priority: priority,
    };
    // SAFETY: the calling thread's handle and a valid sched_param.
    let rc = unsafe { libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_FIFO, &param) };
    if rc == 0 {
        Step::Active(format!("SCHED_FIFO priority {priority}"))
    } else {
        Step::Degraded(format!(
            "SCHED_FIFO priority {priority} refused: {}; needs CAP_SYS_NICE{} or \
             RLIMIT_RTPRIO >= {priority}",
            std::io::Error::from_raw_os_error(rc),
            if effective_has(CAP_SYS_NICE) {
                " (effective)"
            } else {
                ""
            }
        ))
    }
}

fn block_signals() -> Step {
    // SAFETY: `set` is initialised by sigfillset before use.
    let rc = unsafe {
        let mut set = std::mem::zeroed::<libc::sigset_t>();
        libc::sigfillset(&mut set);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut())
    };
    if rc == 0 {
        Step::Active("all signals blocked".to_owned())
    } else {
        Step::Degraded(format!(
            "pthread_sigmask failed: {}",
            std::io::Error::from_raw_os_error(rc)
        ))
    }
}

/// The RT thread's steps, run on the RT thread itself: SCHED_FIFO,
/// affinity, FTZ/DAZ and signal blocking. FTZ/DAZ stays set for the
/// thread's life.
pub fn setup_thread(rt: &Rt) -> ThreadStatus {
    // SAFETY: gettid has no preconditions.
    let tid = unsafe { libc::gettid() };
    let (sched, affinity) = if rt.enabled {
        (sched_fifo(rt.priority), affinity(rt.rt_cpu))
    } else {
        let off = || Step::Off("rt.enabled is false".to_owned());
        (off(), off())
    };
    // The previous MXCSR is not restored: FTZ/DAZ stays for the thread's
    // life.
    let _ = samplez_dsp::set_ftz_daz(true);
    let flags = samplez_dsp::FTZ | samplez_dsp::DAZ;
    let ftz_daz = if samplez_dsp::mxcsr() & flags == flags {
        Step::Active("MXCSR FTZ and DAZ set".to_owned())
    } else {
        Step::Degraded("MXCSR FTZ and DAZ did not stick".to_owned())
    };
    ThreadStatus {
        tid,
        sched,
        affinity,
        ftz_daz,
        signals: block_signals(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auto(
        r: Option<&str>,
        l: Option<&str>,
        n: usize,
        q: Option<(u64, u64)>,
    ) -> Result<(), String> {
        auto_pins(r, l, n, Ok(q))
    }

    #[test]
    fn auto_pins_only_an_exclusive_integer_allocation() {
        assert_eq!(auto(Some("2000"), Some("2000"), 2, None), Ok(()));
        assert_eq!(
            auto(Some("1000"), Some("1000"), 1, Some((100_000, 100_000))),
            Ok(())
        );
        // Burstable: request below limit.
        assert!(auto(Some("1000"), Some("2000"), 2, None).is_err());
        // Fractional CPUs.
        assert!(auto(Some("1500"), Some("1500"), 2, None).is_err());
        // Shared pool: the allowed set is larger than the allocation.
        assert!(auto(Some("2000"), Some("2000"), 8, None).is_err());
        // A quota narrower than the set.
        assert!(auto(Some("2000"), Some("2000"), 2, Some((100_000, 100_000))).is_err());
        // BestEffort: no values from the downward API.
        assert!(auto(None, None, 2, None).is_err());
        assert!(auto(Some("0"), Some("0"), 0, None).is_err());
        assert!(auto(Some("2"), Some("2000"), 2, None).is_err());
        assert!(auto_pins(Some("1000"), Some("1000"), 1, Err("no cgroup".to_owned())).is_err());
    }

    #[test]
    fn cpu_max_is_parsed() {
        assert_eq!(parse_cpu_max("max 100000\n"), Some(None));
        assert_eq!(
            parse_cpu_max("200000 100000\n"),
            Some(Some((200_000, 100_000)))
        );
        assert_eq!(parse_cpu_max("max"), None);
        assert_eq!(parse_cpu_max("x 100000"), None);
    }
}
