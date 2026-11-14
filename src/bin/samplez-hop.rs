//! One matrix hop: N MXL inputs through the configured gain matrix to M
//! MXL outputs. `samplez-hop --config <file>`; runs until SIGINT or
//! SIGTERM, then releases its writers, which deletes their flows.

use std::path::PathBuf;
use std::process::ExitCode;

use samplez::{Config, Engine, rt};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let path = match args.as_slice() {
        [_, flag, path] if flag == "--config" => PathBuf::from(path),
        _ => {
            eprintln!("usage: samplez-hop --config <file>");
            return ExitCode::from(2);
        }
    };
    let config = match Config::load(&path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("samplez-hop: {e}");
            return ExitCode::from(2);
        }
    };
    // SIGINT and SIGTERM are blocked before any thread exists, so every
    // thread inherits the mask and only sigwait below receives them.
    // SAFETY: `set` is initialised by sigemptyset before use.
    let set = unsafe {
        let mut set = std::mem::zeroed::<libc::sigset_t>();
        libc::sigemptyset(&mut set);
        libc::sigaddset(&mut set, libc::SIGINT);
        libc::sigaddset(&mut set, libc::SIGTERM);
        libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
        set
    };
    let process = rt::setup_process(&config.rt);
    let engine = match Engine::start(config, process, None) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("samplez-hop: {e}");
            return ExitCode::FAILURE;
        }
    };
    let rt = engine.status().rt;
    for (name, step) in [
        ("capabilities", &rt.process.capabilities),
        ("dumpable", &rt.process.dumpable),
        ("mlock", &rt.process.mlock),
        ("stack", &rt.process.stack),
        ("sched", &rt.thread.sched),
        ("affinity", &rt.thread.affinity),
        ("ftz_daz", &rt.thread.ftz_daz),
        ("signals", &rt.thread.signals),
    ] {
        eprintln!("samplez-hop: rt {name}: {step}");
    }
    let mut sig = 0;
    // SAFETY: `set` is a valid signal set and `sig` is writable.
    unsafe { libc::sigwait(&set, &mut sig) };
    eprintln!("samplez-hop: signal {sig}, stopping");
    engine.stop();
    ExitCode::SUCCESS
}
