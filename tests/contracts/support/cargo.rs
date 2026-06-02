#![allow(dead_code)]

use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    path::Path,
    process::{Command, ExitStatus, Output},
    sync::{Mutex, MutexGuard, OnceLock},
};

struct CargoRunGuard {
    _process_guard: MutexGuard<'static, ()>,
    _file: File,
}

fn in_process_cargo_run_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn cargo_run_lock() -> CargoRunGuard {
    let process_guard = in_process_cargo_run_lock().lock().unwrap();
    let lock_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join(".tmp");
    fs::create_dir_all(&lock_dir).unwrap();
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(lock_dir.join("cargo-run.lock"))
        .unwrap();
    fs4::FileExt::lock(&file).unwrap();
    CargoRunGuard {
        _process_guard: process_guard,
        _file: file,
    }
}

fn cargo_command() -> Command {
    let mut command = Command::new("cargo");
    command.current_dir(env!("CARGO_MANIFEST_DIR"));
    command
}

fn add_profiled_cargo_args<I, S>(command: &mut Command, args: I)
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut args = args.into_iter();
    if let Some(first) = args.next() {
        let is_run = first.as_ref() == OsStr::new("run");
        command.arg(first);
        if is_run && !cfg!(debug_assertions) {
            command.arg("--release");
        }
    }
    command.args(args);
}

pub fn cargo_status<I, S>(args: I) -> ExitStatus
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let _guard = cargo_run_lock();
    let mut command = cargo_command();
    add_profiled_cargo_args(&mut command, args);
    command.status().unwrap()
}

pub fn cargo_status_with_env<I, S, E, K, V>(args: I, envs: E) -> ExitStatus
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
    E: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    let _guard = cargo_run_lock();
    let mut command = cargo_command();
    command.envs(envs);
    add_profiled_cargo_args(&mut command, args);
    command.status().unwrap()
}

pub fn cargo_output<I, S>(args: I) -> Output
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let _guard = cargo_run_lock();
    let mut command = cargo_command();
    add_profiled_cargo_args(&mut command, args);
    command.output().unwrap()
}

pub fn rax_output(args: &[&str]) -> Output {
    cargo_output(
        ["run", "-p", "rax-cli", "--"]
            .into_iter()
            .chain(args.iter().copied()),
    )
}
