use std::sync::OnceLock;

use tokio::runtime::Runtime;

#[inline]
pub fn rt() -> &'static Runtime {
    static TOKIO_RT: OnceLock<Runtime> = OnceLock::new();
    static PID: OnceLock<u32> = OnceLock::new();
    let pid = std::process::id();
    let runtime_pid = *PID.get_or_init(|| pid);
    if pid != runtime_pid {
        panic!(
            "Forked process detected - current PID is {pid} but the tokio runtime was created by {runtime_pid}. The tokio \
            runtime does not support forked processes https://github.com/tokio-rs/tokio/issues/4301. If you are \
            seeing this message while using Ruby multithreading make sure to use the `spawn` or `forkserver` \
            mode."
        );
    }
    TOKIO_RT.get_or_init(|| Runtime::new().expect("Failed to create a tokio runtime."))
}
