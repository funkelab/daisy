//! TCP keepalive for the server↔worker connections.
//!
//! Both ends of a daisy run can legitimately sit in a blocking read for a
//! long time: a parked worker waits in `acquire_block` until dependency-
//! gated blocks unlock, and the server waits on each worker's socket for
//! its next message. On a healthy connection that's fine. The failure
//! mode is a *half-open* connection: the peer's whole machine vanishes
//! (cloud scale-down deprovisions the node, a cluster job's node is
//! reclaimed) so no FIN or RST is ever sent. A blocking read on such a
//! connection waits forever — the peer's death is only ever reported by
//! the peer's own OS, and that OS no longer exists. This is exactly how
//! workers outlived a dead driver by 12+ hours: the 138 workers that
//! touched the socket while the driver's *host* was still up got an
//! instant RST and died; the ones that reached the socket after the host
//! was deprovisioned would have waited forever.
//!
//! Keepalive is the right tool (rather than a read timeout) because it
//! probes the peer *while the connection is idle* — precisely the state
//! a parked worker is in — without putting any ceiling on how long a
//! legitimate wait may last.
//!
//! Detection budget with the values below: the OS starts probing after
//! [`IDLE`] of silence and gives up after [`RETRIES`] unanswered probes
//! [`INTERVAL`] apart — a vanished peer surfaces as a read error in
//! about `IDLE + RETRIES × INTERVAL` ≈ 2 minutes (Windows does not
//! expose the retry count and uses 10 probes ≈ 3.5 minutes). Note that
//! keepalive only governs an idle connection; when there is unacked
//! outbound data, the retransmission timeout (~15–30 min at Linux
//! defaults) reports the death instead — later, but still bounded.
//! Combined with the per-block watchdog (`client::arm_watchdog`), this
//! caps an orphaned worker's lifetime at roughly `Task(timeout)` plus
//! keepalive detection, instead of "until someone runs scancel".

use socket2::{SockRef, TcpKeepalive};
use std::time::Duration;
use tokio::net::TcpStream;

/// Silence on the connection before the OS starts probing.
pub const IDLE: Duration = Duration::from_secs(60);

/// Gap between unanswered probes.
pub const INTERVAL: Duration = Duration::from_secs(15);

/// Unanswered probes before the connection is declared dead
/// (platforms that expose it; Windows fixes this at 10).
pub const RETRIES: u32 = 4;

/// Enable keepalive on a daisy connection. Called on the worker's
/// client socket at connect and on every socket the server accepts.
///
/// Failure is returned rather than unwrapped: an exotic platform that
/// rejects an option should degrade to the old behaviour (no liveness
/// probing) with a warning, not refuse to run.
pub fn apply(stream: &TcpStream) -> std::io::Result<()> {
    let ka = TcpKeepalive::new().with_time(IDLE);
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "windows",
    ))]
    let ka = ka.with_interval(INTERVAL);
    #[cfg(any(
        target_os = "android",
        target_os = "dragonfly",
        target_os = "freebsd",
        target_os = "fuchsia",
        target_os = "illumos",
        target_os = "ios",
        target_os = "linux",
        target_os = "macos",
        target_os = "netbsd",
        target_os = "tvos",
        target_os = "watchos",
    ))]
    let ka = ka.with_retries(RETRIES);
    SockRef::from(stream).set_tcp_keepalive(&ka)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keepalive_is_enabled_with_the_configured_idle_time() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let stream = TcpStream::connect(addr).await.unwrap();

            apply(&stream).expect("setting keepalive should succeed on CI platforms");

            let sock = SockRef::from(&stream);
            assert!(sock.keepalive().unwrap(), "SO_KEEPALIVE not enabled");
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            {
                assert_eq!(sock.tcp_keepalive_time().unwrap(), IDLE);
                assert_eq!(sock.tcp_keepalive_interval().unwrap(), INTERVAL);
            }
            #[cfg(target_os = "linux")]
            assert_eq!(sock.tcp_keepalive_retries().unwrap(), RETRIES);
        });
    }
}
