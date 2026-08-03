//! Choosing the address workers dial.
//!
//! A blockwise run has two distinct addresses that are easy to conflate:
//! the interfaces the scheduler *listens* on, and the address it *tells
//! workers to connect to*. They are not the same value, and getting the
//! second one wrong is silent: the driver waits for workers that connect
//! somewhere useless, which looks exactly like workers that failed to start.
//!
//! Binding loopback and advertising `local_addr()` — one value serving both
//! roles — works perfectly on one machine and cannot work at all across
//! nodes, which is the shape of bug that survives every local test.
//!
//! ## What gets chosen, in order
//!
//! 1. An explicit address from the caller (`run_blockwise(host=...)`).
//! 2. `DAISY_HOST`, for stacks that don't thread the parameter through — a
//!    pipeline library between the user and daisy usually doesn't, and an
//!    operator can still set this in a submit script.
//! 3. The host's own name, resolved. On a cluster this is nearly always the
//!    address other nodes reach it on, and unlike a routing probe it does not
//!    assume the node can see the internet.
//! 4. The default-route interface, learned by asking the routing table (a
//!    connected UDP socket sends nothing). Covers laptops and cloud VMs whose
//!    hostname resolves to loopback.
//! 5. Loopback, with a warning — correct for a single-machine run, and the
//!    only honest answer when nothing else is reachable.
//!
//! Candidates from 3 and 4 are *verified* before use: bind them, connect,
//! and push a byte through. An address can look right and refuse traffic —
//! the macOS application firewall does exactly this to non-loopback
//! interfaces — and discovering that from a hung run is expensive.
//!
//! ## Listening
//!
//! The listener binds every interface unless the advertised address is
//! loopback, in which case it binds loopback alone. So a run that says
//! nothing is reachable from other nodes, and a run that explicitly asks for
//! `127.0.0.1` gets a scheduler no other machine can reach — including one
//! that would otherwise be exposed to the local network for a single-process
//! job. The wire protocol has no authentication, so that distinction is worth
//! keeping deliberate.

use std::io::{Read, Write};
use std::net::{IpAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::time::Duration;

use tracing::{debug, warn};

/// Every interface. What we listen on unless told to stay on loopback.
pub const BIND_ALL: &str = "0.0.0.0";

/// Overrides the auto-detected address without touching any code between the
/// caller and daisy.
pub const HOST_ENV_VAR: &str = "DAISY_HOST";

/// How long a verification round trip may take. Localhost or a local
/// interface answers in microseconds; anything slower is a firewall dropping
/// the connection, and waiting longer only delays the fallback.
const VERIFY_TIMEOUT: Duration = Duration::from_millis(500);

/// The address to hand workers, and the interfaces to listen on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Advertise {
    /// What goes in the worker context as `hostname`.
    pub host: String,
    /// What the listener binds.
    pub bind: String,
}

/// Decide both addresses. `explicit` is the caller's `host=`, if any.
///
/// An explicit address is taken at face value: it may be a DNS name this
/// machine cannot resolve but the compute nodes can, or a container-mapped
/// address, and second-guessing it would defeat the point of the override.
pub fn resolve(explicit: Option<&str>) -> Advertise {
    if let Some(host) = explicit.filter(|h| !h.is_empty()) {
        return Advertise::from_host(host);
    }
    if let Ok(host) = std::env::var(HOST_ENV_VAR) {
        if !host.is_empty() {
            debug!(%host, "advertising address from {HOST_ENV_VAR}");
            return Advertise::from_host(&host);
        }
    }
    for (source, candidate) in [
        ("hostname", hostname_ip()),
        ("default route", default_route_ip()),
    ] {
        let Some(ip) = candidate else { continue };
        if ip.is_loopback() {
            continue;
        }
        if is_usable(ip) {
            debug!(%ip, source, "advertising auto-detected address");
            return Advertise::from_host(&ip.to_string());
        }
        warn!(
            %ip, source,
            "address failed its connectivity check and will not be advertised \
             (a firewall may be blocking non-loopback interfaces)"
        );
    }
    warn!(
        "no reachable non-loopback address found; advertising 127.0.0.1. Workers \
         on other machines cannot connect — pass run_blockwise(host=...) or set \
         {HOST_ENV_VAR} if this run has remote workers"
    );
    Advertise::from_host("127.0.0.1")
}

impl Advertise {
    /// Loopback stays private; anything else has to be reachable from
    /// elsewhere, which means listening on every interface.
    fn from_host(host: &str) -> Self {
        let loopback = host
            .parse::<IpAddr>()
            .map(|ip| ip.is_loopback())
            .unwrap_or_else(|_| host.eq_ignore_ascii_case("localhost"));
        Self {
            host: host.to_string(),
            bind: if loopback {
                host.to_string()
            } else {
                BIND_ALL.to_string()
            },
        }
    }
}

/// This host's own name, resolved to an IPv4 address.
fn hostname_ip() -> Option<IpAddr> {
    let name = hostname()?;
    // Port 0 is irrelevant; `to_socket_addrs` just needs one.
    (name.as_str(), 0u16)
        .to_socket_addrs()
        .ok()?
        .find(|addr| addr.is_ipv4() && !addr.ip().is_loopback())
        .map(|addr| addr.ip())
}

fn hostname() -> Option<String> {
    // SAFETY: gethostname writes at most `len` bytes into the buffer, which
    // we own; we then read only up to the first NUL.
    let mut buf = vec![0u8; 256];
    let rc = unsafe { libc::gethostname(buf.as_mut_ptr() as *mut libc::c_char, buf.len()) };
    if rc != 0 {
        return None;
    }
    let end = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    buf.truncate(end);
    String::from_utf8(buf).ok().filter(|s| !s.is_empty())
}

/// The local address of the interface the routing table would use to leave
/// this host. Connecting a UDP socket sends no packets — it only asks the
/// kernel which source address it would pick — so this needs no reachable
/// peer and costs nothing.
fn default_route_ip() -> Option<IpAddr> {
    let socket = UdpSocket::bind((BIND_ALL, 0u16)).ok()?;
    // TEST-NET-1 (RFC 5737): reserved for documentation, so this cannot be
    // mistaken for real traffic to someone's DNS server.
    socket.connect(("192.0.2.1", 80u16)).ok()?;
    socket.local_addr().ok().map(|addr| addr.ip())
}

/// Can this address actually carry a connection? Bind it, dial it, push a
/// byte through, read it back.
///
/// Single-threaded on purpose: the kernel completes the handshake from the
/// listen backlog, so `connect` returns before anything calls `accept`.
fn is_usable(ip: IpAddr) -> bool {
    const PROBE: &[u8] = b"daisy";

    let Ok(listener) = TcpListener::bind((ip, 0u16)) else {
        return false;
    };
    let Ok(addr) = listener.local_addr() else {
        return false;
    };
    let Ok(mut client) = TcpStream::connect_timeout(&addr, VERIFY_TIMEOUT) else {
        return false;
    };
    if client.write_all(PROBE).is_err() {
        return false;
    }
    let Ok((mut conn, _)) = listener.accept() else {
        return false;
    };
    if conn.set_read_timeout(Some(VERIFY_TIMEOUT)).is_err() {
        return false;
    }
    let mut buf = [0u8; PROBE.len()];
    conn.read_exact(&mut buf).is_ok() && buf == PROBE
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_explicit_address_is_taken_verbatim() {
        // Including names this machine cannot resolve: the compute nodes can,
        // and that is the caller's business, not ours.
        let a = resolve(Some("node07.cluster.internal"));
        assert_eq!(a.host, "node07.cluster.internal");
        assert_eq!(a.bind, BIND_ALL, "a routable name must listen everywhere");
    }

    #[test]
    fn explicit_loopback_stays_private() {
        for host in ["127.0.0.1", "localhost", "LOCALHOST", "::1"] {
            let a = resolve(Some(host));
            assert_eq!(a.host, host);
            assert_eq!(
                a.bind, host,
                "{host} is loopback; the listener must not open other interfaces"
            );
        }
    }

    #[test]
    fn an_empty_explicit_address_is_ignored() {
        // `host=""` from a shell variable that didn't get set should fall
        // through to detection, not bind the empty string.
        let a = resolve(Some(""));
        assert!(!a.host.is_empty());
    }

    #[test]
    fn detection_yields_something_bindable() {
        let a = resolve(None);
        assert!(!a.host.is_empty());
        // Whatever it picked, we must be able to listen on it.
        assert!(
            TcpListener::bind((a.bind.as_str(), 0u16)).is_ok(),
            "cannot bind the chosen interface {}",
            a.bind
        );
    }

    #[test]
    fn a_non_loopback_choice_listens_on_every_interface() {
        let a = resolve(None);
        if a.host == "127.0.0.1" {
            // CI sandboxes often have no usable external interface; the
            // fallback is the correct answer there, and it must stay private.
            assert_eq!(a.bind, "127.0.0.1");
        } else {
            assert_eq!(a.bind, BIND_ALL);
        }
    }

    #[test]
    fn loopback_verifies_as_usable() {
        // Sanity check on the verifier itself: if loopback fails this, the
        // check is broken rather than the network.
        assert!(is_usable("127.0.0.1".parse().unwrap()));
    }

    #[test]
    fn an_unbindable_address_is_not_usable() {
        // No interface owns this, so binding it must fail rather than hang.
        assert!(!is_usable("192.0.2.1".parse().unwrap()));
    }

    #[test]
    fn hostname_is_readable() {
        let name = hostname().expect("gethostname should work on a test host");
        assert!(!name.contains('\0'), "NUL leaked into {name:?}");
    }
}

#[cfg(test)]
mod show {
    /// Not an assertion — prints what this host resolves to, so a failing
    /// deployment can be diagnosed with `cargo test -- --nocapture`.
    #[test]
    fn what_this_host_advertises() {
        let a = super::resolve(None);
        println!("advertise host={} bind={}", a.host, a.bind);
    }
}
