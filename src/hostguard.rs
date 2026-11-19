//! Host guard for article fetches. Article URLs come from search results, so they are untrusted, and the bot
//! runs next to Grafana, Prometheus, the Pushgateway and the slot-writer on mini-mac. A fetch must never reach
//! a local, private or tailnet host, whether by URL, by DNS answer, or by redirect.
//!
//! [`HostGuard::check_url`] runs before every request (the caller sends each redirect hop as its own request,
//! see `Http::send_capped`) and resolves the name itself. The same rules also filter the answers of the
//! article client's DNS resolver ([`HostGuard::resolver`]), so a name that changes its answer between the
//! check and the connection still cannot reach a blocked address.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use reqwest::Url;

/// Which addresses an article fetch may reach: public ones only, unless built with
/// [`HostGuard::allowing_loopback`] (tests).
#[derive(Debug, Clone, Default)]
pub struct HostGuard {
    allow_loopback: bool,
}

impl HostGuard {
    /// Public hosts only.
    pub fn strict() -> HostGuard {
        HostGuard {
            allow_loopback: false,
        }
    }

    /// Like [`HostGuard::strict`] but loopback addresses and `localhost` are allowed. For tests that talk to a
    /// mock server on 127.0.0.1; everything else (private, tailnet, link-local ...) stays blocked.
    pub fn allowing_loopback() -> HostGuard {
        HostGuard {
            allow_loopback: true,
        }
    }

    /// Whether `ip` may be connected to.
    pub fn ip_allowed(&self, ip: IpAddr) -> bool {
        let ip = canonical(ip);
        if self.allow_loopback && ip.is_loopback() {
            return true;
        }
        !ip_is_blocked(ip)
    }

    /// Whether a host name (not an IP literal) is acceptable before any DNS lookup: not `localhost`, not
    /// `*.local`, `*.ts.net` or `*.internal`, and not a single-label name (an intranet short name such as
    /// `mini-mac` or `grafana`).
    pub fn name_allowed(&self, host: &str) -> bool {
        let host = host.trim_end_matches('.').to_ascii_lowercase();
        if host.is_empty() {
            return false;
        }
        if host == "localhost" || host.ends_with(".localhost") {
            return self.allow_loopback;
        }
        host.contains('.')
            && !host.ends_with(".local")
            && !host.ends_with(".ts.net")
            && !host.ends_with(".internal")
    }

    /// Check `url` before sending: http or https, a host, and an allowed host name or IP. A name is resolved
    /// here and refused when any of its addresses is blocked. A failed lookup is not a refusal (the request
    /// then fails on its own).
    pub async fn check_url(&self, url: &Url) -> Result<(), String> {
        if !matches!(url.scheme(), "http" | "https") {
            return Err(format!("scheme {} is not http or https", url.scheme()));
        }
        match url.host() {
            None => Err("url has no host".to_string()),
            Some(url::Host::Ipv4(ip)) => self.require_ip(IpAddr::V4(ip)),
            Some(url::Host::Ipv6(ip)) => self.require_ip(IpAddr::V6(ip)),
            Some(url::Host::Domain(name)) => {
                if !self.name_allowed(name) {
                    return Err(format!("host name {name} is local or internal"));
                }
                let port = url.port_or_known_default().unwrap_or(80);
                if let Ok(addrs) = tokio::net::lookup_host((name, port)).await {
                    for addr in addrs {
                        if !self.ip_allowed(addr.ip()) {
                            return Err(format!(
                                "{name} resolves to blocked address {}",
                                addr.ip()
                            ));
                        }
                    }
                }
                Ok(())
            }
        }
    }

    fn require_ip(&self, ip: IpAddr) -> Result<(), String> {
        if self.ip_allowed(ip) {
            Ok(())
        } else {
            Err(format!(
                "address {ip} is loopback, private, link-local, tailnet or reserved"
            ))
        }
    }

    /// DNS resolver for the article client: resolves with the system resolver and refuses a name when any of
    /// its addresses is blocked.
    pub(crate) fn resolver(&self) -> GuardedResolver {
        GuardedResolver {
            guard: Arc::new(self.clone()),
        }
    }
}

/// IPv4-mapped IPv6 addresses (`::ffff:a.b.c.d`) are judged as the IPv4 address.
fn canonical(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
        v4 => v4,
    }
}

fn ip_is_blocked(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => v4_is_blocked(v4),
        IpAddr::V6(v6) => v6_is_blocked(v6),
    }
}

fn v4_is_blocked(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_unspecified()
        || ip.is_broadcast()
        || ip.is_multicast()
        || o[0] == 0 // 0.0.0.0/8
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10: CGNAT, includes the tailnet
        || o[0] >= 240 // reserved
}

fn v6_is_blocked(ip: Ipv6Addr) -> bool {
    let first = ip.segments()[0];
    ip.is_loopback()
        || ip.is_unspecified()
        || ip.is_multicast()
        || (first & 0xffc0) == 0xfe80 // fe80::/10 link-local
        || (first & 0xfe00) == 0xfc00 // fc00::/7 unique-local
}

/// reqwest DNS resolver: the lookup fails when any address of the name is blocked.
pub(crate) struct GuardedResolver {
    guard: Arc<HostGuard>,
}

impl Resolve for GuardedResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let guard = self.guard.clone();
        Box::pin(async move {
            let host = name.as_str().to_string();
            let addrs: Vec<SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if let Some(bad) = addrs.iter().find(|a| !guard.ip_allowed(a.ip())) {
                return Err(format!("{host} resolves to blocked address {}", bad.ip()).into());
            }
            let iter: Addrs = Box::new(addrs.into_iter());
            Ok(iter)
        })
    }
}
