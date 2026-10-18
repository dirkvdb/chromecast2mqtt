//! Continuous mDNS discovery, including service updates and removals.

use std::net::{Ipv6Addr, SocketAddr, SocketAddrV6};

use anyhow::{Context, Result};
use mdns_sd::{ResolvedService, ScopedIp, ServiceDaemon, ServiceEvent};

const SERVICE_TYPE: &str = "_googlecast._tcp.local.";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Device {
    pub service: String,
    pub name: String,
    pub model: String,
    pub address: SocketAddr,
    pub cast_type: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DiscoveryEvent {
    Resolved(Device),
    Removed(String),
}

pub struct Discovery {
    daemon: ServiceDaemon,
    receiver: mdns_sd::Receiver<ServiceEvent>,
}

impl Discovery {
    pub fn new() -> Result<Self> {
        let daemon = ServiceDaemon::new().context("starting mDNS discovery")?;
        let receiver = match daemon.browse(SERVICE_TYPE) {
            Ok(receiver) => receiver,
            Err(error) => {
                let _ = daemon.shutdown();
                return Err(error).context("browsing Google Cast services");
            }
        };
        Ok(Self { daemon, receiver })
    }

    /// Cancellation-safe: no detached blocking receive task can consume a later event.
    pub async fn next(&self) -> Result<DiscoveryEvent> {
        loop {
            let event = self
                .receiver
                .recv_async()
                .await
                .context("mDNS discovery stopped")?;
            if let Some(event) = discovery_event(event) {
                return Ok(event);
            }
        }
    }
}

impl Drop for Discovery {
    fn drop(&mut self) {
        let _ = self.daemon.stop_browse(SERVICE_TYPE);
        let _ = self.daemon.shutdown();
    }
}

fn discovery_event(event: ServiceEvent) -> Option<DiscoveryEvent> {
    match event {
        ServiceEvent::ServiceResolved(service) => device(&service).map(DiscoveryEvent::Resolved),
        ServiceEvent::ServiceRemoved(_, fullname) => Some(DiscoveryEvent::Removed(fullname)),
        _ => None,
    }
}

fn socket_address(address: &ScopedIp, port: u16) -> Option<SocketAddr> {
    match address {
        ScopedIp::V4(address) => Some(SocketAddr::new((*address.addr()).into(), port)),
        ScopedIp::V6(address) => Some(ipv6_socket(*address.addr(), port, address.scope_id().index)),
        _ => None,
    }
}

fn ipv6_socket(address: Ipv6Addr, port: u16, interface: u32) -> SocketAddr {
    SocketAddr::V6(SocketAddrV6::new(
        address,
        port,
        0,
        if address.is_unicast_link_local() {
            interface
        } else {
            0
        },
    ))
}

fn device(service: &ResolvedService) -> Option<Device> {
    if !service.is_valid() || service.get_port() == 0 {
        return None;
    }
    // Deterministic selection avoids unnecessary reconnects when HashSet order changes.
    let address = service
        .get_addresses()
        .iter()
        .filter_map(|address| socket_address(address, service.get_port()))
        .min_by_key(|address| (!address.is_ipv4(), *address))?;
    let model = service
        .get_property_val_str("md")
        .unwrap_or_default()
        .to_owned();
    let capabilities = service
        .get_property_val_str("ca")
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0);
    let cast_type = if model == "Google Cast Group" {
        "group"
    } else if capabilities & 1 != 0 {
        "cast"
    } else {
        "audio"
    };
    Some(Device {
        service: service.get_fullname().to_owned(),
        name: service
            .get_property_val_str("fn")
            .filter(|name| !name.is_empty())
            .unwrap_or(service.get_fullname())
            .to_owned(),
        model,
        address,
        cast_type: cast_type.to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use mdns_sd::ServiceInfo;

    fn service(model: &str, capabilities: &str, addresses: &str) -> ResolvedService {
        ServiceInfo::new(
            SERVICE_TYPE,
            "device",
            "cast.local.",
            addresses,
            8009,
            &[("fn", "Living Room"), ("md", model), ("ca", capabilities)][..],
        )
        .unwrap()
        .as_resolved_service()
    }

    #[test]
    fn properties_capabilities_and_address_preference() {
        let resolved = service("Chromecast", "5", "2001:db8::1,192.0.2.20,192.0.2.10");
        let device = device(&resolved).unwrap();
        assert_eq!(device.name, "Living Room");
        assert_eq!(device.model, "Chromecast");
        assert_eq!(device.cast_type, "cast");
        assert_eq!(device.address, "192.0.2.10:8009".parse().unwrap());
        assert_eq!(device.service, "device._googlecast._tcp.local.");
        assert_eq!(
            super::device(&service("Google Home", "4", "192.0.2.1"))
                .unwrap()
                .cast_type,
            "audio"
        );
        assert_eq!(
            super::device(&service("Google Cast Group", "5", "192.0.2.1"))
                .unwrap()
                .cast_type,
            "group"
        );
        assert_eq!(
            super::device(&service("unknown", "invalid", "192.0.2.1"))
                .unwrap()
                .cast_type,
            "audio"
        );
    }

    #[test]
    fn ipv6_fallback_and_unresolved_events() {
        assert_eq!(
            device(&service("TV", "1", "2001:db8::1")).unwrap().address,
            "[2001:db8::1]:8009".parse().unwrap()
        );
        let mut unresolved = service("TV", "1", "192.0.2.1");
        unresolved.addresses.clear();
        assert!(device(&unresolved).is_none());
        assert!(discovery_event(ServiceEvent::ServiceFound(
            SERVICE_TYPE.into(),
            "pending".into()
        ))
        .is_none());
        assert_eq!(
            discovery_event(ServiceEvent::ServiceRemoved(
                SERVICE_TYPE.into(),
                "gone".into()
            )),
            Some(DiscoveryEvent::Removed("gone".into()))
        );
    }

    #[test]
    fn ipv6_link_local_retains_interface_scope() {
        assert_eq!(
            ipv6_socket("fe80::1234".parse().unwrap(), 8009, 7),
            "[fe80::1234%7]:8009".parse().unwrap(),
        );
        assert_eq!(
            ipv6_socket("2001:db8::1234".parse().unwrap(), 8009, 7),
            "[2001:db8::1234]:8009".parse().unwrap(),
        );
    }

    #[test]
    fn missing_properties_and_incomplete_resolutions() {
        let mut resolved = service("TV", "1", "192.0.2.1");
        resolved.txt_properties = Default::default();
        let found = device(&resolved).unwrap();
        assert_eq!(found.name, resolved.fullname);
        assert_eq!(found.model, "");
        assert_eq!(found.cast_type, "audio");
        resolved.port = 0;
        assert!(device(&resolved).is_none());
        resolved.port = 8009;
        resolved.host.clear();
        assert!(device(&resolved).is_none());
    }

    #[test]
    fn repeated_resolutions_are_not_deduplicated_by_address() {
        let old = service("TV", "1", "192.0.2.1");
        let mut renamed = old.clone();
        renamed.txt_properties = service("Speaker", "4", "192.0.2.1").txt_properties;
        let first = discovery_event(ServiceEvent::ServiceResolved(Box::new(old))).unwrap();
        let second = discovery_event(ServiceEvent::ServiceResolved(Box::new(renamed))).unwrap();
        assert_ne!(first, second);
        let mut renamed = service("TV", "1", "192.0.2.1");
        renamed.txt_properties = ServiceInfo::new(
            SERVICE_TYPE,
            "device",
            "cast.local.",
            "192.0.2.1",
            8009,
            &[("fn", "Renamed TV"), ("md", "TV"), ("ca", "1")][..],
        )
        .unwrap()
        .as_resolved_service()
        .txt_properties;
        let DiscoveryEvent::Resolved(updated) =
            discovery_event(ServiceEvent::ServiceResolved(Box::new(renamed))).unwrap()
        else {
            panic!("expected a resolved device");
        };
        assert_eq!(updated.name, "Renamed TV");
        assert_eq!(updated.service, "device._googlecast._tcp.local.");
    }
}
