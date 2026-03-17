use std::net::Ipv4Addr;
use std::sync::mpsc::TryRecvError;
use std::process::{Command, Stdio, exit}; 
use std::thread::{self, JoinHandle};
use std::{net::IpAddr, time::Duration};
use std::sync::mpsc::{self, Receiver};
use log::{debug, error, info};
use pnet::datalink::{Config, DataLinkReceiver, DataLinkSender, NetworkInterface, interfaces};
use pnet::packet::icmp::echo_request::{EchoRequestPacket, MutableEchoRequestPacket};
use pnet::packet::icmp::{self, IcmpCode, IcmpType, echo_request};
use pnet::packet::icmp::{MutableIcmpPacket, IcmpTypes, IcmpPacket};
use pnet::packet::ethernet::{EtherTypes, MutableEthernetPacket};
use pnet::packet::ip::IpNextHeaderProtocols;
use pnet::packet::ipv4::{self, MutableIpv4Packet};
use pnet::packet::{MutablePacket, Packet};
use pnet::util::MacAddr;

use crate::detector::SysError;
pub enum PingThreadMsg {
    Pause(Duration),
    Halt,
}
fn ping(tx: & mut Box<dyn DataLinkSender>, rx: & mut Box<dyn DataLinkReceiver>, if_mac: MacAddr, iface: &NetworkInterface) -> () {
    // 23.15 disable info!("[PING] Beginning ping...");
    // reference: https://github.com/nanu9000/ping/blob/master/src/main.rs
    let mut buffer = vec![0u8; 128]; // universal buffer
    
    let mut ethernet_packet = MutableEthernetPacket::new(&mut buffer).unwrap();
    ethernet_packet.set_destination(MacAddr::new(0x00, 0x1d, 0x4a, 0x01, 0xe0, 0xbd));
    ethernet_packet.set_source(if_mac);
    ethernet_packet.set_ethertype(EtherTypes::Ipv4);
    
    let mut ipv4_packet=  MutableIpv4Packet::new(ethernet_packet.payload_mut()).unwrap();
    ipv4_packet.set_version(4);
    ipv4_packet.set_header_length(5);
    ipv4_packet.set_total_length(28);
    ipv4_packet.set_ttl(64);
    ipv4_packet.set_next_level_protocol(IpNextHeaderProtocols::Icmp);
    ipv4_packet.set_source(Ipv4Addr::new(10, 0, 1, 125));
    ipv4_packet.set_destination(Ipv4Addr::new(10, 0, 1, 150));
    //ipv4_packet.set_identification(0x000);
    // must be last
    let checksum = ipv4::checksum(&ipv4_packet.to_immutable());
    ipv4_packet.set_checksum(checksum);

    let mut echo_packet = MutableEchoRequestPacket::new(ipv4_packet.payload_mut()).unwrap();
    echo_packet.set_icmp_type(IcmpType::new(8));
    echo_packet.set_icmp_code(IcmpCode::new(0));
    echo_packet.set_identifier(6464_u16);
    echo_packet.set_sequence_number(0_u16);
    echo_packet.set_checksum(pnet::packet::util::checksum(&echo_packet.packet(), 1));

    match tx.send_to(&buffer, Some(iface.clone())) {
        None => info!("[PING] Sent without fault"),
        Some (e) => match &e {
            Ok(_) => info!("[PING] Sent, empty OK"),
            Err(e) => error!("[PING] Failed to send: {e:?}"),
        }
    }
}
fn ping_thread(remote_address: IpAddr, ifname: String, incoming: Receiver<PingThreadMsg>, interval: Duration) -> (){
    //! main function for the async ping thread.
    //! executes a ping at the beginning of every interval.
    //! can be killed by sending PingThreadMsg::Halt.
    //! may be paused for imaging with PingThreadMsg::Pause(Duration).
    // pnet interface finding
    let interface_list = interfaces();
    let interface_config = Config {
        read_timeout: Some(Duration::from_millis(100)),
        ..Default::default()
    };
    let detector_interface = interface_list.iter().find(
        |n| {
            n.is_up() && n.name == ifname && !n.is_loopback()
        }
    );
    if detector_interface.is_none() {
        error!("[PING] Failed to find the detector interface.");
    }
    let (mut icmptx, mut icmprx) = match pnet::datalink::channel(detector_interface.unwrap(), interface_config) {
        Ok(pnet::datalink::Channel::Ethernet(t,r)) => {
            info!("[PING] created datalink channel");
            (t,r)
        },
        Ok(_) => {
            error!("[PING] Did not get an ethernet channel");
            return;
        }
        Err(e) => {
            error!("[PING] Failed to create datalink channel: {e:?}");
            return;
        } 
    };
    let detector_if_mac = match detector_interface.unwrap().mac {
        Some(m) => {
            info!("[PING] Our mac address is: {m:?}");
            m
        },
        None => {
            error!("[PING] Failed to retreive detector MAC address.");
            return
        }
    };

    'outer: loop {
        match incoming.try_recv() {
            Err(e) => {
                match e {
                    TryRecvError::Empty => {
                        ping(& mut icmptx,& mut icmprx, detector_if_mac, detector_interface.unwrap());
                        std::thread::sleep(interval);
                        continue 'outer
                    }
                    TryRecvError::Disconnected => {
                        error!("Main process hung up.");
                        exit(0)
                    }
                }
            },
            Ok(msg) => {
                match msg {
                    PingThreadMsg::Pause(d) => {
                        std::thread::sleep(d);
                        continue 'outer;
                    },
                    PingThreadMsg::Halt => {
                        exit(0)
                    }
                }
            }
        }
    }
}
pub fn create_ping_thread(remote_address: IpAddr,ifname: String ,rx: Receiver<PingThreadMsg>, interval: Duration) -> Result<JoinHandle<()>,std::io::Error>{
    //! creates the ping thread to keep the detector awake.
    //! returns the messaging channel and thread's join handle.
    info!("[PING] Creating ping thread");
    let handle = thread::Builder::new().name("ping-thread".to_string()).spawn(
        move || {
            ping_thread(remote_address,ifname, rx, interval)
        }
    );
    return handle;
}