use pnet::datalink::{Config, DataLinkReceiver, DataLinkSender, NetworkInterface, interfaces};
use pnet::packet::icmp::echo_request::{EchoRequestPacket, MutableEchoRequestPacket};
use pnet::packet::ethernet::{EtherTypes, MutableEthernetPacket};
use pnet::packet::ip::IpNextHeaderProtocols;
use pnet::packet::ipv4::{self, Ipv4, MutableIpv4Packet};
use pnet::packet::tcp::{self, MutableTcpPacket};
use pnet::packet::{MutablePacket, Packet};
use pnet::util::MacAddr;
use std::collections::VecDeque;
use std::net::{Ipv4Addr, SocketAddrV4};
use std::sync::mpsc::{Receiver, Sender, channel};
use std::thread::{self, JoinHandle};
use std::time::Duration;

enum ThreadMsg {
    /// Stop thread execution
    Halt,
    /// Send data (only valid for the sending thread)
    Send(Vec<u8>),
    /// Check if the recieving thread has new data (only valid for the recieving thread)
    Poll,
    /// Return value of `tx.send(ThreadMsg::Poll)`
    PollResult(u32),
    /// Retrieve packet data from recieving thread
    GetPackets,
    /// Return value of `tx.send(ThreadMsg::GetPackets)`
    GetPacketsResult(Vec<Vec<u8>>)
}
struct ThreadSpawnIntermediary {
    pub send_thread: JoinHandle<()>,
    pub recieve_thread: JoinHandle<()>
}

/// (URG, ACK, PSH, RST, SYN, FIN)
struct TcpFlags {
    urg: bool,
    ack: bool,
    psh: bool,
    rst: bool,
    syn: bool,
    fin: bool
}
impl TcpFlags {
    pub fn into_u8(&self) -> u8 {
        let mut out = 0_u8;
        if self.fin == true {
            out += 0b00000001
        }
        if self.syn == true {
            out += 0b00000010
        }
        if self.rst == true {
            out += 0b00000100
        }
        if self.psh == true {
            out += 0b00001000
        }
        if self.ack == true {
            out += 0b00010000
        }
        if self.urg == true {
            out += 0b00100000
        }
        return out;
    }
}
pub struct LTCPStream {
    connected: bool,
    rx_buffer: [u8; 65535],
    rx_packets: Vec<Vec<u8>>,
    tx_queue: VecDeque<Vec<u8>>,
    tx_count: u64,
    rx_count: u64,
    interface: NetworkInterface,
    sender: Box<dyn DataLinkSender>,
    reciever: Box<dyn DataLinkReceiver>,
    keepalive_handle: Option<JoinHandle<()>>,
    keepalive_sender: Option<Sender<u8>>,
    ethernet_packet_header: Vec<u8>, 
    ipv4_packet_header: Vec<u8>,
    remote_mac: MacAddr,
    local_mac: MacAddr,
    remote_ip: Ipv4Addr,
    local_ip: Ipv4Addr,
    remote_port: u16,
    local_port: u16,
    sequence: u32,
    ackno: u32,
}
impl LTCPStream {
    fn gen_packet_raw(data: Option<Vec<u8>>, flags: TcpFlags, remote_mac: MacAddr, local_mac: MacAddr, remote_ip: Ipv4Addr, local_ip: Ipv4Addr, remote_port: u16, local_port: u16, sequence: u32, ackno: u32) -> Result<Vec<u8>, ()> {
        let length: u16;
        if data.is_some() {
            let rlength = data.unwrap().len();
            if rlength >= (u16::MAX - 20) as usize {
                return Err(())
            } else {
                length = rlength as u16;
            }
        } else {
            length = 40;
        } 

        let mut packet_buffer = [0_u8; 128];

        let mut eth_packet = MutableEthernetPacket::new(& mut packet_buffer).unwrap();
        eth_packet.set_destination(remote_mac);
        eth_packet.set_source(local_mac);
        eth_packet.set_ethertype(EtherTypes::Ipv4);

        let mut ipv4_packet = MutableIpv4Packet::new(eth_packet.payload_mut()).unwrap();
        ipv4_packet.set_version(4);
        ipv4_packet.set_header_length(5);
        ipv4_packet.set_total_length(20 + length);
        ipv4_packet.set_ttl(64);
        ipv4_packet.set_next_level_protocol(IpNextHeaderProtocols::Tcp);
        ipv4_packet.set_source(local_ip);
        ipv4_packet.set_destination(remote_ip);
        ipv4_packet.set_checksum(ipv4::checksum(&ipv4_packet.to_immutable()));

        let mut tcp_packet = MutableTcpPacket::new(ipv4_packet.payload_mut()).unwrap();
        tcp_packet.set_source(local_port);
        tcp_packet.set_destination(remote_port);
        tcp_packet.set_sequence(sequence); 
        tcp_packet.set_acknowledgement(ackno);
        tcp_packet.set_flags(flags.into_u8());
        tcp_packet.set_window(64256);
        tcp_packet.set_checksum(pnet::packet::tcp::ipv4_checksum(&tcp_packet.to_immutable(), &local_ip, &remote_ip));
        tcp_packet.set_urgent_ptr(0);

        return Ok(packet_buffer.to_vec())
    }
    /// only valid after connect
    fn gen_packet(&self, data: Option<Vec<u8>>, flags: TcpFlags) -> Result<Vec<u8>, ()> {
        if self.connected == false {
            return Err(())
        }
        Self::gen_packet_raw(
            data,
            flags,
            self.remote_mac,
            self.local_mac,
            self.remote_ip,
            self.local_ip,
            self.remote_port,
            self.local_port,
            self.sequence,
            self.ackno
        )
    }
    fn send_thread_logic(from_main: Receiver<ThreadMsg>, to_main: Sender<ThreadMsg>) -> () {
        todo!();
    }
    fn recieve_thread_logic(from_main: Receiver<ThreadMsg>, to_main: Sender<ThreadMsg>) -> () {
        todo!();
    }
    fn spawn_threads(to_send: Receiver<ThreadMsg>, to_recieve: Receiver<ThreadMsg>, from_send: Sender<ThreadMsg>, from_recieve: Sender<ThreadMsg>) -> Result<ThreadSpawnIntermediary, ()> {
        // send thread
        let st = match thread::Builder::new()
        .name("sn-thread".to_string())
        .spawn(
            || {
                Self::send_thread_logic(to_send, from_send);
            }
        ){
            Ok(h) => h,
            Err(_) => return Err(())
        };

        //recieve thread
        let rt = match thread::Builder::new()
        .name("rx-thread".to_string())
        .spawn(
            || {
                Self::recieve_thread_logic(to_recieve, from_recieve);
            }
        ) {
            Ok(h) => h,
            Err(_) => return Err(())
        };

        return Ok(ThreadSpawnIntermediary {
            send_thread: st,
            recieve_thread: rt
        })
    }

    /// Creates a TCP stream to a destination with a mac address on a specified interface.
    pub fn connect(addr: SocketAddrV4, remote_mac: MacAddr, local_port: u16, ifname: String, keepalive: Option<Duration>) -> Result<Self, ()> {
        let remote_ip = addr.ip();
        let remote_port = addr.port();

        let interfaces = interfaces();
        let interface = match interfaces.iter().find(
            |i| {
                i.is_up() && i.name == ifname && !i.is_loopback()
            }
        ) {
            Some(i) => i,
            None => return Err(())
        };

        let local_ip = Ipv4Addr::new(10,0,1,125);


        let configuration = Config {
            ..Default::default()
        };

        let (mut dltx, mut dlrx) = match pnet::datalink::channel(interface, configuration) {
            Ok(pnet::datalink::Channel::Ethernet(t,r )) => (t,r),
            Ok(_) => return Err(()),
            Err(_) => return Err(()),
        };

        let local_mac = match interface.mac {
            Some(m) => m,
            None => return Err(()),
        };

        let seqn = 0;
        let ano  = 0; 

        // THREADS
        let (ts, fts) = channel::<ThreadMsg>();
        let (fs, ffs) = channel::<ThreadMsg>();
        let (tr, ftr) = channel::<ThreadMsg>();
        let (fr, ffr) = channel::<ThreadMsg>();
        let send_jh: JoinHandle<()>;
        let recieve_jh: JoinHandle<()>;
        match Self::spawn_threads(fts, ffr, fs, fr) {
            Ok(tsi) => {
                send_jh = tsi.send_thread;
                recieve_jh = tsi.recieve_thread;
            },
            Err(_) => return Err(())
        }

        // SENDING SYN
        let syn_packet = Self::gen_packet_raw(None,
            TcpFlags { 
                urg: false, 
                ack: false, 
                psh: false, 
                rst: false, 
                syn: true, 
                fin: false }, 
            remote_mac, 
            local_mac, 
            *remote_ip, 
            local_ip, 
            remote_port, 
            local_port, 
            seqn, 
            ano
        ).unwrap();
        ts.send(ThreadMsg::Send(syn_packet));

        // RECIEVING SYN ACK

        // SENDING ACK

        // CONNECTED

        return Err(());
        // return Ok(Self {
        //     rx_buffer: [0_u8; 65535],
        //     rx_packets: Vec::new(),
        //     tx_queue: VecDeque::new(),
        //     tx_count: 0,
        //     rx_count: 0,
        //     interface: interface.clone()
        // })

    }
    pub fn send(data: Vec<u8>) -> Result<usize, ()> {
        Err(())
    }
}