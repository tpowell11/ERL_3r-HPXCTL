use crate::VERSION;
// use crate::commands::Command;
use crate::detector_config::DetectorConfig;
use crate::image::Image;
use crate::ping_helper::{PingThreadMsg, create_ping_thread};
use crate::tcp_helper::{KeepAliveThreadMsg, create_keepalive_thread, create_log_thread};
use log::error;
use log::{info, trace, warn};
use pnet::util::MacAddr;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use socket2::{Domain, Protocol, SockAddr, Socket, TcpKeepalive, Type};
use std::collections::VecDeque;
use std::io::{self, BufRead, ErrorKind, Read, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4, TcpStream};
use std::str::FromStr;
use std::sync::mpsc::{Sender, channel};
use std::thread::{JoinHandle, sleep};
use std::time::{Duration, SystemTime};
use std::{u64, vec};
use uuid::Uuid;

pub const BASE_PORT: u16 = 60_000;
pub const REMOTE_IMAGE_PORT_OS: u16 = 1;
pub const REMOTE_LOG_PORT_OS: u16 = 2;
pub const REMOTE_RESPONSE_PORT_OS: u16 = 4;

type Command = (&'static str, Option<String>);
type Return = (usize, String);
fn back_value<T: FromStr>(v: Return) -> Option<T> {
    let split: VecDeque<String> = v.1.split_whitespace().map(|s| s.to_string()).collect();
    let val = split.back();
    if val.is_none() {
        return None;
    }
    if let Ok(v) = T::from_str(val.unwrap()) {
        return Some(v);
    } else {
        return None;
    }
}

#[derive(Debug)]
pub enum SysError {
    Connection(&'static str),
    Command(String),
    Internal(&'static str),
    InvalidType(&'static str),
    InvalidInput(&'static str),
    NotConnected,
    UnknownEndpoint(&'static str),
    UnknownID(&'static str),
}
impl SysError {
    pub fn to_http_code(&self) -> usize {
        match self {
            SysError::Connection(_) => 500,
            SysError::Command(_) => 500,
            SysError::Internal(_) => 500,
            SysError::InvalidInput(_) => 403,
            SysError::InvalidType(_) => 405,
            SysError::NotConnected => 503,
            SysError::UnknownEndpoint(_) => 404,
            SysError::UnknownID(_) => 404,
        }
    }
}
impl ToString for SysError {
    fn to_string(&self) -> String {
        match self {
            &SysError::Connection(why) => return format!("Connection Error: {}", why),
            SysError::Command(why) => return format!("Command Error: {}", why),
            &SysError::Internal(why) => return format!("Internal faliure: {}", why),
            &SysError::InvalidType(why) => return format!("Invalid Type: {}", why),
            &SysError::InvalidInput(why) => return format!("Invalid input data: {}", why),
            &SysError::NotConnected => return "Detector is not connected".to_string(),
            &SysError::UnknownEndpoint(why) => return format!("Unknown API endpoint: {}", why),
            &SysError::UnknownID(why) => return format!("Id not found: {}", why),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[allow(non_snake_case)]
pub struct DetectorInfo {
    SerialNum: String,
    PanelID: String,
    MACAddress: String,
}
impl DetectorInfo {
    pub fn new() -> Self {
        Self {
            SerialNum: String::default(),
            PanelID: String::default(),
            MACAddress: String::default(),
        }
    }
}

fn get_port(o: &Option<TcpStream>, which: &str) -> u16 {
    let p = o.as_ref().unwrap().local_addr().unwrap().port();
    info!("{} port is: {}", which, p);
    return p;
}
// patch 12 kill readn_usize and readn_text
// patch 23: remove gratuitous tcp keepalive infra

/// State persistance for the detector.
/// Contains all detector-facing command logic.
#[derive(Debug)]
pub struct Detector {
    command_count: u64,
    config: DetectorConfig,
    images: Vec<Image>,
    currentimage: Uuid,
    detectorinfo: Option<DetectorInfo>,
    has_connected: bool,
    root_stream: Option<TcpStream>,
    root_keepalive_handle: Option<JoinHandle<()>>,
    root_keepalive_sender: Option<Sender<KeepAliveThreadMsg>>,
    response_stream: Option<TcpStream>,
    log_stream: Option<TcpStream>,
    image_stream: Option<TcpStream>,
    command_stream: Option<TcpStream>,
    ping_thread_handle: Option<JoinHandle<()>>,
    ping_thread_sender: Option<Sender<PingThreadMsg>>,
    tcp_keepalive_handle: Option<JoinHandle<()>>,
    tcp_keepalive_sender: Option<Sender<KeepAliveThreadMsg>>,
    log_keepalive_handle: Option<JoinHandle<()>>,
    /*
    Host                    HPX                     Remote Offset           Local Offset        Remark
    local_base_port     <-> BASE_PORT               fixed, 60000            fixed, OS assigned  none
    local_command_port  <-> remote_command_port     +0, given by detector   +1                  TCP keepalive origin
    local_image_port    <-- remote_image_port       +1                      +2
    local_log_port      <-- remote_log_port         +2                      +3
    local_response_port <-- remote_response_port    +4                      +4
    */
    /// Commands going to the detector
    remote_command_port: u16,
    /// Logs coming from the detector
    remote_log_port: u16,
    /// Responses from the detector
    remote_response_port: u16,
    /// Images from the detector
    remote_image_port: u16,
    /// Local initial port
    local_base_port: u16,
    /// Local command output port
    local_command_port: u16,
    /// Local log input port
    local_log_port: u16,
    /// Local response input port
    local_response_port: u16,
    /// Local image input port
    local_image_port: u16,
    // program interface config
    ifconfig: crate::config::Config,
}

impl Detector {
    /// Constructs a "zeroed" instance of [`crate::detector::Detector`].
    /// All stream options are set to `None`.
    pub fn new(config: &crate::config::Config) -> Self {
        Self {
            command_count: 2_u64, // patch 7 REVERTED patch 21.1
            config: DetectorConfig::default(),
            images: Vec::new(),
            currentimage: Uuid::nil(),
            detectorinfo: None,
            has_connected: false,
            root_stream: None,
            root_keepalive_sender: None,
            root_keepalive_handle: None,
            response_stream: None,
            log_stream: None,
            image_stream: None,
            command_stream: None,
            ping_thread_handle: None,
            ping_thread_sender: None,
            tcp_keepalive_handle: None,
            tcp_keepalive_sender: None,
            log_keepalive_handle: None,
            remote_command_port: BASE_PORT,
            remote_log_port: 0,
            remote_response_port: 0,
            remote_image_port: 0,
            local_base_port: 0,
            local_command_port: 0,
            local_log_port: 0,
            local_response_port: 0,
            local_image_port: 0,
            ifconfig: config.clone(),
        }
    }
    /// Initializes communication with the detector and begins keepalive procedures.
    /// This method can only be called once, and no other methods except [`Self::new()`] may be called without first calling this method.
    /// All other methods ensure that `connect()` has been called before attempting to execute any further insturctions.
    pub fn connect(&mut self, ifname: String) -> Result<(), SysError> {
        if self.has_connected {
            warn!("Connection request recived despite already being connected.");
            return Err(SysError::Connection("Already connected"));
        }

        let detector_ipv4: Ipv4Addr = Ipv4Addr::from_str(self.ifconfig.detector_ip.as_str())
            .expect("Invalid detector IPv4 in config.");
        let detector_mac: MacAddr = MacAddr::from_str(self.ifconfig.detector_mac.as_str())
            .expect("Invalid MAC address in config");
        // patch 23: remove arp
        // patch 23: implement hail ping
        let (ptx, prx) = channel::<PingThreadMsg>();
        match create_ping_thread(
            IpAddr::V4(detector_ipv4),
            ifname,
            prx,
            Duration::from_millis(1000),
        ) {
            Ok(h) => {
                self.ping_thread_sender = Some(ptx);
                self.ping_thread_handle = Some(h);
            }
            Err(e) => {
                error!("Failed to start ping thread: {e:?}");
                return Err(SysError::Connection("Failed to start ping thread"));
            }
        };

        info!(
            "Connecting to detector at {:?}:{:?}",
            detector_ipv4, BASE_PORT
        );
        let detector_socket = match Socket::new(Domain::IPV4, Type::STREAM, None) {
            Ok(s) => s,
            Err(e) => {
                error!("Failed to create detector socket. {:?}", e);
                panic!();
            }
        };
        let detector_socket_address = SockAddr::from(SocketAddrV4::new(
            detector_ipv4.to_string().parse::<Ipv4Addr>().unwrap(),
            BASE_PORT,
        ));
        match detector_socket.connect(&detector_socket_address) {
            Ok(_) => {
                info!("Connected to detector socket.")
            }
            Err(e) => {
                error!("Failed to connect to detector socket: {:?}", e)
            }
        };

        // Stream creation
        let mut init_stream: TcpStream = detector_socket.into();
        self.local_base_port = init_stream.local_addr().unwrap().port();
        info!(
            "Bound command stream to remote base port: {} -> 60000; local ip:{:?}",
            self.local_base_port,
            init_stream.local_addr().unwrap().ip()
        );
        let command_text = "00010009Reserve 1".as_bytes();
        match init_stream.write(command_text) {
            Ok(n) => {
                info!("Wrote {n} bytes to stream.")
            }
            Err(e) => {
                error!("Failed to write. error: {e:?}");
                return Err(SysError::Connection("Could not write to init stream"));
            }
        };
        //block until we get a response
        let mut init_buf: Vec<u8> = Vec::new();
        let init_bytes_rx = init_stream.read_to_end(&mut init_buf).expect("A");
        info!(
            "Init buffer: {}, ({})",
            String::from_utf8(init_buf.clone()).unwrap(),
            init_bytes_rx
        );

        //processing the result
        match init_buf.len() {
            0 => {
                warn!("No response to Reserve request");
                return Err(SysError::Connection("Detector did not respond to reserve"));
            }
            1.. => {
                let text = String::from_utf8(init_buf).unwrap();
                let vd: VecDeque<String> = text.split_whitespace().map(|s| s.to_string()).collect();
                let new_port = match vd.back().unwrap().parse::<u16>() {
                    Ok(v) => {
                        info!("Reserve succeeded");
                        v
                    }
                    Err(_) => {
                        info!("Reserve failed");
                        return Err(SysError::Connection("Failed to reserve"));
                    }
                };
                self.remote_command_port = new_port;
                self.remote_image_port = new_port + REMOTE_IMAGE_PORT_OS;
                self.remote_log_port = new_port + REMOTE_LOG_PORT_OS;
                self.remote_response_port = new_port + REMOTE_RESPONSE_PORT_OS;
                info!("Remote command port is: {}", self.remote_command_port);
                info!("Remote image port is: {}", self.remote_image_port);
                info!("Remote log port is: {}", self.remote_log_port);
                info!("Remote response port is: {}", self.remote_response_port);

                // TCP STREAM CREATION
                self.response_stream =
                    match TcpStream::connect((detector_ipv4, self.remote_response_port)) {
                        Ok(s) => {
                            info!("Opened response stream (HOST <- REMOTE)");
                            Some(s)
                        }
                        Err(e) => {
                            error!("Failed to open response stream (HOST <- REMOTE): {e}");
                            return Err(SysError::Connection("Failed to open response stream"));
                        }
                    };
                self.image_stream =
                    match TcpStream::connect((detector_ipv4, self.remote_image_port)) {
                        Ok(s) => {
                            info!("Opened image stream (HOST <- REMOTE)");
                            Some(s)
                        }
                        Err(e) => {
                            error!("Failed to open image stream (HOST <- REMOTE): {e}");
                            return Err(SysError::Connection("Failed to open image stream"));
                        }
                    };
                self.log_stream = match TcpStream::connect((detector_ipv4, self.remote_log_port)) {
                    Ok(s) => {
                        info!("Opened log stream (HOST <- REMOTE)");
                        Some(s)
                    }
                    Err(e) => {
                        error!("Failed to open log stream (HOST <- REMOTE): {e}");
                        return Err(SysError::Connection("Failed to open log stream"));
                    }
                };
                self.command_stream =
                    match TcpStream::connect((detector_ipv4, self.remote_command_port)) {
                        Ok(s) => {
                            info!("Opened command stream (HOST -> REMOTE)");
                            Some(s)
                        }
                        Err(e) => {
                            error!("Failed to open command stream (HOST -> REMOTE): {e}");
                            return Err(SysError::Connection("Failed to open command stream"));
                        }
                    };
                self.local_command_port = get_port(&self.command_stream, "local command");
                self.local_image_port = get_port(&self.image_stream, "local image");
                self.local_log_port = get_port(&self.log_stream, "local log");
                self.local_response_port = get_port(&self.response_stream, "local response");
                info!("All streams connected");
            }
        };
        // patch 28: dont kill the root stream maybe
        self.root_stream = Some(init_stream);

        // patch 23: remove gratuitous TCP keepalives & refactor
        // patch 23: use new keepalive implementation
        //let (ttx, trx) = channel::<KeepAliveThreadMsg>();
        //match create_keepalive_thread(self.command_stream.as_ref().unwrap().try_clone().unwrap(), trx, "command".to_string()){
        //    Ok(h) => {
        //        self.tcp_keepalive_sender = Some(ttx);
        //        self.tcp_keepalive_handle = Some(h);
        //    }
        //    Err(e) => {
        //        error!("Failed to start TCP keepalive thread: {e:?}");
        //        return Err(SysError::Connection("Failed to start tcp keepalive thread"));
        //    }
        //};

        // patch 23.4: keepalive on log thread
        match create_log_thread(self.log_stream.as_ref().unwrap().try_clone().unwrap()) {
            Ok(h) => self.log_keepalive_handle = Some(h),
            Err(_) => {
                error!("Failed to start log keepalive thread.");
                return Err(SysError::Connection("Failed to start log keepalive thread"));
            }
        }

        // patch 29: add keepalive to response stream
        // let (stx, srx) = channel::<KeepAliveThreadMsg>();
        // match create_keepalive_thread(self.response_stream.as_ref().unwrap().try_clone().unwrap(), srx, "response".to_string()) {
        // Ok(h) => {
        // self.response_keepalive_sender = Some(stx);
        // self.response_keepalive_handle = Some(h)
        // }
        // Err(_) => {
        // error!("Failedo keepalive response")
        // return Err(SysError::Connection("Failed to keepalive response"))
        // }
        // }

        // patch 28: add keepalive to the root stream
        //let (rtx, rrx) = channel::<KeepAliveThreadMsg>();
        //match create_keepalive_thread(self.root_stream.as_ref().unwrap().try_clone().unwrap(), rrx, "root".to_string()) {
        //    Ok(h) => {
        //        self.root_keepalive_sender = Some(rtx);
        //        self.root_keepalive_handle = Some(h);
        //    }
        //    Err(e) => {
        //        error!("Failed to create root stream keepalive");
        //        return Err(SysError::Connection("Failed to create root keepalive"))
        //    }
        //}

        self.has_connected = true;

        // 26.2 enable logging to async/log port
        match self.command2(("GetLog", None)) {
            Ok((_, t)) => info!("Enabled logging: {t}"),
            Err(e) => warn!("Failed to enable logging: {e:?}"),
        };
        info!("Connection successful");
        // 26.2 remove state print
        return Ok(());
    }
    /// Sends the desired configuration to the detector.
    /// See [`crate::detector_config::DetectorConfig`] for currently implemented fields.
    pub fn configure(&mut self, config: DetectorConfig) -> Result<(), ()> {
        if self.has_connected == false {
            return Err(());
        }
        self.config = config;
        // patch 26 redone to match carestream standard
        // patch 26.2 testing abbrv. config with proper syntax this time...
        let script: Vec<Command> = vec![
            ("Config", None), //enter config mode
            (
                "Set TriggerSource",
                Some(self.config.trigger_source.to_string()),
            ),
            ("Set Preview", Some(self.config.preview.to_string())),
            (
                "Set OffsetAdjustmentCorrectionOn",
                Some(self.config.offset_adjustment_correction.to_string()),
            ),
            (
                "Set GainCorrectionOn",
                Some(self.config.gain_correction.to_string()),
            ),
            (
                "Set DefectCorrectionOn",
                Some(self.config.defect_correction_on.to_string()),
            ),
            (
                "Set GridCorrectionOn",
                Some(self.config.grid_correction.to_string()),
            ),
            (
                "Set NumDarkImgs",
                Some(self.config.num_dark_images.to_string()),
            ),
            (
                "Set CaptureMode",
                Some(self.config.capture_mode.to_string()),
            ),
            (
                "Set IntegrationTime",
                Some(self.config.integration_time.to_string()),
            ),
            ("Set NumPreDarks", Some("0".to_string())),
            (
                "Set BinningMode",
                Some(self.config.binning_mode.to_string()),
            ),
            ("Set HSDataPath", Some(self.config.hs_data_path.to_string())),
            ("Set ImagingSubMode", Some("0".to_string())),
            ("Set NumPreCaptures", Some("0".to_string())),
            ("Set NumPreDarks", Some("0".to_string())),
            (
                "Set NumberImagesInSequence",
                Some(self.config.number_images_in_sequence.to_string()),
            ),
            ("Set AutoRearmPolicy", Some("0".to_string())),
            ("Set IntegrationTime", Some("100".to_string())), // there may need to be a ("Config", None) here (26.2)
        ];

        info!("Executing configuration script...");

        let res = self.script(script);
        for r in res {
            match r {
                Ok(msg) => {
                    info!("Config message: {}", msg.1)
                }
                Err(e) => {
                    warn!("Config error: {:?}", e)
                }
            }
        }
        return Ok(());
    }
    /// Retrieves the configuration from the detector.
    pub fn dump_config(&mut self) -> String {
        let script: Vec<Command> = vec![
            ("Config", None),
            ("Get NumPreCaptures", None),
            ("Get IntegrationTime", None),
            ("Get NumDarkImgs", None),
            ("Get Preview", None),
            ("Get HoldOnPrep", None),
            ("Get MinCharge", None),
            ("Get DCOffset", None),
            ("Get DiagType", None),
            ("Get InactivityTimeout", None),
            ("Get CaptureMode", None),
            ("Get NumberImagesInSequence", None),
            ("Get TopBorder", None),
            ("Get BottomBorder", None),
            ("Get LeftBorder", None),
            ("Get RightBorder", None),
            ("Get OffsetAdjustmentCorrectionOn", None),
            ("Get GainCorrectionOn", None),
            ("Get DefectCorrectionOn", None),
            ("Get GridCorrectionOn", None),
            ("Get TriggerSource", None),
            ("Get BeamPrepTimer", None),
            ("Get BeamIdleTimer", None),
            ("Get BeamExitTimer", None),
            ("Get FastPreviewOn", None),
            ("Get WarningCharge", None),
            ("Get BackupBatteryMinVoltage", None),
            ("Get HSDataPath", None),
            ("Get BinningMode", None),
            ("Get TomoMinPanelWarmup", None),
            ("Get InterTomoRestTime", None),
            ("Get DeleteOnXferComplete", None),
            ("Get ImageReadyDelivery", None),
            ("Get MaxStoredImages", None),
            ("Get Silent", None),
        ];
        let mut _dc = DetectorConfig::default();
        let sres = self.script(script);
        for res in sres {
            match res {
                Ok(r) => {
                    trace!("Got parameter {:?}", r);
                }
                Err(e) => {
                    warn!("Failed to execute command {e:?}");
                    return "".to_string();
                }
            }
        }
        // TODO: Get the configuration from the detector instead of from local state.
        return serde_json::to_string(&self.config).unwrap();
    }
    /// Instructs the detector to capture an image using the previously posted configuration.
    /// May produce unexpected results if the configuration has not been resent to the detector since the last run of the program.
    /// This method takes at least as long as the integration time to complete.
    /// All clients need to ensure that HTTP timeouts are set correctly to prevent errors.
    pub fn capture(&mut self, new_id: Uuid) -> Result<(), SysError> {
        if self.has_connected == false {
            return Err(SysError::NotConnected);
        }
        let mut new_image = Image::new();
        self.currentimage = new_id;
        new_image.id = new_id; //26.5 setting the uuid of the image to the requested one
        info!("Capturing image with ID: {}", new_id.to_string());

        // columns
        if let Ok(cols) = self.command2(("Get NumImgCols", None)) {
            let icols = match back_value::<usize>(cols) {
                Some(columns) => columns,
                None => {
                    warn!("Failed to get image columns");
                    return Err(SysError::Internal("Failed to get image columns"));
                }
            };
            new_image.set_cols(icols);
        } else {
            return Err(SysError::Internal("Failed to get image columns"));
        }

        // rows
        if let Ok(rows) = self.command2(("Get NumImgRows", None)) {
            let irows = match back_value::<usize>(rows) {
                Some(nrow) => nrow,
                None => {
                    warn!("Failed to get image columns");
                    return Err(SysError::Internal("Failed to get image rows"));
                }
            };
            new_image.set_rows(irows);
        } else {
            return Err(SysError::Internal("Failed to get image rows"));
        }

        if let Ok(ready) = self.command2(("ImagerReadyForArm", None)) {
            info!("Imager ready for arm: {}", ready.1);
        } else {
            return Err(SysError::Internal("Failed to ready detector"));
        }

        //26.2 add missing commands & reorder existing
        let _ = self.command2(("ClearFault", Some("0".to_string())));
        let _ = self.command2(("BeginStudy", None));
        let _ = self.command2(("OrphImgs", None));

        //four clearfaults for flavour ig
        let _ = self.command2(("ClearFault", Some("0".to_string())));
        let _ = self.command2(("ClearFault", Some("0".to_string())));
        let _ = self.command2(("ClearFault", Some("0".to_string())));
        let _ = self.command2(("ClearFault", Some("0".to_string())));

        //dont take two images at once
        let _ = self.command2(("AbortImage", None));
        let _ = self.command2(("OrphImgs", None));

        //let _ = self.command2(("Set NumberImagesInSequence", Some(self.config.number_images_in_sequence.to_string())));
        //let _ = self.command2(("Set ImageReadyDelivery", Some(self.config.image_ready_delivery.to_string())));

        // INNER CONFIG
        let icscript = vec![
            ("Set TriggerSource", Some("0".to_string())),
            ("SetPreview", Some("0".to_string())),
            ("OffsetCorrectionAdjustmentOn", Some("0".to_string())),
            ("GainCorrectionOn", Some("0".to_string())),
            ("DefectCorrectionOn", Some("0".to_string())),
            ("GridCorrectionOn", Some("0".to_string())),
            ("NumDarkImgs", Some("0".to_string())),
            ("Set CaptureMode", Some("4".to_string())),
            ("Set IntegrationTime", Some("1100".to_string())),
            ("Set NumPreCaptures", Some("0".to_string())),
            ("Set NumPreDarks", Some("0".to_string())),
            ("Set BinningMode", Some("0".to_string())),
            ("Set HSDataPath", Some("0".to_string())),
            ("Set ImagingSubMode", Some("0".to_string())),
            ("Set NumPreCaptures", Some("0".to_string())),
            ("Set NumPreDarks", Some("0".to_string())),
            ("Set NumDarkImgs", Some("0".to_string())),
            ("Set NumberImagesInSequence", Some("0".to_string())),
            ("Set AutoRearmPolicy", Some("0".to_string())),
            ("Set IntegrationTime", Some("100".to_string())),
        ];
        let _ = self.script(icscript);

        // FAKE ARM
        // generate a random uuid for the fake arm
        let mut rng = rand::rng();
        let fakeuuid = Uuid::from_u128(rng.random::<u128>())
            .as_hyphenated()
            .to_string();
        let _ = self.command2(("Arm", Some(format!("Norm {fakeuuid}"))));
        let _ = self.command2(("Trigger", None));
        sleep(Duration::from_secs(1)); // 1361-1470 refcap001.pcapng
        let _ = self.command2(("OrphImgs", None)); //26.10 fixed 

        // REAL ARM
        let _ = self.command2((
            "Set NumberImagesInSequence",
            Some(self.config.number_images_in_sequence.to_string()),
        ));
        let _ = self.command2((
            "Set IntegrationTime",
            Some(self.config.integration_time.to_string()),
        ));
        let _ = self.command2(("Arm", Some(format!("Norm {}", new_id.to_string()))));
        let _ = self.command2(("Trigger", None));

        // HOLD FOR CAPTURE TIME
        // also allow for some internal data transfer to happen
        sleep(Duration::from_millis(self.config.integration_time + 2000)); // patch 26

        // DATA RECOVERY
        info!("Attempting image retrieval...");
        let _ = self.command2((
            "ExportImageEx",
            Some(format!("{} NormCor", new_id.to_string())),
        ));

        // patch 13: img ret w bufreader
        // patch 26.2: actually implement this correctly, only using read
        // patch 26.7: yeah this needs to be threaded to handle the CaptureCompletes
        // patch 26.8: redo into inline exec, just check the log stream for 0000XXXXCaptureComplete 0 XXX 0 every iter.
        // patch 26.11: use the expected number of bytes to terminate 'rxl
        info!("Reading image data...");
        let mut image_data_buffer: Vec<u8> = Vec::new();
        let mut total_bytes: usize = 0;
        let expected_bytes = 2560 * 3072 * 2; // 15728640
        let t_start = SystemTime::now();
        'rxl: loop {
            let mut iter_buffer = [0_u8; 1460]; // much more reasonable buffer
            match self.image_stream.as_ref().unwrap().read(&mut iter_buffer) {
                Ok(bytes) => {
                    image_data_buffer.extend_from_slice(&iter_buffer[..bytes]);
                    total_bytes += bytes
                }
                Err(e) => {
                    error!("Error reading image data: {e:?}");
                }
            }
            if total_bytes >= expected_bytes {
                info!("Exited image recieve loop due to image size");
                break 'rxl;
            }
            // patch 26.12 add timeout here.
            if SystemTime::now().duration_since(t_start).unwrap().as_secs() > 4 {
                info!("Exited image recieve loop due to timeout");
                break 'rxl;
            }
        }

        info!("Got {} bytes", image_data_buffer.len());
        // 26.2 assuming the first 0x230 bytes are metadata we don't need:
        new_image.push_data(image_data_buffer[(0x230 - 0x2e)..].to_vec());
        self.images.push(new_image);
        info!("Image stream status{:?}", self.image_stream);
        // 26.8 trace!("image buffer: {idb:?}"); // 26.3
        return Ok(());
    }
    /// Returns JSON containing the image pixel data and metadata.
    /// The image data is not stored as a single, 1D array.
    /// The client is expected to use the width and height data to reconstruct the images once they are recieved.
    pub fn get_image(&mut self, id: Uuid) -> Result<String, SysError> {
        //ensure we have the detector identifying information
        if self.detectorinfo.is_none() {
            if let Err(e) = self.get_detectorinfo() {
                return Err(SysError::Internal("Failed to get detector information"));
            }
        }

        let di = self.detectorinfo.as_ref().unwrap();

        info!("Trying UUID: {}", id.to_string());

        for i in &self.images {
            if i.uuid_match(id) {
                info!("Found UUID: {}", id.to_string());
                let imdata = i.data.clone();
                //26.14 ...just use really bad json..
                //26.15 fix the byte conversion
                //26.16 fix the byte conversion again
                let mut buf_16: Vec<u16> = Vec::new();
                for bytes in imdata.chunks(2) {
                    buf_16.push(u16::from_le_bytes([bytes[0], bytes[1]]));
                }

                #[derive(Serialize)]
                struct OutgoingImage {
                    version: String,
                    serial: String,
                    panel: String,
                    integration_time: usize,
                    top_boder: usize,
                    bottom_boder: usize,
                    left_boder: usize,
                    right_boder: usize,
                    image_data: Vec<u16>,
                }
                let oi = OutgoingImage {
                    version: VERSION.to_string(),
                    serial: di.SerialNum.clone(),
                    panel: di.PanelID.clone(),
                    integration_time: self.config.integration_time as usize,
                    top_boder: self.config.top_border as usize,
                    bottom_boder: self.config.bottom_border as usize,
                    left_boder: self.config.left_border as usize,
                    right_boder: self.config.right_border as usize,
                    image_data: buf_16,
                };
                let s = serde_json::to_string(&oi).unwrap();
                return Ok(s);
            }
        }
        return Err(SysError::UnknownID("UUID not found"));
    }
    /// Gets identifiying information from the detector and saves it to the detector's state.
    /// Intended to be used to ensure that a given detector is connected to the correct host device/port as specified in a client configuration.
    pub fn get_detectorinfo(&mut self) -> Result<DetectorInfo, SysError> {
        if self.has_connected == false {
            return Err(SysError::Connection("Detector not connected."));
        }
        let serial = if let Ok(s) = self.command2(("Get SerialNum", None)) {
            s.1
        } else {
            warn!("Failed to get serial number");
            return Err(SysError::Command("Failed to get serial number".to_owned()));
        };
        let panel = if let Ok(p) = self.command2(("Get PanelID", None)) {
            p.1
        } else {
            warn!("Failed to get panel id");
            return Err(SysError::Command("Failed to get panel id".to_owned()));
        };
        let macadd = if let Ok(m) = self.command2(("Get MACAddress", None)) {
            m.1
        } else {
            warn!("Failed to get mac address");
            return Err(SysError::Command("Failed to get mac address".to_owned()));
        };
        let i = DetectorInfo {
            SerialNum: serial,
            PanelID: panel,
            MACAddress: macadd,
        };
        self.detectorinfo = Some(i.clone());
        return Ok(i);
    }
    /// Responsible for sending all commands to the detector with the only exceptions occuring in [`Self::connect()`] when reserving the connection to the detector.
    fn command2(&mut self, cmd: Command) -> Result<Return, SysError> {
        let detector_ipv4: Ipv4Addr = Ipv4Addr::from_str(self.ifconfig.detector_ip.as_str())
            .expect("Invalid detector IPv4 in config.");
        // data prep
        let command = cmd.0;
        let arguments = cmd.1;
        info!("[command2] Sending {command}...");
        if !self.has_connected {
            return Err(SysError::Connection("Detector is not connected"));
        }

        // patch 23: remove tcp kal pausing
        // patch 23.2: reimplement tcp kal pausing
        //match self.tcp_keepalive_sender.as_ref().unwrap().send(KeepAliveThreadMsg::Pause(Duration::from_micros(500))){ //23.6 500 -> 5 23.8 5 -> 500us
        //    Ok(_) => info!("[command2] paused tcp keepalive"),
        //    Err(e) => warn!("[command2] failed to pause tcp keepalive: {e:?}")
        //};
        std::thread::sleep(Duration::from_micros(250)); //23.3 (10ms) 23.8 (250us)

        let mut lc = self.command_stream.as_ref().unwrap().try_clone().unwrap();
        let lr = self.response_stream.as_ref().unwrap().try_clone().unwrap();
        info!("[command2] got copy of the command stream");
        let ctxt: String;
        if let Some(args) = arguments {
            // (patch 10) issue here with args length
            ctxt = format!("{:04x}", self.command_count)
                + &format!("{:04x}", command.len() + 1 + args.len())
                + &command
                + " "
                + &args;
        } else {
            ctxt =
                format!("{:04x}", self.command_count) + &format!("{:04x}", command.len()) + &command
        }

        // sending the command
        let bytes_sent = match lc.write(ctxt.as_bytes()) {
            Ok(b) => b,
            Err(e) => {
                // patch 23: attempt reconnection on peer resets
                match e.kind() {
                    ErrorKind::ConnectionReset => {
                        warn!(
                            "[command2] Connection reset by peer. Attempting to reinitalize the command stream..."
                        );
                        self.command_stream = match TcpStream::connect((
                            detector_ipv4,
                            self.remote_command_port,
                        )) {
                            Ok(s) => {
                                info!("[command2] Reopened command stream (HOST -> REMOTE)");
                                Some(s)
                            }
                            Err(e) => {
                                error!(
                                    "[command2] Failed to reopen command stream (HOST -> REMOTE): {e}"
                                );
                                return Err(SysError::Connection("Failed to open command stream"));
                            }
                        };
                        return Err(SysError::Connection("Reset by peer"));
                    }
                    ErrorKind::BrokenPipe => {
                        error!("[command2] Broken pipe on the command stream");
                        return Err(SysError::Connection("Broken pipe"));
                    }
                    _ => {
                        println!("[command2] Failed to send \"{command}\" to the detector: {e:?}");
                        return Err(SysError::Command(
                            "Failed to send command to detector".to_string(),
                        ));
                    }
                }
            }
        };
        info!("[command2] sent {bytes_sent} bytes");
        if bytes_sent == 0 {
            return Err(SysError::Connection("Failed to send any bytes"));
        }

        // getting a response
        std::thread::sleep(Duration::from_millis(3));
        let mut buf = std::io::BufReader::new(lr);
        let mut charbuffer: Vec<u8> = Vec::new();
        charbuffer.append(&mut buf.fill_buf().unwrap().to_vec());
        let s = String::from_utf8(charbuffer).unwrap();
        info!("[command2] got: {s}");
        //wrap-up
        self.command_count += 1;
        return Ok((0, s));
    }
    /// Executes a series of commands.
    /// Does not halt on errors.
    fn script(&mut self, scr: Vec<Command>) -> Vec<Result<Return, SysError>> {
        let mut out: Vec<Result<(usize, String), SysError>> = Vec::new();
        for command in scr {
            out.push(self.command2(command));
        }
        return out;
    }
}
