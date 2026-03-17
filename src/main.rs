mod commands;
mod detector;
mod detector_config;
mod http_helper;
mod local_tcp;
mod image;
mod ping_helper;
mod tcp_helper;
mod config;
use crate::detector::Detector;
use crate::http_helper::{err, resp_200, resp_200_plain, resp_204, resp_400, resp_404, resp_500};
use log::{info, trace, warn};
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};
use tiny_http::{Method, Server};
use uuid::Uuid;
const VERSION: &str = env!("CARGO_PKG_VERSION");
fn main() -> ! {
    println!("Loading...");
    let t0 = SystemTime::now();
    env_logger::Builder::from_default_env()
        .format(
            move |buf, record| {
                writeln!(
                    buf,
                    "T:{} ID:{:x} [{}/{}] {}",
                    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
                    SystemTime::now().duration_since(t0).unwrap().as_nanos() as u64,
                    record.file().unwrap(),
                    record.line().unwrap(),
                    record.args()
                )
            }
        )
        .init();
    info!("Initialized logging");
    let conf = config::Config::load();
    let mut detector = Detector::new();
    let address = ("0.0.0.0", conf.server_port);
    let http_server = Server::http(address).unwrap();
    info!("Started server");
    info!("Created all pre-http objects");
    'mainloop: loop {
        let request = match http_server.try_recv() {
            Ok(r) => r,
            Err(_) => continue 'mainloop,
        };
        if request.is_some() {
            let mut r = request.unwrap();
            trace!("url: {:?}", r.url());
            let url = http_helper::URL::ingest(r.url());
            let method = r.method();
            info!(
                "Request from {:?}, method {:?}, path {:?}",
                r.remote_addr(),
                method,
                url.base()
            );
            match (url.base().as_str(), method) {
                ("/version", Method::Get) => {
                    info!("Responding on /version::Get");
                    r.respond(resp_200_plain(format!("{}", VERSION).as_bytes()))
                        .unwrap();
                }
                ("/connect", Method::Get) => {
                    info!("Responding on /connect::Get");
                    let res = detector.connect(conf.detector_interface_name.clone());
                    match res {
                        Ok(_) => r.respond(resp_204()).unwrap(),
                        Err(_) => {
                            r.respond(resp_500(
                                err(001, "Failed to bind ports for the detector.").as_bytes(),
                            ))
                            .unwrap();
                        }
                    }
                }
                ("/detectorinfo", Method::Get) => {
                    info!("Responding on /detectorinfo::Get");
                    let info = detector.get_detectorinfo();
                    match info {
                        Ok(i) => r
                            .respond(resp_200(serde_json::to_string(&i).unwrap().as_bytes()))
                            .unwrap(),
                        Err(_) => r
                            .respond(resp_500(
                                err(000, "Failed to communicate with detector").as_bytes(),
                            ))
                            .unwrap(),
                    };
                }
                ("/config", Method::Post) => {
                    info!("Responding on /config::Post");
                    let mut config_text = String::new();
                    let _ = r.as_reader().read_to_string(&mut config_text);
                    let config = match serde_json::from_str::<detector_config::DetectorConfig>(
                        config_text.as_str(),
                    ) {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("failed to post configuration");
                            r.respond(resp_400(
                                err(000, format!("invalid configuration: {}", e)).as_bytes(),
                            ))
                            .unwrap();
                            continue 'mainloop;
                        }
                    };
                    match detector.configure(config) {
                        Ok(_) => {
                            r.respond(resp_204()).unwrap();
                        }
                        Err(_) => r.respond(resp_500("failed".as_bytes())).unwrap(),
                    }
                }
                ("/config", Method::Get) => {
                    info!("Responding on /config::Get");
                    let config_text = detector.dump_config();
                    r.respond(resp_200(config_text.as_bytes())).unwrap();
                }
                //TODO:
                //  - add 500 response
                ("/trigger", Method::Post) => {
                    info!("Responding on /trigger::Post");
                    let mut uuid_text = String::new();
                    let _ = r.as_reader().read_to_string(&mut uuid_text);
                    let uuid = match Uuid::try_parse(&uuid_text) {
                        Ok(u) => u,
                        Err(_) => {
                            r.respond(resp_400(err(000, "invalid UUID").as_bytes()))
                                .unwrap();
                            continue 'mainloop;
                        }
                    };
                    match detector.capture(uuid) {
                        Ok(_) => {
                            r.respond(resp_204()).unwrap();
                        }
                        Err(_) => r.respond(resp_500("data".as_bytes())).unwrap(),
                    }
                }
                ("/retrieve", Method::Post) => {
                    info!("Responding on /retreieve::Post");
                    let mut uuid_text = String::new();
                    let _ = r.as_reader().read_to_string(&mut uuid_text);
                    let uuid = match Uuid::try_parse(&uuid_text) {
                        Ok(u) => u,
                        Err(_) => {
                            r.respond(resp_400(err(000, "invalid UUID").as_bytes()))
                                .unwrap();
                            continue 'mainloop;
                        }
                    };
                    match detector.get_image(uuid) {
                        Ok(image_data) => {
                            r.respond(resp_200(
                                image_data.as_bytes(),
                            ))
                            .unwrap()
                        }
                        Err(detector::SysError::UnknownID(_)) => r
                            .respond(resp_404(err(000, "UUID Not Found").as_bytes()))
                            .unwrap(),
                        Err(detector::SysError::Internal(_)) => {
                            r.respond(resp_500(err(000, "Detector failiure!").as_bytes()))
                                .unwrap();
                            continue 'mainloop;
                        }
                        _ => {
                            r.respond(resp_500(err(000, "Unknown failiure").as_bytes()))
                                .unwrap();
                            continue 'mainloop;
                        }
                    };
                }
                (&_, _) => continue 'mainloop,
            }
        }
    }
}
