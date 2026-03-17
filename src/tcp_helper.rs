use std::io::{self, ErrorKind, Read};
use std::net::TcpStream;
use std::process::exit; 
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::sync::mpsc::Receiver;
use log::{error,info, warn, trace};
pub enum KeepAliveThreadMsg {
    Pause(Duration),
    Halt,
}
pub fn create_keepalive_thread(s: TcpStream,rx: Receiver<KeepAliveThreadMsg>, name: String) -> Result<JoinHandle<()>,io::Error>{
    //! creates the ping thread to keep the detector awake.
    //! returns the messaging channel and thread's join handle.
    let handle = thread::Builder::new().name(format!("tkal-{name}")).spawn(
        move || {
            trace!("[KEEPALIVE ({name})] Starting keepalive");
            let mut stream = s.try_clone().unwrap();
            let incoming = rx;
            let mut buffer = [0_u8; 64];
            'outer: loop {
                match stream.read(&mut buffer) {
                    Ok(bytes) => info!("[KEEPALIVE ({name})] Ok, bytes: {bytes}"),
                    Err(why) => error!("[KEEPALIVE ({name})] Err: {why:?}")
                };
                // rezero the buffer
                buffer = [0_u8; 64];
                if let Ok(msg) = incoming.try_recv() {
                    match msg {
                        KeepAliveThreadMsg::Pause(d) => {
                            thread::sleep(d);
                            trace!("[KEEPALIVE ({name})] sleeping for {d:?}");
                            continue 'outer;
                        }
                        KeepAliveThreadMsg::Halt => {
                            trace!("[KEEPALIVE ({name})] exiting");
                            exit(0);
                        }
                    }
                }
            }
        }
    );
    return handle;
}

// patch 23.4 async log reading
pub fn create_log_thread(s: TcpStream) -> Result<JoinHandle<()>, io::Error> {
    let handle = thread::Builder::new().name("log-thread".to_string()).spawn(
        move || {
            info!("[LOG] created log thread");
            let mut stream = s.try_clone().unwrap();
            let mut read_buffer = [0_u8; 1024]; //23.10
            // 23.11 rem
            let mut log_buffer = String::new();
            loop { // 23.9 how do i get through a day???? i forgot the loop
                match stream.read(& mut read_buffer) {
                    Ok(length) => match length {
                        0 => {
                            info!("[LOG] TCP keepalive ({length} bytes), sent ACK");
                            thread::sleep(Duration::from_millis(0)); // 23.7 (10) 23.8 (0)
                        }
                        1.. => {
                            info!("[LOG] Log message ({length} bytes), appending to messages");
                            let ps = String::from_utf8_lossy(&read_buffer[..]);
                            trace!("[LOG] Log Message: {ps}"); // 23.6
                            log_buffer.push_str(&ps); 
                            //23.11 rem
                        }
                    }
                    Err(e) => {
                        warn!("[LOG] Failed to read from tcp stream: {e:?}")
                    }
                }            
            }
        }
    );
    return handle;
}