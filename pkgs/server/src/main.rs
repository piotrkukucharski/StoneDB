mod interpreter;
mod engine;
mod clock;

use std::collections::HashMap;
use std::net::{IpAddr};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt};
use bytes::{BytesMut};
use serde_json::Value;
use tokio::io;
use uuid::Uuid;
use logos::Logos;
use std::env;

struct Configuration {
    data_path: String,
    port: u16,
    ip_address: IpAddr,
}

fn load_configuration() -> Configuration {
    Configuration {
        data_path: env::var("STONEDB_DATA").unwrap(),
        port: env::var("STONEDB_PORT").unwrap().parse::<u16>().unwrap(),
        ip_address: env::var("STONEDB_ADDRESS").unwrap().parse::<IpAddr>().unwrap(),
    }
}

struct Command {
    method: MethodCommand,
    params: HashMap<&'static str, &'static str>,
}

enum MethodCommand {
    SetSpace,
    Reserve,
    Confirm,
    PushEvent,
    PullEvents,
    DeleteEvent,
    DeleteStream,
    PushProjection,
    PullProjection,
    PullProjections,
}


fn parser(input: &[u8]) -> Result<Command, &'static str> {
    let text = std::str::from_utf8(input).unwrap();
    if !text.ends_with(";") {
        return Result::Err("Input should end with ';'");
    }
    //SET SPACE (space) VALUES (?);
    if text.starts_with("SET SPACE") {
        return Result::Ok(Command { method: MethodCommand::SetSpace, params: HashMap::new() });
    }
    //RESERVE (kind, stream) VALUES (?, ?);
    if text.starts_with("RESERVE") {
        return Result::Ok(Command { method: MethodCommand::Reserve, params: HashMap::new() });
    }
    //CONFIRM;
    if text.starts_with("CONFIRM") {
        return Result::Ok(Command { method: MethodCommand::Confirm, params: HashMap::new() });
    }
    //PUSH EVENT (kind, stream, event_id, sequence, recorded_at, event_type, payload) VALUES (?,?,?,?,?,?,?);
    if text.starts_with("PUSH EVENT") {
        return Result::Ok(Command { method: MethodCommand::PushEvent, params: HashMap::new() });
    }
    //PULL EVENTS (kind, stream) VALUES (?,?);
    if text.starts_with("PULL EVENTS") {
        return Result::Ok(Command { method: MethodCommand::PullEvents, params: HashMap::new() });
    }
    //DELETE EVENT (kind, stream, event_id) VALUES (?,?,?);
    if text.starts_with("DELETE EVENT") {
        return Result::Ok(Command { method: MethodCommand::DeleteEvent, params: HashMap::new() });
    }
    //DELETE STREAM (kind, stream) VALUES (?,?);
    if text.starts_with("DELETE STREAM") {
        return Result::Ok(Command { method: MethodCommand::DeleteStream, params: HashMap::new() });
    }
    //PUSH PROJECTION (kind, stream, name) VALUES (?,?,?);
    if text.starts_with("PUSH PROJECTION") {
        return Result::Ok(Command { method: MethodCommand::PushProjection, params: HashMap::new() });
    }
    //PULL PROJECTION (kind, stream, name) VALUES (?,?,?);
    if text.starts_with("PULL PROJECTION") {
        return Result::Ok(Command { method: MethodCommand::PullProjection, params: HashMap::new() });
    }
    //PULL PROJECTIONS (kind, stream) VALUES (?,?);
    if text.starts_with("PULL PROJECTIONS") {
        return Result::Ok(Command { method: MethodCommand::PullProjections, params: HashMap::new() });
    }
    return Result::Err("Input does not match command format");
}

struct ConnectionState {
    space: String,
    connection_id: Uuid,
}

impl ConnectionState {
    fn new() -> ConnectionState {
        ConnectionState { space: "default".to_string(), connection_id: Uuid::new_v4() }
    }
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let configuration = load_configuration();
    let addr = format!("{}:{}", configuration.ip_address, configuration.port);
    let listener = TcpListener::bind(&addr).await?;
    log::info!("Server listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);
            let runtime = interpreter::runtime::Runtime::new();
            log::info!("Start connection ID {}",runtime.current_state.connection_id);
            loop {
                // Reset or clear the buffer at the beginning of each iteration to avoid data contamination
                buf.clear();

                // Example protocol: Read the length (u32), then read the command
                match socket.read_u32().await {
                    Ok(len) => {
                        // Ensure the buffer has enough space
                        buf.resize(len as usize, 0);

                        match socket.read_exact(&mut buf).await {
                            Ok(_) => {

                            }
                            Err(e) => {
                                eprintln!("Failed to read command payload: {}", e);
                                break; // Exit loop on error
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Failed to read command length: {}", e);
                        break; // Exit loop on error
                    }
                }
            }
        });
    }
}
