use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, Buf};
use format_bytes::format_bytes;
use log::Log;
use serde_json::Value;
use tokio::io;
use uuid::Uuid;
use log::{info, trace, warn};

struct Configuration {
    data_path: String,
    port: u16,
    ip_address: IpAddr,
}

fn load_configuration() -> Configuration {
    Configuration {
        data_path: env!("STONEDB_DATA").to_string(),
        port: env!("STONEDB_PORT").parse::<u16>().unwrap(),
        ip_address: env!("STONEDB_ADDRESS").parse::<IpAddr>().unwrap(),
    }
}

type Stream = String;
type EventId = String;
type Space = String;

struct Event {
    space: Space,
    kind: String,
    stream: Stream,
    event_id: EventId,
    sequence: u64,
    recorded_at: u64,
    event_type: String,
    payload: Value,
}

struct Request {}

struct Response {}

struct Command {
    method: MethodCommand,
    params: HashMap<String, String>,
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

fn parse(input: &[u8]) -> Result<Command, &'static str> {
    let text = std::str::from_utf8(input)?;
    if(!text.ends_with(";")){
        return Result::Err("Input does not match command format");
    }
    //SET SPACE (space) VALUES (?);
    if (text.starts_with("SET SPACE")) {
        return Result::Ok(Command { method: MethodCommand::SetSpace, params: HashMap::new() });
    }
    //RESERVE (kind, stream) VALUES (?, ?);
    if (text.starts_with("RESERVE")) {
        return Result::Ok(Command { method: MethodCommand::Reserve, params: HashMap::new() });
    }
    //CONFIRM;
    if (text.starts_with("CONFIRM")) {
        return Result::Ok(Command { method: MethodCommand::Confirm, params: HashMap::new() });
    }
    //PUSH EVENT (kind, stream, event_id, sequence, recorded_at, event_type, payload) VALUES (?,?,?,?,?,?,?);
    if (text.starts_with("PUSH EVENT")) {
        return Result::Ok(Command { method: MethodCommand::PushEvent, params: HashMap::new() });
    }
    //PULL EVENTS (kind, stream) VALUES (?,?);
    if (text.starts_with("PULL EVENTS")) {
        return Result::Ok(Command { method: MethodCommand::PullEvents, params: HashMap::new() });
    }
    //DELETE EVENT (kind, stream, event_id) VALUES (?,?,?);
    if (text.starts_with("DELETE EVENT")) {
        return Result::Ok(Command { method: MethodCommand::DeleteEvent, params: HashMap::new() });
    }
    //DELETE STREAM (kind, stream) VALUES (?,?);
    if (text.starts_with("DELETE STREAM")) {
        return Result::Ok(Command { method: MethodCommand::DeleteStream, params: HashMap::new() });
    }
    //PUSH PROJECTION (kind, stream, name) VALUES (?,?,?);
    if (text.starts_with("PUSH PROJECTION")) {
        return Result::Ok(Command { method: MethodCommand::PushProjection, params: HashMap::new() });
    }
    //PULL PROJECTION (kind, stream, name) VALUES (?,?,?);
    if (text.starts_with("PULL PROJECTION")) {
        return Result::Ok(Command { method: MethodCommand::PullProjection, params: HashMap::new() });
    }
    //PULL PROJECTIONS (kind, stream) VALUES (?,?);
    if (text.starts_with("PULL PROJECTIONS")) {
        return Result::Ok(Command { method: MethodCommand::PullProjections, params: HashMap::new() });
    }
    return Result::Err("Input does not match command format");
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let configuration = load_configuration();
    let addr = format!("{}:{}", configuration.ip_address, configuration.port);
    let listener = TcpListener::bind(&addr).await?;
    println!("Server listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(1024);
            let connection_id = Uuid::new_v4();
            log::info!("Start connection ID {}",connection_id);
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
                                // let (command, args) = std::str::from_utf8(&buf[..])
                                //     .unwrap()
                                //     .to_string()
                                //     .split_once(" ")
                                //     .unwrap();
                                let command = parse(&buf[..]);
                                match command {
                                    "RESERVE" => {
                                        let response_message = format!("BEGIN with {}", connection_id);
                                        let response = response_message.as_bytes();
                                        let len = response.len() as u32;

                                        // Send length + response
                                        if let Err(e) = socket.write_u32(len).await {
                                            eprintln!("Failed to write response length: {}", e);
                                            return;
                                        }

                                        if let Err(e) = socket.write_all(response).await {
                                            eprintln!("Failed to write response: {}", e);
                                            return;
                                        }
                                    }
                                    "PUSH" => {
                                        let response_message = format!("PUSH with {}", connection_id);
                                        let response = response_message.as_bytes();
                                        let len = response.len() as u32;

                                        // Send length + response
                                        if let Err(e) = socket.write_u32(len).await {
                                            eprintln!("Failed to write response length: {}", e);
                                            return;
                                        }

                                        if let Err(e) = socket.write_all(response).await {
                                            eprintln!("Failed to write response: {}", e);
                                            return;
                                        }
                                    }
                                    "PULL" => {
                                        let response_message = format!("PUSH with {}", connection_id);
                                        let response = response_message.as_bytes();
                                        let len = response.len() as u32;

                                        // Send length + response
                                        if let Err(e) = socket.write_u32(len).await {
                                            eprintln!("Failed to write response length: {}", e);
                                            return;
                                        }

                                        if let Err(e) = socket.write_all(response).await {
                                            eprintln!("Failed to write response: {}", e);
                                            return;
                                        }
                                    }
                                    "CONFIRM" => {
                                        let response_message = format!("PUSH with {}", connection_id);
                                        let response = response_message.as_bytes();
                                        let len = response.len() as u32;

                                        // Send length + response
                                        if let Err(e) = socket.write_u32(len).await {
                                            eprintln!("Failed to write response length: {}", e);
                                            return;
                                        }

                                        if let Err(e) = socket.write_all(response).await {
                                            eprintln!("Failed to write response: {}", e);
                                            return;
                                        }
                                    }
                                    _ => {
                                        eprintln!("Unknown command");
                                    }
                                }
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