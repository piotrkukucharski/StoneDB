use std::net::{IpAddr, Ipv4Addr, ToSocketAddrs};
use tokio::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use bytes::{BytesMut, Buf};
use format_bytes::format_bytes;
use serde_json::Value;
use tokio::io;
use uuid::Uuid;

struct Configuration {
    data_path: String,
    port: u16,
    ip_address: IpAddr
}

fn load_configuration() -> Configuration {
    Configuration {
        data_path: env!("STONEDB_DATA").to_string(),
        port: env!("STONEDB_PORT").parse::<u16>().unwrap(),
        ip_address: env!("STONEDB_ADDRESS").parse::<IpAddr>().unwrap()
    }
}

type StreamId = String;
type EventId = String;
type TenantId = String;

struct Event {
    event_id: EventId,
    stream_id: StreamId,
    sequence: u64,
    recorded_at: u64,
    tenant_id: TenantId,
    event_type: String,
    payload: Value,
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
            let id = Uuid::new_v4();
            println!("Do connection ID {}", id.to_string());

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
                                // Process the command, for example, a simple ping/pong
                                if &buf[..] == b"ping" {
                                    let response_message = format!("pong with {}", id);
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
                                } else {
                                    eprintln!("Unknown command");
                                }
                            },
                            Err(e) => {
                                eprintln!("Failed to read command payload: {}", e);
                                break; // Exit loop on error
                            },
                        }
                    },
                    Err(e) => {
                        eprintln!("Failed to read command length: {}", e);
                        break; // Exit loop on error
                    },
                }
            }
        });
    }
}