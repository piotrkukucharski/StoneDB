use std::collections::HashMap;
use std::{fs, io};
use std::fs::OpenOptions;
use std::io::Read;
use std::path::{PathBuf};
use avro_rs::{Codec, Reader, Schema, Writer};
use avro_rs::types::Record;
use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;
use crate::clock::{ChronoUtcSystemClock, Clock};
use base64::{Engine as _, engine::{general_purpose}};
use log::kv::{Key, Source};


type Stream = String;
type EventId = String;
type SpaceId = String;
type Kind = String;

type ConnectionId = Uuid;

struct Event {
    space: SpaceId,
    kind: Kind,
    stream: Stream,
    event_id: EventId,
    sequence: u64,
    recorded_at: u64,
    event_type: String,
    payload: Value,
}

struct Space {
    space_id: SpaceId,
    space_encoded: String,
    // kinds: HashSet<Kind>,
}

#[derive(Clone, Copy)]
struct Connection {
    connection_id: ConnectionId,
    initiation_at: DateTime<Utc>,
}

pub struct EngineConfiguration {
    data_path: PathBuf,
    default_space: SpaceId,
}

pub struct Engine {
    clock: ChronoUtcSystemClock,
    pub connections: HashMap<String, Connection>,
    configuration: EngineConfiguration,
    spaces: HashMap<SpaceId, Space>,
}

impl Engine {
    pub fn new(configuration: EngineConfiguration) -> Result<Engine,String> {
        let mut engine = Engine { clock: ChronoUtcSystemClock, connections: HashMap::new(), configuration, spaces: HashMap::new() };
        let result = engine.init();
        match result {
            Err(why)=>Err(why),
            Ok(())=>Ok(engine)
        }
    }

    fn init(&mut self) -> Result<(), String> {
        //create a data directory if not exist
        if !self.configuration.data_path.exists() {
            let result = fs::create_dir(&self.configuration.data_path);
            if result.is_err() {
                return Err(format!("Can't create directory for path '{}'", self.configuration.data_path.to_str().unwrap()));
            }
        }
        //throw error if data is not directory
        if !self.configuration.data_path.is_dir() {
            return Err(format!("Path '{}' is not a directory", self.configuration.data_path.to_str().unwrap()));
        }

        if self.scan_spaces().is_err(){
            return Err("Can't scan spaces".to_string());
        }

        // create a default space directory
        if !self.spaces.contains_key(&self.configuration.default_space) {
            let result = self.create_space(self.configuration.default_space.clone());
            if result.is_err(){
                return Err(format!("Can't create space '{}'",self.configuration.default_space.clone()));
            }
        }

        Ok(())
    }

    pub fn open_connection(&mut self) -> Connection {
        let connection = Connection { connection_id: ConnectionId::new_v4(), initiation_at: self.clock.now() };
        self.connections.insert(connection.connection_id.to_string(), connection);
        connection
    }
    pub fn close_connection(&mut self, connection_id: ConnectionId) {
        self.connections.remove(&connection_id.to_string());
    }
    fn create_space(&self, space: SpaceId) -> Result<(), std::io::Error> {
        let space_path = self.configuration.data_path.join(general_purpose::STANDARD.encode(space));
        fs::create_dir(space_path)?;
        Ok(())
    }

    fn scan_spaces(&mut self) -> io::Result<()> {
        for entry in fs::read_dir(&self.configuration.data_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    let decoded_name = String::from_utf8(
                        general_purpose::STANDARD.decode(name)
                            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Decode failed"))?
                    )
                        .map_err(|_| io::Error::new(
                            io::ErrorKind::InvalidData, "Convert to String failed")
                        )?;
                    self.spaces.insert(decoded_name.clone(), Space { space_id: decoded_name, space_encoded: name.to_string() });
                }
            }
        }
        Ok(())
    }

    pub fn write_event(&self, event: Event) -> Result<(), String> {
        let file_path = self.configuration.data_path.join(format!("{}/{}/{}.avro", event.space, event.kind, event.stream));
        let raw_schema = r#"
    {
        "type": "record",
        "name": "event",
        "fields": [
            {"name": "event_id", "type": "string"},
            {"name": "sequence", "type": "long"},
            {"name": "recorded_at", "type": "long"},
            {"name": "event_type", "type": "string"},
            {"name": "payload", "type": "string"}
        ]
    }
    "#;
        let schema = Schema::parse_str(raw_schema).map_err(|e| e.to_string())?;

        let mut events = Vec::new();
        if let Ok(mut file) = OpenOptions::new().read(true).open(file_path.clone()) {
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer).map_err(|e| e.to_string())?;
            let reader = Reader::with_schema(&schema, &buffer[..]).map_err(|e| e.to_string())?;

            for value in reader {
                let record = value.map_err(|e| e.to_string())?;
                events.push(record);
            }
        }

        if let Some(last) = events.last() {
            let last_sequence = last.get(Key::from_str("sequence")).unwrap().as_long().unwrap();
            if last_sequence >= event.sequence {
                return Err("Sequence number is not greater than the last record".to_string());
            }
        }



        let mut record = Record::new(&schema).map_err(|e| e.to_string())?;
        record.put("event_id", &event.event_id);
        record.put("sequence", event.sequence);
        record.put("recorded_at", event.recorded_at);
        record.put("event_type", &event.event_type);
        record.put("payload", &event.payload);

        events.push(record);

        // @TODO Change codec to snappy
        let mut writer = Writer::with_codec(&schema, Vec::new(), Codec::Null);
        for event in events {
            writer.append(event).map_err(|e| e.to_string())?;
        }
        writer.flush().map_err(|e| e.to_string())?;
        let encoded_data = writer.into_inner().map_err(|e| e.to_string())?;

        let mut file = OpenOptions::new().write(true).create(true).open(file_path).map_err(|e| e.to_string())?;
        file.write_all(&encoded_data).map_err(|e| e.to_string())?;

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use tempdir::TempDir;
    use crate::engine::{Engine, EngineConfiguration};

    #[test]
    fn test_init_engine_happy_path() {
        let tmp_dir = TempDir::new("temp_dir_for_test_init_engine").unwrap();
        let tmp_dir_path = tmp_dir.into_path();
        let configuration = EngineConfiguration{
            data_path: tmp_dir_path.clone(),
            default_space: "default".to_string(),
        };
        let engine = Engine::new(configuration);
        assert!(engine.is_ok());
        fs::remove_dir_all(tmp_dir_path).unwrap();
    }

    #[test]
    fn test_init_engine_with_file_instead_of_dir() {
        let tmp_dir = TempDir::new("temp_dir_for_test").unwrap();
        let tmp_dir_path = tmp_dir.into_path();
        let tmp_path = tmp_dir_path.join("not_a_directory");
        fs::write(&tmp_path, "This is a file, not a directory.").unwrap();

        let configuration = EngineConfiguration {
            data_path: tmp_path,
            default_space: "default".to_string(),
        };
        let engine = Engine::new(configuration);
        assert!(engine.is_err());
        fs::remove_dir_all(tmp_dir_path).unwrap();
    }

    #[test]
    fn test_init_engine_without_write_permission() {
        let tmp_dir = TempDir::new("temp_dir_for_test").unwrap();
        let tmp_dir_path = tmp_dir.into_path();

        fs::set_permissions(&tmp_dir_path, fs::Permissions::from_mode(0o444)).unwrap();

        let configuration = EngineConfiguration {
            data_path: tmp_dir_path.clone(),
            default_space: "default".to_string(),
        };
        let engine = Engine::new(configuration);
        assert!(engine.is_err());

        fs::set_permissions(&tmp_dir_path, fs::Permissions::from_mode(0o755)).unwrap();
        fs::remove_dir_all(tmp_dir_path).unwrap();
    }
}