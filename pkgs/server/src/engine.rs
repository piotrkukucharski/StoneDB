use std::collections::HashMap;
use std::{fs, io};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::path::{PathBuf};
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};
use uuid::Uuid;
use crate::clock::{ChronoUtcSystemClock, Clock};
use base64::{Engine as _, engine::{general_purpose}};
use log::kv::{Key, Source};
use serde::{Deserialize, Serialize};
use serde_jsonlines::{append_json_lines, json_lines, write_json_lines};
use std::io::Result;


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
    payload: Map<String, Value>,
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

#[derive(Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct StructureOfRecord {
    pub event_id: String,
    pub sequence: usize,
    pub recorded_at: usize,
    pub event_type: String,
    pub payload: String
}


impl Engine {
    pub fn new(configuration: EngineConfiguration) -> Result<Engine> {
        let mut engine = Engine { clock: ChronoUtcSystemClock, connections: HashMap::new(), configuration, spaces: HashMap::new() };
        let result = engine.init();
        match result {
            Err(why)=>Err(why),
            Ok(())=>Ok(engine)
        }
    }

    fn init(&mut self) -> Result<(),> {
        //create a data directory if not exist
        if !self.configuration.data_path.exists() {
            if fs::create_dir(&self.configuration.data_path).is_err(){
                panic!("Can't create path '{}' for data", self.configuration.data_path.to_str().);
            }
        }
        //throw error if data is not directory
        if !self.configuration.data_path.is_dir() {
            panic!("Path '{}' is not a directory", self.configuration.data_path.to_str().unwrap());
        }

        if self.scan_spaces().is_err(){
            panic!("{}","Can't scan spaces".to_string());
        }

        // create a default space directory
        if !self.spaces.contains_key(&self.configuration.default_space) {
            let result = self.create_space(self.configuration.default_space.clone());
            if result.is_err(){
                panic!("Can't create space '{}'",self.configuration.default_space.clone());
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
    fn create_space(&self, space: SpaceId) -> Result<()> {
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

    pub fn write_event(&self, event: Event) -> Result<()> {
        let file_path = self.configuration.data_path.join(format!("{}/{}/{}.jsonl", event.space, event.kind, event.stream));
        let previews_records = json_lines(file_path.clone())?.collect::<Result<Vec<StructureOfRecord>>>()?;

        append_json_lines(file_path, vec![]);
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::fs;
    use std::os::unix::fs::PermissionsExt;
    use serde_json::{Map, Value};
    use tempdir::TempDir;
    use uuid::Uuid;
    use crate::engine::{Engine, EngineConfiguration, Event};

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

    #[test]
    fn test_write_event_happy_path_add_one_event(){
        let tmp_dir = TempDir::new("temp_dir_for_test_write_event").unwrap();
        let tmp_dir_path = tmp_dir.into_path();
        let space = "default".to_string();
        let configuration = EngineConfiguration{
            data_path: tmp_dir_path.clone(),
            default_space: space.clone(),
        };
        let engine = Engine::new(configuration);
        assert!(engine.is_ok());
        let event = Event{
            space: space,
            kind: "order".to_string(),
            stream: Uuid::new_v4().to_string(),
            event_id: Uuid::new_v4().to_string(),
            sequence: 0,
            recorded_at: 0,
            event_type: "init".to_string(),
            payload: Value::Object(Map::new()),
        };
        engine.unwrap().write_event(event).unwrap()
    }
    // #[test]
    // fn test_write_event_happy_path_add_many_events(){
    //     let tmp_dir = TempDir::new("temp_dir_for_test_write_event").unwrap();
    //     let tmp_dir_path = tmp_dir.into_path();
    //     let space = "default".to_string();
    //     let configuration = EngineConfiguration{
    //         data_path: tmp_dir_path.clone(),
    //         default_space: space.clone(),
    //     };
    //     let engine = Engine::new(configuration)?;
    //     assert!(engine.is_ok());
    //     let kind = "order".to_string();
    //     let stream = Uuid::new_v4().to_string();
    //     let event = Event{
    //         space: space.clone(),
    //         kind: kind.clone(),
    //         stream: stream.clone(),
    //         event_id: Uuid::new_v4().to_string(),
    //         sequence: 0,
    //         recorded_at: 0,
    //         event_type: "init".to_string(),
    //         payload: Value::Object(Map::new()),
    //     };
    //     engine.write_event(event).unwrap();
    //     let event = Event{
    //         space: space.clone(),
    //         kind: kind.clone(),
    //         stream: stream.clone(),
    //         event_id: Uuid::new_v4().to_string(),
    //         sequence: 1,
    //         recorded_at: 1,
    //         event_type: "add".to_string(),
    //         payload: Value::Object(Map::new()),
    //     };
    //     engine.write_event(event).unwrap();
    // }
}