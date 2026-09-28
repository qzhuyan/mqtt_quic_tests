use flowsdk::mqtt_client::{ClientSessionState, ClientSessionStore};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io;
use std::path::PathBuf;

// Explicit test checkpoints, not automatic crash-consistent MQTT persistence.
// File-backed use requires one writer per key, as required by ClientSessionStore.
#[derive(Default)]
pub(super) struct SessionStore {
    directory: Option<PathBuf>,
    memory: BTreeMap<String, Vec<u8>>,
}

impl SessionStore {
    pub fn new(directory: Option<PathBuf>) -> Self {
        Self {
            directory,
            ..Default::default()
        }
    }

    fn validate_key(key: &str) -> io::Result<()> {
        if key.is_empty()
            || key.len() > 100
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "session key must contain 1-100 ASCII letters, digits, '.', '_' or '-'",
            ));
        }
        Ok(())
    }

    fn save(&mut self, key: &str, state: &ClientSessionState, create: bool) -> io::Result<()> {
        Self::validate_key(key)?;
        let bytes = serde_json::to_vec(state)?;
        if let Some(directory) = &self.directory {
            fs::create_dir_all(directory)?;
            let path = directory.join(format!("{key}.json"));
            if !create {
                fs::metadata(&path)?;
            }
            let mut temporary = tempfile::NamedTempFile::new_in(directory)?;
            std::io::Write::write_all(temporary.as_file_mut(), &bytes)?;
            temporary.as_file().sync_all()?;
            if create {
                temporary.persist_noclobber(path).map_err(|e| e.error)?;
            } else {
                temporary.persist(path).map_err(|e| e.error)?;
            }
            self.sync_directory()
        } else {
            if self.memory.contains_key(key) == create {
                return Err(io::Error::from(if create {
                    io::ErrorKind::AlreadyExists
                } else {
                    io::ErrorKind::NotFound
                }));
            }
            self.memory.insert(key.into(), bytes);
            Ok(())
        }
    }

    fn sync_directory(&self) -> io::Result<()> {
        #[cfg(unix)]
        if let Some(directory) = &self.directory {
            File::open(directory)?.sync_all()?;
        }
        Ok(())
    }
}

impl ClientSessionStore for SessionStore {
    type Error = io::Error;

    fn create(&mut self, key: &str, state: &ClientSessionState) -> io::Result<()> {
        self.save(key, state, true)
    }

    fn resume(&mut self, key: &str) -> io::Result<Option<ClientSessionState>> {
        Self::validate_key(key)?;
        if let Some(directory) = &self.directory {
            match File::open(directory.join(format!("{key}.json"))) {
                Ok(file) => Ok(Some(serde_json::from_reader(file)?)),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(err) => Err(err),
            }
        } else {
            self.memory
                .get(key)
                .map(|bytes| serde_json::from_slice(bytes).map_err(io::Error::from))
                .transpose()
        }
    }

    fn update(&mut self, key: &str, state: &ClientSessionState) -> io::Result<()> {
        self.save(key, state, false)
    }

    fn delete(&mut self, key: &str) -> io::Result<()> {
        Self::validate_key(key)?;
        if let Some(directory) = &self.directory {
            match fs::remove_file(directory.join(format!("{key}.json"))) {
                Ok(()) => self.sync_directory(),
                Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(err),
            }
        } else {
            self.memory.remove(key);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowsdk::mqtt_client::{engine::MqttEngine, MqttClientOptions};

    #[test]
    fn stores_round_trip_state_and_enforce_create_update_delete_contracts() {
        let dir = tempfile::tempdir().unwrap();
        for directory in [None, Some(dir.path().into())] {
            let mut store = SessionStore::new(directory.clone());
            let state = MqttEngine::new(
                MqttClientOptions::builder()
                    .peer("localhost:1883")
                    .client_id("saved")
                    .build(),
            )
            .snapshot_session()
            .unwrap();
            assert!(store.resume("main").unwrap().is_none());
            assert_eq!(
                store.update("main", &state).unwrap_err().kind(),
                io::ErrorKind::NotFound
            );
            store.create("main", &state).unwrap();
            assert_eq!(
                store.create("main", &state).unwrap_err().kind(),
                io::ErrorKind::AlreadyExists
            );
            store.update("main", &state).unwrap();
            if directory.is_some() {
                store = SessionStore::new(directory);
            }
            let loaded = store.resume("main").unwrap().unwrap();
            assert_eq!(loaded.client_id(), "saved");
            assert_eq!(
                serde_json::to_value(loaded).unwrap(),
                serde_json::to_value(&state).unwrap()
            );
            assert!(store.resume("main").unwrap().is_some());
            store.delete("main").unwrap();
            store.delete("main").unwrap();
            assert!(store.resume("main").unwrap().is_none());
        }
    }

    #[test]
    fn corrupt_files_and_escaping_keys_are_errors_not_missing_records() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = SessionStore::new(Some(dir.path().into()));
        fs::write(dir.path().join("broken.json"), "{").unwrap();
        assert!(store.resume("broken").is_err());
        for key in ["", "../escape", "/absolute", "a\\b"] {
            assert!(store.resume(key).is_err());
            assert!(store.delete(key).is_err());
        }
    }
}
