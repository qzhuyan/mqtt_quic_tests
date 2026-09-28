use super::connection::{Channel, Connection};
use super::session::SessionStore;
use super::{equal, json_value, Transport};
use crate::{RunConfig, ScenarioReport};
use flowsdk::mqtt_client::commands::{PublishCommand, SubscribeCommand, UnsubscribeCommand};
use flowsdk::mqtt_client::engine::{MqttEvent, OperationKind};
use flowsdk::mqtt_client::opts::MqttClientOptions;
use flowsdk::mqtt_client::{ClientSessionState, ClientSessionStore};
use flowsdk::mqtt_serde::mqttv5::common::properties::Property;
use flowsdk::mqtt_serde::mqttv5::willv5::Will;
use rhai::{Dynamic, Map, INT};
use serde::Deserialize;
use std::collections::{BTreeSet, HashSet};
use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};

pub(super) type Result<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ClientHandle(pub usize);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct StreamHandle {
    pub client: usize,
    pub generation: u64,
    pub channel: Channel,
}

#[derive(Debug, Clone, Copy)]
pub(super) struct Operation(pub usize);

#[derive(Debug, Clone, Copy)]
pub(super) struct Inbox(pub usize);

struct Pending {
    client: usize,
    generation: u64,
    kind: &'static str,
    packet_id: Option<u16>,
    result: Option<Map>,
    success: bool,
    observed: bool,
    event: Option<usize>,
}

struct Cursor {
    client: usize,
    kind: String,
    start: usize,
    consumed: BTreeSet<usize>,
}

pub(super) struct Event {
    client: usize,
    kind: String,
    pub data: Map,
    fault: bool,
    claimed: bool,
}

pub(super) struct Client {
    pub name: String,
    cfg: RunConfig,
    options: Option<MqttClientOptions>,
    saved_session: Option<ClientSessionState>,
    connection: Option<Connection>,
    generation: u64,
    pending_meta: Option<(Option<u16>, Option<u64>)>,
    closing: bool,
    pub report: ScenarioReport,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ClientOptions {
    client_id: Option<String>,
    clean_start: Option<bool>,
    session_expiry_interval: Option<u32>,
    keep_alive: Option<u16>,
    maximum_packet_size: Option<u32>,
    topic_alias_maximum: Option<u16>,
    receive_maximum: Option<u16>,
    auto_ack: Option<bool>,
    auto_keepalive: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PublishOptions {
    topic: String,
    #[serde(default)]
    qos: u8,
    #[serde(default)]
    retain: bool,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WillOptions {
    topic: String,
    #[serde(default)]
    qos: u8,
    #[serde(default)]
    retain: bool,
    delay_interval: Option<u32>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct DisconnectOptions {
    reason_code: u8,
}

pub(super) struct Runtime {
    pub cfg: RunConfig,
    pub transport: Transport,
    pub deadline: Instant,
    pub started: Instant,
    pub clients: Vec<Client>,
    pending: Vec<Pending>,
    cursors: Vec<Cursor>,
    pub events: Vec<Event>,
    pub assertions: usize,
    pub failures: Vec<String>,
    pub session_store: SessionStore,
    event_bytes: usize,
}

impl Runtime {
    pub fn new(cfg: RunConfig, transport: Transport, timeout: Duration) -> Self {
        let started = Instant::now();
        Self {
            cfg,
            transport,
            started,
            deadline: started + timeout,
            clients: vec![],
            pending: vec![],
            cursors: vec![],
            events: vec![],
            assertions: 0,
            failures: vec![],
            session_store: SessionStore::default(),
            event_bytes: 0,
        }
    }

    pub fn check_deadline(&self) -> Result<()> {
        if Instant::now() >= self.deadline {
            Err("scenario deadline exceeded".into())
        } else {
            Ok(())
        }
    }

    pub fn create_client(&mut self, name: &str, mut map: Map) -> Result<ClientHandle> {
        if name.is_empty() || self.clients.iter().any(|c| c.name == name) {
            return Err(format!(
                "client label must be nonempty and unique: {name:?}"
            ));
        }
        if self.clients.len() >= 64 {
            return Err("client limit (64) exceeded".into());
        }
        let will_override = map.remove("will");
        let options: ClientOptions = decode(map)?;
        let mut cfg = self.cfg.clone();
        cfg.client_id = options.client_id.unwrap_or_else(|| {
            if name == "main" {
                cfg.client_id.clone()
            } else {
                format!("{}-{name}", cfg.client_id)
            }
        });
        if let Some(v) = options.clean_start {
            cfg.clean_start = v;
        }
        if let Some(v) = options.keep_alive {
            cfg.keep_alive = v;
        }
        if let Some(v) = options.session_expiry_interval {
            cfg.session_expiry_interval = Some(v);
        }
        if let Some(v) = options.maximum_packet_size {
            cfg.maximum_packet_size = Some(v);
        }
        if let Some(v) = options.topic_alias_maximum {
            cfg.topic_alias_maximum = Some(v);
        }
        if cfg.maximum_packet_size == Some(0) || options.receive_maximum == Some(0) {
            return Err("maximum_packet_size and receive_maximum must be nonzero".into());
        }
        let mut mqtt = MqttClientOptions::builder()
            .mqtt_version(5)
            .peer(format!("{}:{}", cfg.host, cfg.port))
            .client_id(&cfg.client_id)
            .keep_alive(cfg.keep_alive)
            .clean_start(cfg.clean_start)
            .reconnect(false)
            .auto_ack(options.auto_ack.unwrap_or(true))
            .auto_keepalive(options.auto_keepalive.unwrap_or(true));
        if let Some(v) = cfg.session_expiry_interval {
            mqtt = mqtt.session_expiry_interval(v);
        }
        if let Some(v) = cfg.maximum_packet_size {
            mqtt = mqtt.maximum_packet_size(v);
        }
        let mut properties = vec![];
        if let Some(v) = cfg.topic_alias_maximum {
            properties.push(Property::TopicAliasMaximum(v));
        }
        if let Some(v) = options.receive_maximum {
            properties.push(Property::ReceiveMaximum(v));
        }
        if !properties.is_empty() {
            mqtt = mqtt.connect_properties(properties);
        }
        let will = match will_override {
            Some(value) if value.is_unit() => None,
            Some(value) => Some(decode_will(value)?),
            None if cfg.will_enabled => Some(Will::new(
                cfg.will_topic.clone().unwrap_or_else(|| cfg.topic.clone()),
                cfg.will_payload.clone(),
                cfg.will_qos,
                cfg.will_retain,
            )),
            None => None,
        };
        if let Some(will) = will {
            mqtt = mqtt.will(will);
        }
        let report = ScenarioReport::new(&cfg);
        let id = self.clients.len();
        self.clients.push(Client {
            name: name.into(),
            cfg,
            options: Some(mqtt.build()),
            saved_session: None,
            connection: None,
            generation: 1,
            pending_meta: None,
            closing: false,
            report,
        });
        Ok(ClientHandle(id))
    }

    pub fn connect(&mut self, client: ClientHandle) -> Result<Operation> {
        self.check_deadline()?;
        let c = &mut self.clients[client.0];
        if c.connection.is_some() {
            return Err("client already started".into());
        }
        let remote = (c.cfg.host.as_str(), c.cfg.port)
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
            .next()
            .ok_or("host resolved to no addresses")?;
        let options = c.options.take().ok_or("client options already consumed")?;
        c.connection = Some(Connection::start(
            &c.cfg,
            options,
            c.saved_session.take(),
            self.transport,
            remote,
        )?);
        c.report.local_addr = Some(c.connection.as_ref().unwrap().local_addr()?.to_string());
        let operation = self.operation(client, "connected", None);
        self.drain(client)?;
        Ok(operation)
    }

    fn connection(&mut self, client: ClientHandle) -> Result<&mut Connection> {
        self.clients[client.0]
            .connection
            .as_mut()
            .ok_or_else(|| "client not started".into())
    }

    pub fn control_stream(&mut self, client: ClientHandle) -> Result<StreamHandle> {
        if !self.connection(client)?.connected() {
            return Err("client not connected".into());
        }
        Ok(StreamHandle {
            client: client.0,
            generation: self.clients[client.0].generation,
            channel: Channel::Control,
        })
    }

    pub fn open_stream(&mut self, client: ClientHandle, _label: &str) -> Result<StreamHandle> {
        let channel = Channel::Data(self.connection(client)?.open_stream()?);
        Ok(StreamHandle {
            client: client.0,
            generation: self.clients[client.0].generation,
            channel,
        })
    }

    fn channel(
        &self,
        client: ClientHandle,
        stream: Option<StreamHandle>,
    ) -> Result<Option<Channel>> {
        match stream {
            Some(s)
                if s.client != client.0 || s.generation != self.clients[client.0].generation =>
            {
                Err("stream belongs to a different client or connection generation".into())
            }
            Some(s) => Ok(Some(s.channel)),
            None => Ok(None),
        }
    }

    pub fn subscribe(
        &mut self,
        c: ClientHandle,
        s: Option<StreamHandle>,
        topic: &str,
        qos: INT,
    ) -> Result<Operation> {
        let channel = self.channel(c, s)?;
        let command = SubscribeCommand::builder()
            .add_topic(topic, qos_value(qos)?)
            .build()
            .map_err(|e| e.to_string())?;
        let pid = self.connection(c)?.subscribe(channel, command)?;
        let op = self.operation(c, "subscribed", Some(pid));
        self.drain(c)?;
        Ok(op)
    }

    pub fn unsubscribe(
        &mut self,
        c: ClientHandle,
        s: Option<StreamHandle>,
        topic: &str,
    ) -> Result<Operation> {
        let channel = self.channel(c, s)?;
        let command = UnsubscribeCommand::from_topics(vec![topic.to_owned()]);
        let pid = self.connection(c)?.unsubscribe(channel, command)?;
        let op = self.operation(c, "unsubscribed", Some(pid));
        self.drain(c)?;
        Ok(op)
    }

    pub fn publish(
        &mut self,
        c: ClientHandle,
        s: Option<StreamHandle>,
        mut options: Map,
    ) -> Result<Operation> {
        let channel = self.channel(c, s)?;
        let payload = options
            .remove("payload")
            .and_then(|v| v.try_cast::<rhai::Blob>())
            .ok_or("payload must be bytes (use bytes(text) or hex(text))")?;
        let options: PublishOptions = decode(options)?;
        qos_value(options.qos.into())?;
        let command = PublishCommand::builder()
            .topic(options.topic)
            .payload(payload)
            .qos(options.qos)
            .retain(options.retain)
            .build()
            .map_err(|e| e.to_string())?;
        let pid = self.connection(c)?.publish(channel, command)?;
        let op = self.operation(c, "published", pid);
        if options.qos == 0 {
            self.complete_queued(op);
        }
        self.drain(c)?;
        Ok(op)
    }

    pub fn ping(&mut self, c: ClientHandle) -> Result<Operation> {
        if self
            .pending
            .iter()
            .any(|p| p.client == c.0 && p.kind == "ping_response" && p.result.is_none())
        {
            return Err("a ping is already pending for this client".into());
        }
        self.connection(c)?.ping()?;
        let op = self.operation(c, "ping_response", None);
        self.drain(c)?;
        Ok(op)
    }

    pub fn ack(&mut self, c: ClientHandle, kind: &str, message: Map) -> Result<()> {
        let stream = message
            .get("stream")
            .and_then(|v| v.clone().try_cast::<StreamHandle>())
            .ok_or("ACK requires captured stream metadata")?;
        let channel = self.channel(c, Some(stream))?.unwrap();
        let pid = message
            .get("packet_id")
            .and_then(|v| v.as_int().ok())
            .and_then(|v| u16::try_from(v).ok())
            .filter(|v| *v > 0)
            .ok_or("ACK requires a nonzero packet_id")?;
        self.connection(c)?.ack(kind, channel, pid)?;
        self.clients[c.0].report.manual_acks += 1;
        self.drain(c)
    }

    fn operation(
        &mut self,
        client: ClientHandle,
        kind: &'static str,
        packet_id: Option<u16>,
    ) -> Operation {
        let id = self.pending.len();
        self.pending.push(Pending {
            client: client.0,
            generation: self.clients[client.0].generation,
            kind,
            packet_id,
            result: None,
            success: false,
            observed: false,
            event: None,
        });
        Operation(id)
    }

    fn complete_queued(&mut self, operation: Operation) {
        // QoS 0 has no broker acknowledgement; FlowSDK need not emit Published.
        let pending = &mut self.pending[operation.0];
        let mut result = Map::new();
        result.insert("completion".into(), "locally_queued".into());
        result.insert("qos".into(), 0_i64.into());
        result.insert("packet_id".into(), Dynamic::UNIT);
        result.insert("reason_code".into(), Dynamic::UNIT);
        pending.result = Some(result);
        pending.success = true;
    }

    pub fn wait_operation(&mut self, op: Operation, require_success: bool) -> Result<Map> {
        let until = self.wait_deadline(self.cfg.timeout)?;
        loop {
            if let Some(value) = self.pending[op.0].result.clone() {
                let p = &mut self.pending[op.0];
                p.observed = true;
                if let Some(index) = p.event {
                    self.events[index].claimed = true;
                }
                self.assertions += 1;
                if require_success && !p.success {
                    return Err(format!(
                        "{} failed: {}",
                        p.kind,
                        json_value(&Dynamic::from_map(value))
                    ));
                }
                return Ok(value);
            }
            self.check_deadline()?;
            if Instant::now() >= until {
                let p = &self.pending[op.0];
                return Err(format!(
                    "timed out waiting for {} on client {} packet {:?}",
                    p.kind, self.clients[p.client].name, p.packet_id
                ));
            }
            self.pump()?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn watch(&mut self, c: ClientHandle, kind: &str) -> Result<Inbox> {
        fields(kind)?;
        let id = self.cursors.len();
        self.cursors.push(Cursor {
            client: c.0,
            kind: kind.into(),
            start: self.events.len(),
            consumed: BTreeSet::new(),
        });
        Ok(Inbox(id))
    }

    pub fn expect(
        &mut self,
        inbox: Inbox,
        matcher: Map,
        count: usize,
        duration: Duration,
        full_window: bool,
    ) -> Result<Vec<Map>> {
        let cursor = &self.cursors[inbox.0];
        let allowed = fields(&cursor.kind)?;
        for key in matcher.keys() {
            if !allowed.contains(&key.as_str())
                && !["kind", "client", "generation", "sequence"].contains(&key.as_str())
            {
                return Err(format!("unknown {} matcher field: {key}", cursor.kind));
            }
        }
        let until = self.wait_deadline(duration)?;
        let mut results = vec![];
        loop {
            let cursor = &mut self.cursors[inbox.0];
            for index in cursor.start..self.events.len() {
                let event = &mut self.events[index];
                if event.client == cursor.client
                    && event.kind == cursor.kind
                    && !cursor.consumed.contains(&index)
                    && matcher
                        .iter()
                        .all(|(k, v)| event.data.get(k).is_some_and(|actual| equal(actual, v)))
                {
                    cursor.consumed.insert(index);
                    event.claimed = true;
                    results.push(event.data.clone());
                    if results.len() > count {
                        return Err(format!(
                            "expected {count} {} events, observed at least {}; matcher {}",
                            cursor.kind,
                            results.len(),
                            json_value(&Dynamic::from_map(matcher))
                        ));
                    }
                    if !full_window && results.len() == count {
                        self.assertions += 1;
                        return Ok(results);
                    }
                }
            }
            self.check_deadline()?;
            if Instant::now() >= until {
                if results.len() == count {
                    self.assertions += 1;
                    return Ok(results);
                }
                return Err(format!(
                    "expected {count} {} events, got {} before timeout; matcher {}",
                    self.cursors[inbox.0].kind,
                    results.len(),
                    json_value(&Dynamic::from_map(matcher))
                ));
            }
            self.pump()?;
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn wait_deadline(&self, duration: Duration) -> Result<Instant> {
        self.check_deadline()?;
        Instant::now()
            .checked_add(duration)
            .map(|v| v.min(self.deadline))
            .ok_or_else(|| "duration too large".into())
    }

    pub fn poll_for(&mut self, duration: Duration) -> Result<()> {
        let until = self.wait_deadline(duration)?;
        loop {
            self.check_deadline()?;
            self.pump()?;
            if Instant::now() >= until {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn ready(&self, c: ClientHandle) -> Result<()> {
        let client = &self.clients[c.0];
        if !client
            .connection
            .as_ref()
            .is_some_and(Connection::connected)
        {
            return Err("ready requires a connected client".into());
        }
        if let Some(path) = &self.cfg.ready_file {
            std::fs::write(path, &client.cfg.client_id).map_err(|e| e.to_string())?;
        }
        Ok(())
    }

    pub fn snapshot(&self, c: ClientHandle) -> Result<Map> {
        let client = &self.clients[c.0];
        let connection = client.connection.as_ref().ok_or("client not started")?;
        let mut map = Map::new();
        map.insert("connected".into(), connection.connected().into());
        map.insert(
            "local_addr".into(),
            connection.local_addr()?.to_string().into(),
        );
        map.insert("generation".into(), (client.generation as INT).into());
        map.insert(
            "data_stream_count".into(),
            (connection.stream_count() as INT).into(),
        );
        Ok(map)
    }

    pub fn disconnect(&mut self, c: ClientHandle, options: Map) -> Result<()> {
        let options: DisconnectOptions = decode(options)?;
        self.connection(c)?.disconnect(options.reason_code)?;
        self.clients[c.0].closing = true;
        let until = self.wait_deadline(self.cfg.timeout)?;
        loop {
            self.pump()?;
            if self.connection(c)?.flushed() {
                return Ok(());
            }
            self.check_deadline()?;
            if Instant::now() >= until {
                return Err("disconnect did not complete before timeout".into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn save_session(&mut self, c: ClientHandle, key: &str) -> Result<Map> {
        let state = self.connection(c)?.snapshot_session()?;
        match self.session_store.resume(key).map_err(|e| e.to_string())? {
            Some(existing) => {
                if existing.peer() != state.peer()
                    || existing.client_id() != state.client_id()
                    || existing.mqtt_version() != state.mqtt_version()
                {
                    return Err(
                        "session key already belongs to a different broker/client identity".into(),
                    );
                }
                self.session_store.update(key, &state)
            }
            None => self.session_store.create(key, &state),
        }
        .map_err(|e| e.to_string())?;
        session_metadata(&state)
    }

    pub fn restore_session(&mut self, c: ClientHandle, key: &str) -> Result<Map> {
        self.clients[c.0]
            .options
            .as_ref()
            .ok_or("restore_session requires a fresh client before connect")?;
        let state = self
            .session_store
            .resume(key)
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("session checkpoint {key:?} does not exist"))?;
        // Connection::start validates and restores this before opening a socket.
        let metadata = session_metadata(&state)?;
        self.clients[c.0].saved_session = Some(state);
        Ok(metadata)
    }

    pub fn delete_session(&mut self, key: &str) -> Result<()> {
        self.session_store.delete(key).map_err(|e| e.to_string())
    }

    pub fn close_silent(&mut self, c: ClientHandle) -> Result<()> {
        let connection = self.clients[c.0]
            .connection
            .take()
            .ok_or("client not started")?;
        self.clients[c.0].report.data_stream_count = connection.stream_count();
        self.clients[c.0].closing = true;
        // Dropping QUIC sends no CONNECTION_CLOSE; TCP closes without MQTT DISCONNECT.
        drop(connection);
        Ok(())
    }

    pub fn pump(&mut self) -> Result<()> {
        for index in 0..self.clients.len() {
            let c = ClientHandle(index);
            if let Some(connection) = self.clients[index].connection.as_mut() {
                let events = connection
                    .step()
                    .map_err(|e| format!("client {} I/O: {e}", self.clients[index].name))?;
                self.observe(c, events)?;
            }
        }
        Ok(())
    }

    fn drain(&mut self, c: ClientHandle) -> Result<()> {
        let events = self.connection(c)?.take_events();
        self.observe(c, events)
    }

    fn observe(&mut self, c: ClientHandle, events: Vec<MqttEvent>) -> Result<()> {
        for event in events {
            self.clients[c.0].report.observe(&event);
            let mut fault = false;
            let mut completion = None;
            let mut failed_kind = None;
            let (kind, mut data) = match event {
                MqttEvent::Connected(r) => {
                    fault = r.is_failure();
                    completion = Some((None, r.is_success()));
                    ("connected", serialize_map(&r)?)
                }
                MqttEvent::Subscribed(r) => {
                    fault = !r.is_success();
                    completion = Some((Some(r.packet_id), r.is_success()));
                    ("subscribed", serialize_map(&r)?)
                }
                MqttEvent::Unsubscribed(r) => {
                    fault = !r.is_success();
                    completion = Some((Some(r.packet_id), r.is_success()));
                    ("unsubscribed", serialize_map(&r)?)
                }
                MqttEvent::Published(r) => {
                    fault = !r.is_success();
                    completion = Some((r.packet_id, r.is_success()));
                    ("published", serialize_map(&r)?)
                }
                MqttEvent::PingResponse(r) => {
                    fault = !r.success;
                    completion = Some((None, r.success));
                    ("ping_response", serialize_map(&r)?)
                }
                MqttEvent::PublishReceived { packet_id, stream } => {
                    self.clients[c.0].pending_meta = Some((packet_id, stream));
                    continue;
                }
                MqttEvent::MessageReceived(message) => {
                    let meta = self.clients[c.0]
                        .pending_meta
                        .take()
                        .ok_or("PUBLISH missing stream metadata")?;
                    if meta.0 != message.packet_id {
                        return Err("PUBLISH metadata packet ID mismatch".into());
                    }
                    let mut data = serialize_map(&message)?;
                    data.remove("topic_name");
                    data.insert("topic".into(), message.topic_name.into());
                    data.insert("payload".into(), Dynamic::from_blob(message.payload));
                    data.insert("stream".into(), Dynamic::from(self.event_stream(c, meta.1)));
                    ("message", data)
                }
                MqttEvent::PubRelReceived { packet_id, stream } => {
                    let mut data = Map::new();
                    data.insert("packet_id".into(), INT::from(packet_id).into());
                    data.insert("stream".into(), Dynamic::from(self.event_stream(c, stream)));
                    ("pubrel", data)
                }
                MqttEvent::OperationFailed {
                    operation,
                    packet_id,
                    error,
                } => {
                    fault = true;
                    failed_kind = Some(match operation {
                        OperationKind::Connect => "connected",
                        OperationKind::Publish => "published",
                        OperationKind::Subscribe => "subscribed",
                        OperationKind::Unsubscribe => "unsubscribed",
                    });
                    completion = Some((packet_id, false));
                    let data = serialize_map(&serde_json::json!({
                        "operation": format!("{operation:?}"), "packet_id": packet_id, "error": error.to_string()
                    }))?;
                    ("operation_failed", data)
                }
                MqttEvent::Error(err) => {
                    fault = true;
                    (
                        "error",
                        serialize_map(&serde_json::json!({"error": err.to_string()}))?,
                    )
                }
                MqttEvent::Disconnected(reason) => {
                    fault = !self.clients[c.0].closing;
                    (
                        "disconnected",
                        serialize_map(&serde_json::json!({"reason_code": reason}))?,
                    )
                }
                MqttEvent::DisconnectReceived {
                    reason_code,
                    properties,
                } => {
                    fault = !self.clients[c.0].closing;
                    (
                        "disconnect_received",
                        serialize_map(
                            &serde_json::json!({"reason_code": reason_code, "properties": properties}),
                        )?,
                    )
                }
                MqttEvent::TransportClosed {
                    reason,
                    by_peer,
                    error_code,
                } => {
                    fault = !self.clients[c.0].closing;
                    (
                        "transport_closed",
                        serialize_map(
                            &serde_json::json!({"reason": reason, "by_peer": by_peer, "error_code": error_code}),
                        )?,
                    )
                }
                MqttEvent::StreamClosed {
                    stream_id,
                    reason,
                    by_peer,
                } => {
                    let mut data =
                        serialize_map(&serde_json::json!({"reason": reason, "by_peer": by_peer}))?;
                    data.insert(
                        "stream".into(),
                        Dynamic::from(self.event_stream(c, Some(stream_id))),
                    );
                    ("stream_closed", data)
                }
                MqttEvent::StreamReset {
                    stream_id,
                    error_code,
                } => {
                    let mut data = serialize_map(&serde_json::json!({"error_code": error_code}))?;
                    data.insert(
                        "stream".into(),
                        Dynamic::from(self.event_stream(c, Some(stream_id))),
                    );
                    ("stream_reset", data)
                }
                MqttEvent::StreamStopped {
                    stream_id,
                    error_code,
                } => {
                    let mut data = serialize_map(&serde_json::json!({"error_code": error_code}))?;
                    data.insert(
                        "stream".into(),
                        Dynamic::from(self.event_stream(c, Some(stream_id))),
                    );
                    ("stream_stopped", data)
                }
                MqttEvent::ReconnectNeeded => {
                    fault = !self.clients[c.0].closing;
                    ("reconnect_needed", Map::new())
                }
                MqttEvent::ZeroRttStatusChanged { status } => (
                    "zero_rtt_status",
                    serialize_map(&serde_json::json!({"status": status}))?,
                ),
                MqttEvent::AuthReceived(r) => ("auth_received", serialize_map(&r)?),
                MqttEvent::ReconnectScheduled { attempt, delay } => (
                    "reconnect_scheduled",
                    serialize_map(
                        &serde_json::json!({"attempt": attempt, "delay_ms": delay.as_millis() as u64}),
                    )?,
                ),
            };
            data.insert("kind".into(), kind.into());
            data.insert("client".into(), self.clients[c.0].name.clone().into());
            data.insert(
                "generation".into(),
                (self.clients[c.0].generation as INT).into(),
            );
            data.insert("sequence".into(), (self.events.len() as INT).into());
            let event_bytes = json_value(&Dynamic::from_map(data.clone()))
                .to_string()
                .len();
            if self.events.len() >= 16384 || self.event_bytes + event_bytes > 64 * 1024 * 1024 {
                return Err("event journal capacity exceeded".into());
            }
            self.event_bytes += event_bytes;
            // Only reference events accepted into the journal. A timeout is terminal,
            // even if the SDK subsequently emits an acknowledgement for that operation.
            if let Some((packet_id, success)) = completion {
                if let Some(p) = self.pending.iter_mut().find(|p| {
                    p.client == c.0
                        && p.generation == self.clients[c.0].generation
                        && p.kind == failed_kind.unwrap_or(kind)
                        && p.packet_id == packet_id
                        && p.result.is_none()
                }) {
                    p.result = Some(data.clone());
                    p.success = success;
                    p.event = Some(self.events.len());
                }
            }
            self.events.push(Event {
                client: c.0,
                kind: kind.into(),
                data,
                fault,
                claimed: false,
            });
        }
        Ok(())
    }

    fn event_stream(&self, c: ClientHandle, stream: Option<u64>) -> StreamHandle {
        StreamHandle {
            client: c.0,
            generation: self.clients[c.0].generation,
            channel: stream.map(Channel::Data).unwrap_or(Channel::Control),
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        // A bounded final pump sends queued receive ACKs before validating the journal.
        self.poll_for(Duration::from_millis(10))?;
        if self.assertions == 0 {
            return Err("scenario executed no assertions or expectations".into());
        }
        if let Some(failure) = self.failures.first() {
            return Err(failure.clone());
        }
        if let Some(p) = self.pending.iter().find(|p| !p.observed) {
            return Err(format!(
                "unobserved {} operation for client {}",
                p.kind, self.clients[p.client].name
            ));
        }
        if let Some(event) = self.events.iter().find(|e| e.fault && !e.claimed) {
            return Err(format!(
                "unexpected {}: {}",
                event.kind,
                json_value(&Dynamic::from_map(event.data.clone()))
            ));
        }
        Ok(())
    }

    pub fn cleanup(&mut self) {
        // Always drop every transport, including on script exceptions or assertion failures.
        for c in &mut self.clients {
            if let Some(connection) = &c.connection {
                c.report.data_stream_count = connection.stream_count();
            }
            c.connection.take();
        }
    }
}

pub(super) fn qos_value(value: INT) -> Result<u8> {
    u8::try_from(value)
        .ok()
        .filter(|v| *v <= 2)
        .ok_or_else(|| "QoS must be 0, 1, or 2".into())
}

fn decode<T: serde::de::DeserializeOwned>(map: Map) -> Result<T> {
    rhai::serde::from_dynamic(&Dynamic::from_map(map)).map_err(|e| e.to_string())
}

fn decode_will(value: Dynamic) -> Result<Will> {
    let mut map = value
        .try_cast::<Map>()
        .ok_or("will must be an option map or ()")?;
    let payload = map
        .remove("payload")
        .and_then(|value| value.try_cast::<rhai::Blob>())
        .ok_or("will payload must be bytes (use bytes(text) or hex(text))")?;
    let options: WillOptions = decode(map)?;
    qos_value(options.qos.into())?;
    let mut will = Will::new(options.topic, payload, options.qos, options.retain);
    will.properties.will_delay_interval = options.delay_interval;
    Ok(will)
}

fn session_metadata(state: &ClientSessionState) -> Result<Map> {
    serialize_map(&serde_json::json!({
        "version": state.version(), "peer": state.peer(), "client_id": state.client_id(),
        "mqtt_version": state.mqtt_version(), "has_session": state.has_session(),
        "session_expiry_interval": state.session_expiry_interval(),
    }))
}

fn serialize_map<T: serde::Serialize>(value: &T) -> Result<Map> {
    rhai::serde::to_dynamic(value)
        .map_err(|e| e.to_string())?
        .try_cast::<Map>()
        .ok_or_else(|| "expected serialized map".into())
}

fn fields(kind: &str) -> Result<HashSet<&'static str>> {
    let fields: &[&str] = match kind {
        "message" => &[
            "topic",
            "payload",
            "qos",
            "retain",
            "dup",
            "packet_id",
            "stream",
            "properties",
        ],
        "pubrel" => &["packet_id", "stream"],
        "connected" => &["reason_code", "session_present", "properties"],
        "subscribed" | "unsubscribed" => &["packet_id", "reason_codes", "properties"],
        "published" => &["packet_id", "reason_code", "qos", "properties"],
        "ping_response" => &["success"],
        "disconnected" => &["reason_code"],
        "disconnect_received" => &["reason_code", "properties"],
        "transport_closed" => &["reason", "by_peer", "error_code"],
        "stream_closed" => &["stream", "reason", "by_peer"],
        "stream_reset" | "stream_stopped" => &["stream", "error_code"],
        "error" => &["error"],
        "operation_failed" => &["operation", "packet_id", "error"],
        "reconnect_needed" => &[],
        "zero_rtt_status" => &["status"],
        "auth_received" => &["reason_code", "properties"],
        "reconnect_scheduled" => &["attempt", "delay_ms"],
        _ => return Err(format!("unknown event kind: {kind}")),
    };
    Ok(fields.iter().copied().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowsdk::mqtt_client::{ConnectionResult, SubscribeResult};
    use flowsdk::mqtt_serde::mqttv5::publish::MqttPublish;

    fn setup() -> (Runtime, ClientHandle) {
        let cfg = RunConfig {
            timeout: Duration::from_millis(5),
            ..Default::default()
        };
        let mut rt = Runtime::new(cfg, Transport::Quic, Duration::from_secs(1));
        let c = rt.create_client("main", Map::new()).unwrap();
        (rt, c)
    }

    fn message(rt: &mut Runtime, c: ClientHandle, stream: u64, topic: &str) {
        rt.observe(
            c,
            vec![
                MqttEvent::PublishReceived {
                    packet_id: Some(7),
                    stream: Some(stream),
                },
                MqttEvent::MessageReceived(MqttPublish::new(
                    2,
                    topic.into(),
                    Some(7),
                    vec![0, 255],
                    false,
                    false,
                )),
            ],
        )
        .unwrap();
    }

    fn topic(value: &str) -> Map {
        let mut map = Map::new();
        map.insert("topic".into(), value.into());
        map
    }

    fn will_options() -> Map {
        let mut will = topic("script/will");
        will.insert("payload".into(), Dynamic::from_blob(vec![0, 255, 1]));
        will.insert("qos".into(), Dynamic::from_int(1));
        will.insert("retain".into(), true.into());
        will.insert("delay_interval".into(), Dynamic::from_int(300));
        will
    }

    #[test]
    fn per_client_will_overrides_cli_and_preserves_bytes_and_properties() {
        for transport in [Transport::Quic, Transport::Tcp] {
            let cfg = RunConfig {
                will_enabled: true,
                will_topic: Some("cli/will".into()),
                will_payload: b"cli payload".to_vec(),
                will_qos: 2,
                will_retain: true,
                session_expiry_interval: Some(30),
                ..Default::default()
            };
            let mut rt = Runtime::new(cfg, transport, Duration::from_secs(1));
            let inherited = rt.create_client("inherited", Map::new()).unwrap();
            let will = rt.clients[inherited.0]
                .options
                .as_ref()
                .unwrap()
                .will
                .as_ref()
                .unwrap();
            assert_eq!(will.will_topic, "cli/will");
            assert_eq!(will.will_message, b"cli payload");
            assert_eq!(will.will_qos, 2);
            assert!(will.will_retain);
            assert_eq!(will.properties.will_delay_interval, None);

            let mut options = Map::new();
            options.insert("will".into(), Dynamic::UNIT);
            let observer = rt.create_client("observer", options).unwrap();
            assert!(rt.clients[observer.0]
                .options
                .as_ref()
                .unwrap()
                .will
                .is_none());

            let mut options = Map::new();
            options.insert("will".into(), Dynamic::from_map(will_options()));
            options.insert("session_expiry_interval".into(), Dynamic::from_int(1));
            let main = rt.create_client("main", options).unwrap();
            let mqtt = rt.clients[main.0].options.as_ref().unwrap();
            let will = mqtt.will.as_ref().unwrap();
            assert_eq!(mqtt.session_expiry_interval, Some(1));
            assert_eq!(will.will_topic, "script/will");
            assert_eq!(will.will_message, vec![0, 255, 1]);
            assert_eq!(will.will_qos, 1);
            assert!(will.will_retain);
            assert_eq!(will.properties.will_delay_interval, Some(300));
        }
    }

    #[test]
    fn will_optional_fields_have_mqtt_defaults() {
        let mut options = topic("defaults/will");
        options.insert("payload".into(), Dynamic::from_blob(vec![]));
        let will = decode_will(Dynamic::from_map(options)).unwrap();
        assert_eq!(will.will_qos, 0);
        assert!(!will.will_retain);
        assert_eq!(will.properties.will_delay_interval, None);
        assert!(will.will_message.is_empty());
    }

    #[test]
    fn malformed_will_options_fail_before_connecting() {
        for (key, value) in [
            ("payload", Dynamic::from("not bytes")),
            ("qos", Dynamic::from_int(3)),
            ("delay_interval", Dynamic::from_int(-1)),
            ("delay_intervl", Dynamic::from_int(1)),
        ] {
            let mut will = will_options();
            will.insert(key.into(), value);
            assert!(decode_will(Dynamic::from_map(will)).is_err(), "{key}");
        }
        assert!(decode_will(true.into()).is_err());
        let mut will = will_options();
        will.remove("payload");
        assert!(decode_will(Dynamic::from_map(will)).is_err());
    }

    #[test]
    fn disconnect_options_default_to_normal_and_reject_invalid_fields() {
        assert_eq!(
            decode::<DisconnectOptions>(Map::new()).unwrap().reason_code,
            0
        );
        let mut options = Map::new();
        options.insert("reason_code".into(), Dynamic::from_int(0x80));
        assert_eq!(
            decode::<DisconnectOptions>(options).unwrap().reason_code,
            0x80
        );
        for (key, value) in [("reason_code", 256), ("reason_code", -1), ("reason_cod", 0)] {
            let mut options = Map::new();
            options.insert(key.into(), Dynamic::from_int(value));
            assert!(decode::<DisconnectOptions>(options).is_err());
        }
    }

    #[test]
    fn stream_identity_and_binary_payload_survive_normalization() {
        let (mut rt, c) = setup();
        let inbox = rt.watch(c, "message").unwrap();
        message(&mut rt, c, 4, "t");
        message(&mut rt, c, 8, "t");
        for stream in [4, 8] {
            let mut matcher = topic("t");
            matcher.insert(
                "stream".into(),
                Dynamic::from(StreamHandle {
                    client: c.0,
                    generation: 1,
                    channel: Channel::Data(stream),
                }),
            );
            let found = rt.expect(inbox, matcher, 1, Duration::ZERO, false).unwrap();
            assert_eq!(
                found[0]["payload"].clone().cast::<rhai::Blob>(),
                vec![0, 255]
            );
        }
        assert!(rt
            .expect(inbox, topic("t"), 1, Duration::ZERO, false)
            .is_err());
    }

    #[test]
    fn cursors_are_independent_and_keep_unmatched_messages() {
        let (mut rt, c) = setup();
        message(&mut rt, c, 4, "old");
        let one = rt.watch(c, "message").unwrap();
        let two = rt.watch(c, "message").unwrap();
        message(&mut rt, c, 4, "a");
        message(&mut rt, c, 4, "b");
        rt.expect(one, topic("b"), 1, Duration::ZERO, false)
            .unwrap();
        rt.expect(one, topic("a"), 1, Duration::ZERO, false)
            .unwrap();
        rt.expect(two, topic("a"), 1, Duration::ZERO, false)
            .unwrap();
        assert!(rt
            .expect(one, topic("old"), 1, Duration::ZERO, false)
            .is_err());
    }

    #[test]
    fn negative_and_exact_count_assertions_detect_buffered_extras() {
        let (mut rt, c) = setup();
        let inbox = rt.watch(c, "message").unwrap();
        message(&mut rt, c, 4, "t");
        assert!(rt
            .expect(inbox, topic("t"), 0, Duration::ZERO, true)
            .is_err());
        let inbox = rt.watch(c, "message").unwrap();
        message(&mut rt, c, 4, "t");
        message(&mut rt, c, 8, "t");
        assert!(rt
            .expect(inbox, topic("t"), 1, Duration::ZERO, true)
            .is_err());
        let inbox = rt.watch(c, "message").unwrap();
        assert!(rt
            .expect(inbox, topic("t"), 1, Duration::ZERO, true)
            .is_err());
    }

    #[test]
    fn acknowledgements_are_correlated_even_when_batched_out_of_order() {
        let (mut rt, c) = setup();
        let one = rt.operation(c, "subscribed", Some(1));
        let two = rt.operation(c, "subscribed", Some(2));
        rt.observe(
            c,
            vec![2, 1]
                .into_iter()
                .map(|packet_id| {
                    MqttEvent::Subscribed(SubscribeResult {
                        packet_id,
                        reason_codes: vec![1],
                        properties: vec![],
                    })
                })
                .collect(),
        )
        .unwrap();
        assert_eq!(
            rt.wait_operation(one, true).unwrap()["packet_id"]
                .as_int()
                .unwrap(),
            1
        );
        assert_eq!(
            rt.wait_operation(two, true).unwrap()["packet_id"]
                .as_int()
                .unwrap(),
            2
        );
    }

    #[test]
    fn new_connection_cannot_complete_an_old_operation() {
        let (mut rt, c) = setup();
        let old = rt.operation(c, "subscribed", Some(1));
        rt.clients[c.0].generation += 1;
        rt.observe(
            c,
            vec![MqttEvent::Subscribed(SubscribeResult {
                packet_id: 1,
                reason_codes: vec![1],
                properties: vec![],
            })],
        )
        .unwrap();
        assert!(rt.wait_operation(old, true).is_err());
    }

    #[test]
    fn qos_zero_completes_locally_without_inventing_a_broker_ack() {
        let (mut rt, c) = setup();
        let op = rt.operation(c, "published", None);
        rt.complete_queued(op);
        let result = rt.wait_operation(op, true).unwrap();
        assert_eq!(
            result["completion"].clone().cast::<String>(),
            "locally_queued"
        );
        assert!(result["reason_code"].is_unit());
        assert!(rt.events.is_empty());
        rt.finish().unwrap();
    }

    #[test]
    fn operation_failure_cannot_complete_a_different_operation_kind() {
        let (mut rt, c) = setup();
        let connect = rt.operation(c, "connected", None);
        let ping = rt.operation(c, "ping_response", None);
        rt.observe(
            c,
            vec![MqttEvent::OperationFailed {
                operation: OperationKind::Connect,
                packet_id: None,
                error: flowsdk::mqtt_client::error::MqttClientError::OperationTimeout {
                    operation: "Connect".into(),
                    timeout_ms: 5,
                },
            }],
        )
        .unwrap();
        let result = rt.wait_operation(connect, false).unwrap();
        assert_eq!(result["kind"].clone().cast::<String>(), "operation_failed");
        assert!(rt.pending[ping.0].result.is_none());
    }

    #[test]
    fn overflowing_journal_does_not_complete_an_operation_with_a_missing_event() {
        let (mut rt, c) = setup();
        let op = rt.operation(c, "subscribed", Some(1));
        rt.event_bytes = 64 * 1024 * 1024;
        assert!(rt
            .observe(
                c,
                vec![MqttEvent::Subscribed(SubscribeResult {
                    packet_id: 1,
                    reason_codes: vec![1],
                    properties: vec![],
                })]
            )
            .unwrap_err()
            .contains("capacity"));
        assert!(rt.pending[op.0].result.is_none());
        assert!(rt.pending[op.0].event.is_none());
    }

    #[test]
    fn unexpected_errors_and_unobserved_operations_fail_finalization() {
        let (mut rt, c) = setup();
        rt.assertions = 1;
        rt.observe(
            c,
            vec![MqttEvent::TransportClosed {
                reason: "test close".into(),
                by_peer: true,
                error_code: Some(42),
            }],
        )
        .unwrap();
        assert!(rt
            .finish()
            .unwrap_err()
            .contains("unexpected transport_closed"));
        let (mut rt, c) = setup();
        rt.assertions = 1;
        rt.operation(c, "subscribed", Some(1));
        assert!(rt.finish().unwrap_err().contains("unobserved"));
    }

    #[test]
    fn expected_rejection_is_a_result_not_an_automatic_failure() {
        let (mut rt, c) = setup();
        let op = rt.operation(c, "connected", None);
        rt.observe(
            c,
            vec![MqttEvent::Connected(ConnectionResult {
                reason_code: 0x9b,
                session_present: false,
                properties: None,
            })],
        )
        .unwrap();
        assert_eq!(
            rt.wait_operation(op, false).unwrap()["reason_code"]
                .as_int()
                .unwrap(),
            0x9b
        );
        rt.finish().unwrap();
    }

    #[test]
    fn malformed_matchers_fail_before_waiting() {
        let (mut rt, c) = setup();
        let inbox = rt.watch(c, "message").unwrap();
        let mut matcher = Map::new();
        matcher.insert("paylod".into(), "wrong".into());
        assert!(rt
            .expect(inbox, matcher, 1, Duration::ZERO, false)
            .unwrap_err()
            .contains("paylod"));
    }

    #[test]
    fn handles_cannot_cross_client_or_connection_boundaries() {
        let (mut rt, c) = setup();
        let other = rt.create_client("other", Map::new()).unwrap();
        let stream = StreamHandle {
            client: c.0,
            generation: 1,
            channel: Channel::Data(4),
        };
        assert!(rt.channel(other, Some(stream)).is_err());
        rt.clients[c.0].generation += 1;
        assert!(rt.channel(c, Some(stream)).is_err());
    }
}
