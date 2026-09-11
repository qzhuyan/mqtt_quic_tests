use flowsdk::mqtt_client::commands::{PublishCommand, SubscribeCommand, UnsubscribeCommand};
use flowsdk::mqtt_client::engine::{
    MqttEvent, QuicMqttEngine, QuicZeroRttConfig, QuicZeroRttStatus,
};
use flowsdk::mqtt_client::opts::MqttClientOptions;
use flowsdk::mqtt_client::{
    ConnectionResult, MqttClientError, PublishResult, SubscribeResult, UnsubscribeResult,
};
use flowsdk::mqtt_serde::control_packet::MqttPacket;
use flowsdk::mqtt_serde::mqttv5::common::properties::Property;
use flowsdk::mqtt_serde::mqttv5::{connectv5, disconnectv5, subscribev5, unsubscribev5};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::ServerName;
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, UnixTime};
use serde::Serialize;
use std::fmt;
use std::fs;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::path::PathBuf;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    Connect,
    PubSub,
    MultiStream,
    Unsubscribe,
    Malformed,
    WrongStreamConnect,
    ZeroRttPubSub,
    ZeroRttLargePayload,
    ZeroRttStreamContinue,
    ConnResume,
    DataStreamRaceControlStream,
    KeepaliveDataOnlyTimeout,
    KeepaliveDataStreamActive,
    StreamFinish,
    StreamReset,
    StreamStop,
    ManualAckQos1,
    ManualAckQos2,
    SessionResumeQos1,
    SessionResumeQos2,
    SourceBind,
    SourceRebind,
    ParallelPublish,
    MultiStreamPub5x100,
    ParallelNoBlocking,
    CorrelationTopic,
    UnsubscribeViaOther,
    DuplicateSubscribe,
    PacketBoundary,
    PacketTooLarge,
    SubscribeTopicAlias,
    MqttV5Basic,
    MqttV5Session,
    MqttV5PublishProperties,
    MqttV5NoLocal,
    MqttV5InvalidPackets,
    MqttV5BatchSubscribe,
    MqttV5SubscribeMaxQos,
    MqttV5MaxQos,
    MqttV5PublishTooLarge,
    MqttV5SharedQos2Abort,
    MqttV5ConnackUnavailable,
    MqttV5StatsTimer,
    MqttV5ReceiveTooLarge,
    PersistentSessionControls,
    PersistentOfflineQos1,
    PersistentOfflineQos2,
    PersistentManyQos1,
    PersistentManyQos2,
    PersistentCleanStart,
    PersistentExpiry,
    PersistentUnsubscribe,
    PersistentUnsubscribeReplay,
    PersistentMultipleMatches,
    PersistentSysMessages,
    SilentClose,
}

impl Scenario {
    pub const ALL: &'static [Self] = &[
        Self::Connect,
        Self::PubSub,
        Self::MultiStream,
        Self::Unsubscribe,
        Self::Malformed,
        Self::WrongStreamConnect,
        Self::ZeroRttPubSub,
        Self::ZeroRttLargePayload,
        Self::ZeroRttStreamContinue,
        Self::ConnResume,
        Self::DataStreamRaceControlStream,
        Self::KeepaliveDataOnlyTimeout,
        Self::KeepaliveDataStreamActive,
        Self::StreamFinish,
        Self::StreamReset,
        Self::StreamStop,
        Self::ManualAckQos1,
        Self::ManualAckQos2,
        Self::SessionResumeQos1,
        Self::SessionResumeQos2,
        Self::SourceBind,
        Self::SourceRebind,
        Self::ParallelPublish,
        Self::MultiStreamPub5x100,
        Self::ParallelNoBlocking,
        Self::CorrelationTopic,
        Self::UnsubscribeViaOther,
        Self::DuplicateSubscribe,
        Self::PacketBoundary,
        Self::PacketTooLarge,
        Self::SubscribeTopicAlias,
        Self::MqttV5Basic,
        Self::MqttV5Session,
        Self::MqttV5PublishProperties,
        Self::MqttV5NoLocal,
        Self::MqttV5InvalidPackets,
        Self::MqttV5BatchSubscribe,
        Self::MqttV5SubscribeMaxQos,
        Self::MqttV5MaxQos,
        Self::MqttV5PublishTooLarge,
        Self::MqttV5SharedQos2Abort,
        Self::MqttV5ConnackUnavailable,
        Self::MqttV5StatsTimer,
        Self::MqttV5ReceiveTooLarge,
        Self::PersistentSessionControls,
        Self::PersistentOfflineQos1,
        Self::PersistentOfflineQos2,
        Self::PersistentManyQos1,
        Self::PersistentManyQos2,
        Self::PersistentCleanStart,
        Self::PersistentExpiry,
        Self::PersistentUnsubscribe,
        Self::PersistentUnsubscribeReplay,
        Self::PersistentMultipleMatches,
        Self::PersistentSysMessages,
        Self::SilentClose,
    ];

    pub fn parse(name: &str) -> Result<Self, String> {
        match name {
            "connect" => Ok(Self::Connect),
            "pubsub" => Ok(Self::PubSub),
            "multistream" => Ok(Self::MultiStream),
            "unsubscribe" => Ok(Self::Unsubscribe),
            "malformed" => Ok(Self::Malformed),
            "wrong-stream-connect" => Ok(Self::WrongStreamConnect),
            "zero-rtt-pubsub" => Ok(Self::ZeroRttPubSub),
            "zero-rtt-large-payload" => Ok(Self::ZeroRttLargePayload),
            "zero-rtt-stream-continue" => Ok(Self::ZeroRttStreamContinue),
            "conn-resume" => Ok(Self::ConnResume),
            "data-stream-race-control-stream" => Ok(Self::DataStreamRaceControlStream),
            "keepalive-data-only-timeout" => Ok(Self::KeepaliveDataOnlyTimeout),
            "keepalive-data-stream-active" => Ok(Self::KeepaliveDataStreamActive),
            "stream-finish" => Ok(Self::StreamFinish),
            "stream-reset" => Ok(Self::StreamReset),
            "stream-stop" => Ok(Self::StreamStop),
            "manual-ack-qos1" => Ok(Self::ManualAckQos1),
            "manual-ack-qos2" => Ok(Self::ManualAckQos2),
            "session-resume-qos1" => Ok(Self::SessionResumeQos1),
            "session-resume-qos2" => Ok(Self::SessionResumeQos2),
            "source-bind" => Ok(Self::SourceBind),
            "source-rebind" => Ok(Self::SourceRebind),
            "parallel-publish" => Ok(Self::ParallelPublish),
            "multistream-pub-5x100" => Ok(Self::MultiStreamPub5x100),
            "parallel-no-blocking" => Ok(Self::ParallelNoBlocking),
            "correlation-topic" => Ok(Self::CorrelationTopic),
            "unsubscribe-via-other" => Ok(Self::UnsubscribeViaOther),
            "duplicate-subscribe" => Ok(Self::DuplicateSubscribe),
            "packet-boundary" => Ok(Self::PacketBoundary),
            "packet-too-large" => Ok(Self::PacketTooLarge),
            "subscribe-topic-alias" => Ok(Self::SubscribeTopicAlias),
            "mqtt-v5-basic" => Ok(Self::MqttV5Basic),
            "mqtt-v5-session" => Ok(Self::MqttV5Session),
            "mqtt-v5-publish-properties" => Ok(Self::MqttV5PublishProperties),
            "mqtt-v5-no-local" => Ok(Self::MqttV5NoLocal),
            "mqtt-v5-invalid-packets" => Ok(Self::MqttV5InvalidPackets),
            "mqtt-v5-batch-subscribe" => Ok(Self::MqttV5BatchSubscribe),
            "mqtt-v5-subscribe-max-qos" => Ok(Self::MqttV5SubscribeMaxQos),
            "mqtt-v5-max-qos" => Ok(Self::MqttV5MaxQos),
            "mqtt-v5-publish-too-large" => Ok(Self::MqttV5PublishTooLarge),
            "mqtt-v5-shared-qos2-abort" => Ok(Self::MqttV5SharedQos2Abort),
            "mqtt-v5-connack-unavailable" => Ok(Self::MqttV5ConnackUnavailable),
            "mqtt-v5-stats-timer" => Ok(Self::MqttV5StatsTimer),
            "mqtt-v5-receive-too-large" => Ok(Self::MqttV5ReceiveTooLarge),
            "persistent-session-controls" => Ok(Self::PersistentSessionControls),
            "persistent-offline-qos1" => Ok(Self::PersistentOfflineQos1),
            "persistent-offline-qos2" => Ok(Self::PersistentOfflineQos2),
            "persistent-many-qos1" => Ok(Self::PersistentManyQos1),
            "persistent-many-qos2" => Ok(Self::PersistentManyQos2),
            "persistent-clean-start" => Ok(Self::PersistentCleanStart),
            "persistent-expiry" => Ok(Self::PersistentExpiry),
            "persistent-unsubscribe" => Ok(Self::PersistentUnsubscribe),
            "persistent-unsubscribe-replay" => Ok(Self::PersistentUnsubscribeReplay),
            "persistent-multiple-matches" => Ok(Self::PersistentMultipleMatches),
            "persistent-sys-messages" => Ok(Self::PersistentSysMessages),
            "silent-close" => Ok(Self::SilentClose),
            other => Err(format!("unknown scenario: {other}")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::PubSub => "pubsub",
            Self::MultiStream => "multistream",
            Self::Unsubscribe => "unsubscribe",
            Self::Malformed => "malformed",
            Self::WrongStreamConnect => "wrong-stream-connect",
            Self::ZeroRttPubSub => "zero-rtt-pubsub",
            Self::ZeroRttLargePayload => "zero-rtt-large-payload",
            Self::ZeroRttStreamContinue => "zero-rtt-stream-continue",
            Self::ConnResume => "conn-resume",
            Self::DataStreamRaceControlStream => "data-stream-race-control-stream",
            Self::KeepaliveDataOnlyTimeout => "keepalive-data-only-timeout",
            Self::KeepaliveDataStreamActive => "keepalive-data-stream-active",
            Self::StreamFinish => "stream-finish",
            Self::StreamReset => "stream-reset",
            Self::StreamStop => "stream-stop",
            Self::ManualAckQos1 => "manual-ack-qos1",
            Self::ManualAckQos2 => "manual-ack-qos2",
            Self::SessionResumeQos1 => "session-resume-qos1",
            Self::SessionResumeQos2 => "session-resume-qos2",
            Self::SourceBind => "source-bind",
            Self::SourceRebind => "source-rebind",
            Self::ParallelPublish => "parallel-publish",
            Self::MultiStreamPub5x100 => "multistream-pub-5x100",
            Self::ParallelNoBlocking => "parallel-no-blocking",
            Self::CorrelationTopic => "correlation-topic",
            Self::UnsubscribeViaOther => "unsubscribe-via-other",
            Self::DuplicateSubscribe => "duplicate-subscribe",
            Self::PacketBoundary => "packet-boundary",
            Self::PacketTooLarge => "packet-too-large",
            Self::SubscribeTopicAlias => "subscribe-topic-alias",
            Self::MqttV5Basic => "mqtt-v5-basic",
            Self::MqttV5Session => "mqtt-v5-session",
            Self::MqttV5PublishProperties => "mqtt-v5-publish-properties",
            Self::MqttV5NoLocal => "mqtt-v5-no-local",
            Self::MqttV5InvalidPackets => "mqtt-v5-invalid-packets",
            Self::MqttV5BatchSubscribe => "mqtt-v5-batch-subscribe",
            Self::MqttV5SubscribeMaxQos => "mqtt-v5-subscribe-max-qos",
            Self::MqttV5MaxQos => "mqtt-v5-max-qos",
            Self::MqttV5PublishTooLarge => "mqtt-v5-publish-too-large",
            Self::MqttV5SharedQos2Abort => "mqtt-v5-shared-qos2-abort",
            Self::MqttV5ConnackUnavailable => "mqtt-v5-connack-unavailable",
            Self::MqttV5StatsTimer => "mqtt-v5-stats-timer",
            Self::MqttV5ReceiveTooLarge => "mqtt-v5-receive-too-large",
            Self::PersistentSessionControls => "persistent-session-controls",
            Self::PersistentOfflineQos1 => "persistent-offline-qos1",
            Self::PersistentOfflineQos2 => "persistent-offline-qos2",
            Self::PersistentManyQos1 => "persistent-many-qos1",
            Self::PersistentManyQos2 => "persistent-many-qos2",
            Self::PersistentCleanStart => "persistent-clean-start",
            Self::PersistentExpiry => "persistent-expiry",
            Self::PersistentUnsubscribe => "persistent-unsubscribe",
            Self::PersistentUnsubscribeReplay => "persistent-unsubscribe-replay",
            Self::PersistentMultipleMatches => "persistent-multiple-matches",
            Self::PersistentSysMessages => "persistent-sys-messages",
            Self::SilentClose => "silent-close",
        }
    }

    fn needs_manual_ack(self) -> bool {
        matches!(
            self,
            Self::ManualAckQos1
                | Self::ManualAckQos2
                | Self::SessionResumeQos1
                | Self::SessionResumeQos2
                | Self::MqttV5SharedQos2Abort
                | Self::PersistentManyQos1
                | Self::PersistentManyQos2
                | Self::PersistentUnsubscribeReplay
        )
    }

    fn needs_manual_keepalive(self) -> bool {
        matches!(
            self,
            Self::KeepaliveDataOnlyTimeout | Self::KeepaliveDataStreamActive
        )
    }

    fn needs_persistent_session(self) -> bool {
        matches!(self, Self::SessionResumeQos1 | Self::SessionResumeQos2)
    }

    fn receive_maximum(self) -> Option<u16> {
        match self {
            Self::PersistentManyQos1 | Self::PersistentManyQos2 => Some(100),
            Self::PersistentUnsubscribeReplay => Some(10),
            _ => None,
        }
    }

    fn needs_zero_rtt(self) -> bool {
        matches!(
            self,
            Self::ZeroRttPubSub | Self::ZeroRttLargePayload | Self::ZeroRttStreamContinue
        )
    }
}

#[derive(Debug, Clone)]
pub struct RunConfig {
    pub host: String,
    pub port: u16,
    pub server_name: String,
    pub client_id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub scenario: Scenario,
    pub pub_qos: u8,
    pub sub_qos: u8,
    pub timeout: Duration,
    pub keep_alive: u16,
    pub clean_start: bool,
    pub session_expiry_interval: Option<u32>,
    pub maximum_packet_size: Option<u32>,
    pub topic_alias_maximum: Option<u16>,
    pub insecure_skip_verify: bool,
    pub ca_file: Option<PathBuf>,
    pub cert_file: Option<PathBuf>,
    pub key_file: Option<PathBuf>,
    pub malformed_bytes: Vec<u8>,
    pub ready_file: Option<PathBuf>,
    pub hold_after_connect: Duration,
    pub local_bind_addr: Option<SocketAddr>,
    pub rebind_addr: Option<SocketAddr>,
    pub zero_rtt_session_cache_size: usize,
    pub zero_rtt_replay_on_reject: bool,
    pub stream_error_code: u64,
}

impl Default for RunConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 14567,
            server_name: "localhost".to_string(),
            client_id: "mqtt-quic-flow-test".to_string(),
            topic: "test/quic/multistream".to_string(),
            payload: b"hello from flowsdk mqtt_quic_tests".to_vec(),
            scenario: Scenario::MultiStream,
            pub_qos: 1,
            sub_qos: 1,
            timeout: Duration::from_secs(10),
            keep_alive: 30,
            clean_start: true,
            session_expiry_interval: None,
            maximum_packet_size: None,
            topic_alias_maximum: None,
            insecure_skip_verify: false,
            ca_file: None,
            cert_file: None,
            key_file: None,
            malformed_bytes: vec![0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            ready_file: None,
            hold_after_connect: Duration::ZERO,
            local_bind_addr: None,
            rebind_addr: None,
            zero_rtt_session_cache_size: 256,
            zero_rtt_replay_on_reject: true,
            stream_error_code: 42,
        }
    }
}

#[derive(Debug, Default, Serialize)]
pub struct ScenarioReport {
    pub scenario: String,
    pub client_id: String,
    pub connected: bool,
    pub disconnected: bool,
    pub subscribed: usize,
    pub unsubscribed: usize,
    pub published: usize,
    pub messages: usize,
    pub errors: Vec<String>,
    pub data_stream_count: usize,
    pub local_addr: Option<String>,
    pub rebind_addr: Option<String>,
    pub reconnects: usize,
    pub connected_session_present: Vec<bool>,
    pub disconnect_reasons: Vec<Option<u8>>,
    pub pings: usize,
    pub quic_pings: usize,
    pub manual_acks: usize,
    pub pubrel_received: usize,
    pub reconnect_needed: usize,
    pub transport_closed: usize,
    pub transport_close_events: Vec<TransportCloseReport>,
    pub stream_closed: usize,
    pub stream_close_events: Vec<StreamCloseReport>,
    pub stream_reset: usize,
    pub stream_reset_events: Vec<StreamAbortReport>,
    pub stream_stopped: usize,
    pub stream_stop_events: Vec<StreamAbortReport>,
    pub zero_rtt_statuses: Vec<String>,
    pub connection_results: Vec<ConnectionResult>,
    pub subscribe_results: Vec<SubscribeResult>,
    pub unsubscribe_results: Vec<UnsubscribeResult>,
    pub publish_results: Vec<PublishResult>,
}

#[derive(Debug, Serialize)]
pub struct TransportCloseReport {
    pub reason: String,
    pub by_peer: bool,
    pub error_code: Option<u64>,
}

#[derive(Debug, Serialize)]
pub struct StreamCloseReport {
    pub stream_id: u64,
    pub reason: String,
    pub by_peer: bool,
}

#[derive(Debug, Serialize)]
pub struct StreamAbortReport {
    pub stream_id: u64,
    pub error_code: u64,
}

impl ScenarioReport {
    fn new(cfg: &RunConfig) -> Self {
        Self {
            scenario: cfg.scenario.as_str().to_string(),
            client_id: cfg.client_id.clone(),
            ..Self::default()
        }
    }

    fn observe(&mut self, event: &MqttEvent) {
        match event {
            MqttEvent::Connected(result) => {
                self.connection_results.push(result.clone());
                self.connected = result.is_success();
                self.connected_session_present.push(result.session_present);
                if result.is_failure() {
                    self.errors.push(format!(
                        "connect failed: {} ({})",
                        result.reason_description(),
                        result.reason_code
                    ));
                }
            }
            MqttEvent::Disconnected(reason) => {
                self.disconnected = true;
                self.disconnect_reasons.push(*reason);
                if let Some(code) = reason {
                    self.errors
                        .push(format!("disconnected with reason code {code}"));
                }
            }
            MqttEvent::Subscribed(result) => {
                self.subscribe_results.push(result.clone());
                if result.is_success() {
                    self.subscribed += result.successful_subscriptions();
                } else {
                    self.errors
                        .push(format!("subscribe failed: {:?}", result.reason_codes));
                }
            }
            MqttEvent::Unsubscribed(result) => {
                self.unsubscribe_results.push(result.clone());
                if result.is_success() {
                    self.unsubscribed += 1;
                } else {
                    self.errors
                        .push(format!("unsubscribe failed: {:?}", result.reason_codes));
                }
            }
            MqttEvent::Published(result) => {
                self.publish_results.push(result.clone());
                if result.is_success() {
                    self.published += 1;
                } else {
                    self.errors
                        .push(format!("publish failed: {:?}", result.reason_code));
                }
            }
            MqttEvent::MessageReceived(_) => {
                self.messages += 1;
            }
            MqttEvent::PublishReceived { .. } => {}
            MqttEvent::PubRelReceived { .. } => {
                self.pubrel_received += 1;
            }
            MqttEvent::Error(error) => {
                self.errors.push(error.to_string());
            }
            MqttEvent::TransportClosed {
                reason,
                by_peer,
                error_code,
            } => {
                self.transport_closed += 1;
                self.transport_close_events.push(TransportCloseReport {
                    reason: reason.clone(),
                    by_peer: *by_peer,
                    error_code: *error_code,
                });
            }
            MqttEvent::StreamClosed {
                stream_id,
                reason,
                by_peer,
            } => {
                self.stream_closed += 1;
                self.stream_close_events.push(StreamCloseReport {
                    stream_id: *stream_id,
                    reason: reason.clone(),
                    by_peer: *by_peer,
                });
            }
            MqttEvent::StreamReset {
                stream_id,
                error_code,
            } => {
                self.stream_reset += 1;
                self.stream_reset_events.push(StreamAbortReport {
                    stream_id: *stream_id,
                    error_code: *error_code,
                });
            }
            MqttEvent::StreamStopped {
                stream_id,
                error_code,
            } => {
                self.stream_stopped += 1;
                self.stream_stop_events.push(StreamAbortReport {
                    stream_id: *stream_id,
                    error_code: *error_code,
                });
            }
            MqttEvent::ZeroRttStatusChanged { status } => {
                self.zero_rtt_statuses.push(format!("{status:?}"));
            }
            MqttEvent::PingResponse(_) | MqttEvent::ReconnectScheduled { .. } => {}
            MqttEvent::ReconnectNeeded => {
                self.reconnect_needed += 1;
            }
        }
    }
}

pub fn run(cfg: RunConfig) -> Result<ScenarioReport, Box<dyn std::error::Error>> {
    install_crypto_provider();
    let mut client = QuicDriver::connect(cfg)?;
    client.run_scenario()?;
    Ok(client.report)
}

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

struct QuicDriver {
    cfg: RunConfig,
    engine: QuicMqttEngine,
    socket: UdpSocket,
    server_addr: SocketAddr,
    keep_alive: u16,
    report: ScenarioReport,
    pending_publish_meta: Option<PublishMeta>,
    received_publishes: Vec<ReceivedPublish>,
    pubrels: Vec<PubRelSeen>,
}

#[derive(Debug, Clone, Copy)]
struct PublishMeta {
    packet_id: Option<u16>,
    stream: Option<u64>,
}

#[derive(Debug, Clone)]
struct ReceivedPublish {
    topic: String,
    payload: Vec<u8>,
    qos: u8,
    retain: bool,
    dup: bool,
    properties: Vec<Property>,
    packet_id: Option<u16>,
    stream: Option<u64>,
}

#[derive(Debug, Clone)]
struct PubRelSeen {
    packet_id: u16,
    stream: Option<u64>,
}

impl QuicDriver {
    fn connect(cfg: RunConfig) -> Result<Self, Box<dyn std::error::Error>> {
        let server_addr = (cfg.host.as_str(), cfg.port)
            .to_socket_addrs()?
            .next()
            .ok_or_else(|| format!("failed to resolve {}:{}", cfg.host, cfg.port))?;
        let socket = bind_udp_socket(cfg.local_bind_addr, server_addr)?;
        socket.set_nonblocking(true)?;
        let local_addr = socket.local_addr()?.to_string();

        let clean_start = if cfg.scenario.needs_persistent_session() {
            false
        } else {
            cfg.clean_start
        };

        let keep_alive = effective_keep_alive(&cfg);

        let mut opts = MqttClientOptions::builder()
            .peer(format!("{}:{}", cfg.host, cfg.port))
            .client_id(cfg.client_id.clone())
            .keep_alive(keep_alive)
            .clean_start(clean_start)
            .mqtt_version(5)
            .auto_ack(!cfg.scenario.needs_manual_ack())
            .auto_keepalive(!cfg.scenario.needs_manual_keepalive());
        if let Some(interval) = cfg
            .session_expiry_interval
            .or_else(|| cfg.scenario.needs_persistent_session().then_some(60))
        {
            opts = opts.session_expiry_interval(interval);
        }
        if let Some(size) = cfg.maximum_packet_size {
            opts = opts.maximum_packet_size(size);
        }
        let connect_properties = mqtt_connect_properties(&cfg);
        if !connect_properties.is_empty() {
            opts = opts.connect_properties(connect_properties);
        }

        let crypto = build_crypto_config(&cfg)?;
        let mut engine = QuicMqttEngine::new(opts.build())?;
        engine.connect(server_addr, &cfg.server_name, crypto, Instant::now())?;

        let mut report = ScenarioReport::new(&cfg);
        report.local_addr = Some(local_addr);

        Ok(Self {
            report,
            cfg,
            engine,
            socket,
            server_addr,
            keep_alive,
            pending_publish_meta: None,
            received_publishes: Vec::new(),
            pubrels: Vec::new(),
        })
    }

    fn run_scenario(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if self.cfg.scenario == Scenario::MqttV5ConnackUnavailable {
            self.scenario_mqtt_v5_connack_unavailable()?;
            return Ok(());
        }
        self.wait_connected()?;
        if !matches!(
            self.cfg.scenario,
            Scenario::MqttV5StatsTimer | Scenario::MqttV5ReceiveTooLarge
        ) {
            self.write_ready_file()?;
        }
        match self.cfg.scenario {
            Scenario::Connect => {
                self.drive_for(self.cfg.hold_after_connect)?;
                self.engine
                    .disconnect_and_close(0, b"connect scenario complete")?;
                self.drive_for(Duration::from_millis(100))?;
            }
            Scenario::PubSub => self.scenario_pubsub()?,
            Scenario::MultiStream => self.scenario_multistream()?,
            Scenario::Unsubscribe => self.scenario_unsubscribe()?,
            Scenario::Malformed => self.scenario_malformed()?,
            Scenario::WrongStreamConnect => self.scenario_wrong_stream_connect()?,
            Scenario::ZeroRttPubSub => self.scenario_zero_rtt_pubsub(false)?,
            Scenario::ZeroRttLargePayload => self.scenario_zero_rtt_pubsub(true)?,
            Scenario::ZeroRttStreamContinue => self.scenario_zero_rtt_stream_continue()?,
            Scenario::ConnResume => self.scenario_conn_resume()?,
            Scenario::DataStreamRaceControlStream => {
                self.scenario_data_stream_race_control_stream()?
            }
            Scenario::KeepaliveDataOnlyTimeout => self.scenario_keepalive_data_only_timeout()?,
            Scenario::KeepaliveDataStreamActive => self.scenario_keepalive_data_stream_active()?,
            Scenario::StreamFinish => self.scenario_stream_finish()?,
            Scenario::StreamReset => self.scenario_stream_reset()?,
            Scenario::StreamStop => self.scenario_stream_stop()?,
            Scenario::ManualAckQos1 => self.scenario_manual_ack(1)?,
            Scenario::ManualAckQos2 => self.scenario_manual_ack(2)?,
            Scenario::SessionResumeQos1 => self.scenario_session_resume(1)?,
            Scenario::SessionResumeQos2 => self.scenario_session_resume(2)?,
            Scenario::SourceBind => self.scenario_source_bind()?,
            Scenario::SourceRebind => self.scenario_source_rebind()?,
            Scenario::ParallelPublish => self.scenario_parallel_publish()?,
            Scenario::MultiStreamPub5x100 => self.scenario_multistream_pub_5x100()?,
            Scenario::ParallelNoBlocking => self.scenario_parallel_no_blocking()?,
            Scenario::CorrelationTopic => self.scenario_correlation_topic()?,
            Scenario::UnsubscribeViaOther => self.scenario_unsubscribe_via_other()?,
            Scenario::DuplicateSubscribe => self.scenario_duplicate_subscribe()?,
            Scenario::PacketBoundary => self.scenario_packet_boundary()?,
            Scenario::PacketTooLarge => self.scenario_packet_too_large()?,
            Scenario::SubscribeTopicAlias => self.scenario_subscribe_topic_alias()?,
            Scenario::MqttV5Basic => self.scenario_mqtt_v5_basic()?,
            Scenario::MqttV5Session => self.scenario_mqtt_v5_session()?,
            Scenario::MqttV5PublishProperties => self.scenario_mqtt_v5_publish_properties()?,
            Scenario::MqttV5NoLocal => self.scenario_mqtt_v5_no_local()?,
            Scenario::MqttV5InvalidPackets => self.scenario_mqtt_v5_invalid_packets()?,
            Scenario::MqttV5BatchSubscribe => self.scenario_mqtt_v5_batch_subscribe()?,
            Scenario::MqttV5SubscribeMaxQos => self.scenario_mqtt_v5_subscribe_max_qos()?,
            Scenario::MqttV5MaxQos => self.scenario_mqtt_v5_max_qos()?,
            Scenario::MqttV5PublishTooLarge => self.scenario_mqtt_v5_publish_too_large()?,
            Scenario::MqttV5SharedQos2Abort => self.scenario_mqtt_v5_shared_qos2_abort()?,
            Scenario::MqttV5StatsTimer => self.scenario_mqtt_v5_stats_timer()?,
            Scenario::MqttV5ReceiveTooLarge => self.scenario_mqtt_v5_receive_too_large()?,
            Scenario::MqttV5ConnackUnavailable => unreachable!(),
            Scenario::PersistentSessionControls => self.scenario_persistent_session_controls()?,
            Scenario::PersistentOfflineQos1 => self.scenario_persistent_offline(1)?,
            Scenario::PersistentOfflineQos2 => self.scenario_persistent_offline(2)?,
            Scenario::PersistentManyQos1 => self.scenario_persistent_many_qos1()?,
            Scenario::PersistentManyQos2 => self.scenario_persistent_many_qos2()?,
            Scenario::PersistentCleanStart => self.scenario_persistent_clean_start()?,
            Scenario::PersistentExpiry => self.scenario_persistent_expiry()?,
            Scenario::PersistentUnsubscribe => self.scenario_persistent_unsubscribe()?,
            Scenario::PersistentUnsubscribeReplay => {
                self.scenario_persistent_unsubscribe_replay()?
            }
            Scenario::PersistentMultipleMatches => self.scenario_persistent_multiple_matches()?,
            Scenario::PersistentSysMessages => self.scenario_persistent_sys_messages()?,
            Scenario::SilentClose => self.scenario_silent_close()?,
        }
        self.report.data_stream_count = self.engine.data_stream_count();
        Ok(())
    }

    fn write_ready_file(&self) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(path) = &self.cfg.ready_file {
            fs::write(path, &self.cfg.client_id)?;
        }
        Ok(())
    }

    fn scenario_pubsub(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        self.engine
            .subscribe(subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish(publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        self.wait_for_publish_and_message(&topic, &payload)
    }

    fn scenario_multistream(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let sub_stream = self.engine.open_data_stream()?;
        let pub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("stream SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish_on(pub_stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        self.wait_for_publish_and_message(&topic, &payload)
    }

    fn scenario_unsubscribe(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let sub_stream = self.engine.open_data_stream()?;
        let pub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("stream SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish_on(pub_stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        self.wait_for_publish_and_message(&topic, &payload)?;
        self.engine
            .unsubscribe_on(sub_stream, unsubscribe_cmd(&topic)?)?;
        self.drive_until("UNSUBACK", |events| {
            events.iter().any(
                |event| matches!(event, MqttEvent::Unsubscribed(result) if result.is_success()),
            )
        })?;
        Ok(())
    }

    fn scenario_malformed(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self.engine.open_data_stream()?;
        self.engine.send_raw_on(stream, &self.cfg.malformed_bytes)?;
        self.drive_for(Duration::from_millis(250))?;
        Ok(())
    }

    fn scenario_wrong_stream_connect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self.engine.open_data_stream()?;
        let packet = MqttPacket::Connect5(connectv5::MqttConnect::new(
            format!("{}-bad-stream", self.cfg.client_id),
            None,
            None,
            None,
            self.cfg.keep_alive,
            true,
            Vec::new(),
        ));
        self.engine.send_packet_on(stream, packet)?;
        self.drive_for(Duration::from_millis(250))?;
        Ok(())
    }

    fn scenario_zero_rtt_pubsub(
        &mut self,
        large_payload: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.seed_zero_rtt_ticket()?;
        self.start_zero_rtt_reconnect(true)?;
        let topic = self.cfg.topic.clone();
        let payload = if large_payload {
            make_large_payload(&self.cfg.payload)
        } else {
            self.cfg.payload.clone()
        };
        let stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.engine
            .publish_on(stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        self.wait_connected()?;
        self.wait_for_publish_and_message(&topic, &payload)
    }

    fn scenario_zero_rtt_stream_continue(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.seed_zero_rtt_ticket()?;
        self.start_zero_rtt_reconnect(true)?;
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.wait_connected()?;
        self.drive_until("0-RTT stream SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish_on(stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        self.wait_for_publish_and_message(&topic, &payload)
    }

    fn scenario_conn_resume(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.reconnect_and_wait()
    }

    fn scenario_data_stream_race_control_stream(
        &mut self,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let stream = self.engine.open_data_stream()?;
        self.engine
            .publish_on(stream, publish_cmd(&topic, &payload, 0)?)?;
        self.engine
            .subscribe(subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("raced stream publish and control SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.drive_for(Duration::from_millis(100))
    }

    fn scenario_keepalive_data_only_timeout(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self.engine.open_data_stream()?;
        self.engine
            .publish_on(stream, publish_cmd(&self.cfg.topic, &self.cfg.payload, 0)?)?;
        self.drive_until_for(
            "keepalive timeout",
            keepalive_timeout_window(self.keep_alive),
            |events| {
                events.iter().any(|event| {
                    matches!(
                        event,
                        MqttEvent::ReconnectNeeded
                            | MqttEvent::TransportClosed { .. }
                            | MqttEvent::Disconnected(_)
                    )
                })
            },
        )
    }

    fn scenario_keepalive_data_stream_active(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self.engine.open_data_stream()?;
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        self.engine
            .subscribe_on(stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("active keepalive SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        let deadline = Instant::now() + keepalive_timeout_window(self.keep_alive);
        while Instant::now() < deadline {
            let seen_before = self.received_publishes.len();
            self.engine
                .publish_on(stream, publish_cmd(&topic, &payload, 0)?)?;
            self.wait_for_message_after(&topic, &payload, seen_before)?;
        }
        if self.report.reconnect_needed > 0 || self.report.transport_closed > 0 {
            return Err("connection timed out while data stream was active".into());
        }
        Ok(())
    }

    fn scenario_stream_finish(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self.engine.open_data_stream()?;
        self.engine
            .publish_on(stream, publish_cmd(&self.cfg.topic, &self.cfg.payload, 0)?)?;
        self.engine.finish_stream(stream)?;
        self.drive_for(Duration::from_millis(250))?;
        Ok(())
    }

    fn scenario_stream_reset(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self.engine.open_data_stream()?;
        self.engine
            .publish_on(stream, publish_cmd(&self.cfg.topic, &self.cfg.payload, 0)?)?;
        self.engine
            .reset_stream(stream, self.cfg.stream_error_code)?;
        self.drive_for(Duration::from_millis(250))?;
        Ok(())
    }

    fn scenario_stream_stop(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self.engine.open_data_stream()?;
        self.engine
            .stop_stream(stream, self.cfg.stream_error_code)?;
        self.drive_for(Duration::from_millis(250))?;
        Ok(())
    }

    fn scenario_manual_ack(&mut self, qos: u8) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let sub_stream = self.engine.open_data_stream()?;
        let pub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream, subscribe_cmd(&topic, qos)?)?;
        self.drive_until("manual-ack SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish_on(pub_stream, publish_cmd(&topic, &payload, qos)?)?;
        let (packet_id, message_stream) = self.wait_for_message_info(&topic, &payload)?;
        match qos {
            1 => {
                self.engine
                    .puback_on(message_stream.unwrap_or(sub_stream), packet_id)?;
                self.report.manual_acks += 1;
            }
            2 => {
                let ack_stream = message_stream.unwrap_or(sub_stream);
                self.engine.pubrec_on(ack_stream, packet_id)?;
                self.report.manual_acks += 1;
                self.wait_for_pubrel(packet_id, Some(ack_stream))?;
                self.engine.pubcomp_on(ack_stream, packet_id)?;
                self.report.manual_acks += 1;
            }
            _ => {}
        }
        self.drive_for(Duration::from_millis(100))?;
        Ok(())
    }

    fn scenario_session_resume(&mut self, qos: u8) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let sub_stream = self.engine.open_data_stream()?;
        let pub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream, subscribe_cmd(&topic, qos)?)?;
        self.drive_until("session-resume SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish_on(pub_stream, publish_cmd(&topic, &payload, qos)?)?;
        let (packet_id, first_message_stream) = self.wait_for_message_info(&topic, &payload)?;
        if qos == 2 {
            self.engine
                .pubrec_on(first_message_stream.unwrap_or(sub_stream), packet_id)?;
            self.report.manual_acks += 1;
        }
        self.reconnect_and_wait()?;
        let resume_stream = self.engine.open_data_stream()?;
        if qos == 1 {
            self.engine
                .subscribe_on(resume_stream, subscribe_cmd(&topic, qos)?)?;
            self.drive_until("session-resume re-SUBACK", |events| {
                events.iter().any(
                    |event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()),
                )
            })?;
        }
        let (resumed_packet_id, resumed_message_stream) = if qos == 1 {
            self.wait_for_message_info(&topic, &payload)?
        } else {
            (packet_id, first_message_stream)
        };
        match qos {
            1 => {
                self.engine.puback_on(
                    resumed_message_stream.unwrap_or(resume_stream),
                    resumed_packet_id,
                )?;
                self.report.manual_acks += 1;
            }
            2 => {
                let pubrel_stream = self.wait_for_pubrel(packet_id, None)?;
                self.engine
                    .pubcomp_on(pubrel_stream.unwrap_or(resume_stream), packet_id)?;
                self.report.manual_acks += 1;
            }
            _ => {}
        }
        self.drive_for(Duration::from_millis(100))
    }

    fn scenario_source_bind(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.engine.ping()?;
        self.report.pings += 1;
        self.drive_until("PINGRESP after source bind", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::PingResponse(result) if result.success))
        })
    }

    fn scenario_source_rebind(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.rebind_socket()?;
        self.engine.notify_local_address_changed()?;
        self.engine.quic_ping()?;
        self.report.quic_pings += 1;
        self.drive_for(Duration::from_millis(1_000))?;
        self.scenario_pubsub()
    }

    fn scenario_parallel_publish(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload1 = tagged_payload(&self.cfg.payload, b"-parallel-1");
        let payload2 = tagged_payload(&self.cfg.payload, b"-parallel-2");
        let stream1 = self.engine.open_data_stream()?;
        let stream2 = self.engine.open_data_stream()?;
        self.engine
            .subscribe(subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("parallel SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish_on(stream1, publish_cmd(&topic, &payload1, self.cfg.pub_qos)?)?;
        self.engine
            .publish_on(stream2, publish_cmd(&topic, &payload2, self.cfg.pub_qos)?)?;
        self.wait_for_publish_and_message(&topic, &payload1)?;
        self.wait_for_publish_and_message(&topic, &payload2)
    }

    fn scenario_multistream_pub_5x100(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let sub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("5x100 SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;

        let pub_streams = (0..5)
            .map(|_| self.engine.open_data_stream())
            .collect::<Result<Vec<_>, _>>()?;
        let seen_before = self.received_publishes.len();

        for n in 1..=100 {
            let ctrl_payload = tagged_payload(&self.cfg.payload, format!("-ctrl-{n}").as_bytes());
            self.engine.engine_mut().publish(publish_cmd(
                &topic,
                &ctrl_payload,
                self.cfg.pub_qos,
            )?)?;

            for (idx, stream) in pub_streams.iter().enumerate() {
                let payload =
                    tagged_payload(&self.cfg.payload, format!("-s{}-{n}", idx + 1).as_bytes());
                self.engine
                    .publish_on(*stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
            }

            if n % 4 == 0 {
                self.wait_for_topic_message_count_after(&topic, seen_before, n * 6)?;
            } else {
                self.drive_for(Duration::from_millis(1))?;
            }
        }

        self.wait_for_topic_message_count_after(&topic, seen_before, 600)
    }

    fn scenario_parallel_no_blocking(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let blocked_payload = tagged_payload(&self.cfg.payload, b"-blocked");
        let valid_payload = tagged_payload(&self.cfg.payload, b"-unblocked");
        let blocked_stream = self.engine.open_data_stream()?;
        let valid_stream = self.engine.open_data_stream()?;

        self.engine
            .subscribe(subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("parallel-no-blocking SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;

        let full_packet =
            mqtt_v5_publish_bytes(&topic, &blocked_payload, self.cfg.pub_qos, 0x7ffe)?;
        let partial_len = full_packet
            .len()
            .saturating_sub(blocked_payload.len())
            .clamp(1, full_packet.len().saturating_sub(1));
        self.engine
            .send_raw_on(blocked_stream, &full_packet[..partial_len])?;
        self.flush_outgoing_once()?;

        let seen_before = self.received_publishes.len();
        self.engine.publish_on(
            valid_stream,
            publish_cmd(&topic, &valid_payload, self.cfg.pub_qos)?,
        )?;
        self.wait_for_message_after(&topic, &valid_payload, seen_before)?;
        self.drive_for(Duration::from_millis(250))?;

        if self
            .received_publishes
            .iter()
            .skip(seen_before)
            .any(|message| message.topic == topic && message.payload == blocked_payload)
        {
            return Err("incomplete publish payload was delivered".into());
        }
        Ok(())
    }

    fn scenario_correlation_topic(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let response_topic = format!("{topic}/response");
        let correlation_data = b"corr-12345".to_vec();
        let payload = self.cfg.payload.clone();
        let sub_stream = self.engine.open_data_stream()?;
        let pub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("correlation SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        let publish = PublishCommand::builder()
            .topic(&topic)
            .payload(payload.clone())
            .qos(self.cfg.pub_qos)
            .with_response_topic(response_topic.clone())
            .with_correlation_data(correlation_data.clone())
            .build()
            .map_err(|err| MqttClientError::ProtocolViolation {
                message: err.to_string(),
            })?;
        self.engine.publish_on(pub_stream, publish)?;
        self.drive_until("correlated publish", |events| {
            events.iter().any(|event| {
                matches!(
                    event,
                    MqttEvent::MessageReceived(message)
                        if message.topic_name == topic
                            && message.payload == payload
                            && has_property(&message.properties, &Property::ResponseTopic(response_topic.clone()))
                            && has_property(&message.properties, &Property::CorrelationData(correlation_data.clone()))
                )
            })
        })
    }

    fn scenario_unsubscribe_via_other(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let sub_stream = self.engine.open_data_stream()?;
        let unsub_stream = self.engine.open_data_stream()?;
        let pub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("subscribe before cross-stream unsubscribe", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .unsubscribe_on(unsub_stream, unsubscribe_cmd(&topic)?)?;
        self.drive_for(Duration::from_millis(100))?;
        self.engine
            .publish_on(pub_stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        let seen_before = self.received_publishes.len();
        self.wait_for_message_after(&topic, &payload, seen_before)
    }

    fn scenario_duplicate_subscribe(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        let sub_stream1 = self.engine.open_data_stream()?;
        let sub_stream2 = self.engine.open_data_stream()?;
        let pub_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe_on(sub_stream1, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.engine
            .subscribe_on(sub_stream2, subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        let mut subacks = 0usize;
        self.drive_until("duplicate SUBACKs", |events| {
            subacks += events
                .iter()
                .filter(
                    |event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()),
                )
                .count();
            subacks >= 2
        })?;
        let seen_before = self.received_publishes.len();
        self.engine
            .publish_on(pub_stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        self.wait_for_message_after(&topic, &payload, seen_before)?;
        self.drive_for(Duration::from_millis(250))?;
        let delivered = self
            .received_publishes
            .iter()
            .skip(seen_before)
            .filter(|message| message.topic == topic && message.payload == payload)
            .count();
        if delivered != 2 {
            return Err(format!(
                "expected two deliveries after duplicate subscribe, got {delivered}"
            )
            .into());
        }
        Ok(())
    }

    fn scenario_packet_boundary(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload1 = tagged_payload(&self.cfg.payload, b"-boundary-1");
        let payload2 = tagged_payload(&self.cfg.payload, b"-boundary-2");
        let payload3 = make_large_payload(&self.cfg.payload);
        let stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe(subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("packet-boundary SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        self.engine
            .publish_on(stream, publish_cmd(&topic, &payload1, self.cfg.pub_qos)?)?;
        self.engine
            .publish_on(stream, publish_cmd(&topic, &payload2, self.cfg.pub_qos)?)?;
        self.engine
            .publish_on(stream, publish_cmd(&topic, &payload3, self.cfg.pub_qos)?)?;
        self.wait_for_publish_and_message(&topic, &payload1)?;
        self.wait_for_publish_and_message(&topic, &payload2)?;
        self.wait_for_publish_and_message(&topic, &payload3)
    }

    fn scenario_packet_too_large(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let payload = make_large_payload(&self.cfg.payload);
        let stream = self.engine.open_data_stream()?;
        let recovery_stream = self.engine.open_data_stream()?;
        self.engine
            .subscribe(subscribe_cmd(&topic, self.cfg.sub_qos)?)?;
        self.drive_until("packet-too-large SUBACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Subscribed(result) if result.is_success()))
        })?;
        let ok_payload = tagged_payload(&self.cfg.payload, b"-packet-ok");
        self.engine
            .publish_on(stream, publish_cmd(&topic, &ok_payload, self.cfg.pub_qos)?)?;
        self.wait_for_publish_and_message(&topic, &ok_payload)?;
        let seen_before = self.received_publishes.len();
        self.engine
            .publish_on(stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)?;
        self.drive_for(Duration::from_millis(250))?;
        if self
            .received_publishes
            .iter()
            .skip(seen_before)
            .any(|message| message.topic == topic && message.payload == payload)
        {
            return Err("oversized publish was delivered".into());
        }
        let recovery_payload = tagged_payload(&self.cfg.payload, b"-packet-recovery");
        self.engine.publish_on(
            recovery_stream,
            publish_cmd(&topic, &recovery_payload, self.cfg.pub_qos)?,
        )?;
        self.wait_for_publish_and_message(&topic, &recovery_payload)
    }

    fn scenario_subscribe_topic_alias(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic_a = self.cfg.topic.clone();
        let topic_b = format!("{topic_a}/B");
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic(&topic_a, 2)
                .add_topic(&topic_b, 2)
                .build()
                .map_err(|err| MqttClientError::ProtocolViolation {
                    message: err.to_string(),
                })?,
        )?;
        self.drive_until("topic-alias SUBACK", |events| {
            events.iter().any(|event| {
                matches!(
                    event,
                    MqttEvent::Subscribed(result)
                        if result.is_success() && result.successful_subscriptions() == 2
                )
            })
        })?;

        let first = self.received_publishes.len();
        self.engine
            .publish(publish_cmd(&topic_a, b"topic-alias-1", 0)?)?;
        self.wait_for_received_publish_count(first + 1)?;
        let first_message = &self.received_publishes[first];
        if first_message.topic != topic_a
            || !has_property(&first_message.properties, &Property::TopicAlias(1))
        {
            return Err(format!("first aliased delivery was unexpected: {first_message:?}").into());
        }

        self.engine
            .publish(publish_cmd(&topic_a, b"topic-alias-2", 0)?)?;
        self.wait_for_received_publish_count(first + 2)?;
        let second_message = &self.received_publishes[first + 1];
        if !second_message.topic.is_empty()
            || !has_property(&second_message.properties, &Property::TopicAlias(1))
        {
            return Err(format!("reused alias delivery was unexpected: {second_message:?}").into());
        }

        self.engine
            .publish(publish_cmd(&topic_b, b"topic-alias-3", 0)?)?;
        self.wait_for_received_publish_count(first + 3)?;
        let third_message = &self.received_publishes[first + 2];
        if third_message.topic != topic_b
            || third_message
                .properties
                .iter()
                .any(|property| matches!(property, Property::TopicAlias(_)))
        {
            return Err(format!("alias-limit delivery was unexpected: {third_message:?}").into());
        }
        Ok(())
    }

    fn scenario_mqtt_v5_basic(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic, 1)?)?;
        expect_reason_codes(
            "initial QoS 1 SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[1],
        )?;
        self.send_subscribe_on_control(subscribe_cmd(&topic, 2)?)?;
        expect_reason_codes(
            "replacement QoS 2 SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[2],
        )?;

        let seen_before = self.received_publishes.len();
        for index in 0..3 {
            let payload = format!("qos-2-{index}").into_bytes();
            self.engine.publish(publish_cmd(&topic, &payload, 2)?)?;
            self.wait_for_message_after(&topic, &payload, seen_before + index)?;
        }
        self.wait_for_topic_message_count_after(&topic, seen_before, 3)?;
        if self
            .received_publishes
            .iter()
            .skip(seen_before)
            .any(|message| message.qos != 2)
        {
            return Err("replacement subscription did not deliver at QoS 2".into());
        }

        let large_topic = format!("{topic}/large");
        self.send_subscribe_on_control(subscribe_cmd(&large_topic, 1)?)?;
        expect_reason_codes(
            "large-packet SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[1],
        )?;
        for size in [120usize, 1_200, 12_000, 120_000] {
            let payload = vec![b'P'; size];
            let before = self.received_publishes.len();
            self.engine
                .publish(publish_cmd(&large_topic, &payload, 1)?)?;
            self.wait_for_message_after(&large_topic, &payload, before)?;
        }

        self.send_unsubscribe_on_control(unsubscribe_cmd(&topic)?)?;
        expect_reason_codes(
            "existing UNSUBACK",
            &self.wait_for_unsuback()?.reason_codes,
            &[0],
        )?;
        self.send_unsubscribe_on_control(unsubscribe_cmd(&format!("{topic}/missing"))?)?;
        expect_reason_codes(
            "missing UNSUBACK",
            &self.wait_for_unsuback()?.reason_codes,
            &[0x11],
        )?;

        let topic2 = format!("{topic}/second");
        self.send_subscribe_on_control(
            SubscribeCommand::builder()
                .add_topic(&topic, 2)
                .add_topic(&topic2, 2)
                .build()?,
        )?;
        expect_reason_codes(
            "batch QoS 2 SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[2, 2],
        )?;
        self.send_unsubscribe_on_control(UnsubscribeCommand::new(
            None,
            vec![topic.clone(), topic2, format!("{topic}/missing")],
            Vec::new(),
        ))?;
        expect_reason_codes(
            "batch UNSUBACK",
            &self.wait_for_unsuback()?.reason_codes,
            &[0, 0, 0x11],
        )?;

        let action_topic = format!("{topic}/actions");
        self.send_subscribe_on_control(
            SubscribeCommand::builder()
                .add_topic(&action_topic, 2)
                .with_subscription_id(2333)
                .build()?,
        )?;
        expect_reason_codes(
            "subscription-action QoS 2 SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[2],
        )?;
        self.send_subscribe_on_control(
            SubscribeCommand::builder()
                .add_topic(&action_topic, 1)
                .with_subscription_id(2333)
                .build()?,
        )?;
        expect_reason_codes(
            "subscription-action QoS 1 SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[1],
        )?;
        let action_payload = b"subscribe-actions";
        let before = self.received_publishes.len();
        self.engine
            .publish(publish_cmd(&action_topic, action_payload, 2)?)?;
        self.wait_for_message_after(&action_topic, action_payload, before)?;
        let action_message = &self.received_publishes[before];
        if action_message.qos != 1
            || !has_property(
                &action_message.properties,
                &Property::SubscriptionIdentifier(2333),
            )
        {
            return Err(
                format!("subscription replacement was unexpected: {action_message:?}").into(),
            );
        }

        self.engine.ping()?;
        self.drive_until("PINGRESP", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::PingResponse(result) if result.success))
        })
    }

    fn scenario_mqtt_v5_session(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let initial = self
            .report
            .connection_results
            .last()
            .ok_or("missing initial CONNACK")?;
        if initial.session_present {
            return Err("clean-start connection unexpectedly resumed a session".into());
        }

        let takeover_id = self.cfg.client_id.clone();
        let mut takeover = self.connect_peer(&takeover_id, false, None, Scenario::Connect)?;
        if !takeover.last_connection_result()?.session_present {
            return Err("live-session takeover did not set Session Present".into());
        }
        self.wait_for_disconnect_reason(0x8e)?;
        takeover.graceful_disconnect()?;

        let fresh_id = format!("{}-fresh", self.cfg.client_id);
        let mut fresh = self.connect_peer(&fresh_id, false, None, Scenario::Connect)?;
        if fresh.last_connection_result()?.session_present {
            return Err("unknown client id unexpectedly resumed a session".into());
        }
        fresh.graceful_disconnect()?;

        let persistent_id = format!("{}-persistent", self.cfg.client_id);
        let mut persistent =
            self.connect_peer(&persistent_id, true, Some(60), Scenario::Connect)?;
        persistent.graceful_disconnect()?;
        let mut resumed = self.connect_peer(&persistent_id, false, Some(60), Scenario::Connect)?;
        if !resumed.last_connection_result()?.session_present {
            return Err("persisted session did not set Session Present".into());
        }
        resumed.graceful_disconnect()?;

        let stale_id = format!("{}-stale", self.cfg.client_id);
        let mut stale = self.connect_peer(&stale_id, true, Some(60), Scenario::Connect)?;
        stale.engine.close_silent();
        let mut stale_replacement =
            self.connect_peer(&stale_id, false, Some(60), Scenario::Connect)?;
        stale_replacement.graceful_disconnect()?;

        let mut assigned = self.connect_peer("", true, None, Scenario::Connect)?;
        let assigned_id = assigned
            .last_connection_result()?
            .properties
            .as_deref()
            .and_then(|properties| {
                properties.iter().find_map(|property| match property {
                    Property::AssignedClientIdentifier(id) => Some(id.clone()),
                    _ => None,
                })
            })
            .ok_or("CONNACK did not include Assigned Client Identifier")?;
        if assigned_id.is_empty() {
            return Err("broker assigned an empty client id".into());
        }
        assigned.graceful_disconnect()
    }

    fn scenario_mqtt_v5_publish_properties(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let rap_true_topic = format!("{}/rap-true", self.cfg.topic);
        let rap_false_topic = format!("{}/rap-false", self.cfg.topic);
        let retained_payload = b"retained-payload";
        let first = self.received_publishes.len();
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic_with_options(&rap_true_topic, 2, false, true, 0)
                .build()?,
        )?;
        self.wait_for_suback()?;
        self.engine.publish(
            PublishCommand::builder()
                .topic(&rap_true_topic)
                .payload(retained_payload.to_vec())
                .qos(1)
                .retain(true)
                .build()?,
        )?;
        self.wait_for_message_after(&rap_true_topic, retained_payload, first)
            .map_err(|error| format!("RAP=true live delivery failed: {error}"))?;
        if !self.received_publishes[first].retain {
            return Err("RAP=true did not preserve the retained flag".into());
        }

        let second = self.received_publishes.len();
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic_with_options(&rap_false_topic, 2, false, false, 0)
                .build()?,
        )?;
        self.wait_for_suback()?;
        self.engine.publish(
            PublishCommand::builder()
                .topic(&rap_false_topic)
                .payload(retained_payload.to_vec())
                .qos(1)
                .retain(true)
                .build()?,
        )?;
        self.wait_for_message_after(&rap_false_topic, retained_payload, second)
            .map_err(|error| format!("RAP=false live delivery failed: {error}"))?;
        if self.received_publishes[second].retain {
            return Err("RAP=false did not clear the retained flag".into());
        }

        let properties_topic = format!("{}/properties", self.cfg.topic);
        self.engine
            .subscribe(subscribe_cmd(&properties_topic, 2)?)?;
        self.wait_for_suback()?;
        let expected_properties = vec![
            Property::PayloadFormatIndicator(233),
            Property::ResponseTopic(properties_topic.clone()),
            Property::CorrelationData(b"233".to_vec()),
            Property::UserProperty("a".to_string(), "2333".to_string()),
            Property::ContentType("2333".to_string()),
        ];
        let payload = b"publish-properties";
        let before = self.received_publishes.len();
        let mut builder = PublishCommand::builder()
            .topic(&properties_topic)
            .payload(payload.to_vec())
            .qos(0);
        for property in &expected_properties {
            builder = builder.add_property(property.clone());
        }
        self.engine.publish(builder.build()?)?;
        self.wait_for_message_after(&properties_topic, payload, before)?;
        for property in &expected_properties {
            if !has_property(&self.received_publishes[before].properties, property) {
                return Err(format!("delivered PUBLISH omitted property {property:?}").into());
            }
        }

        let overlap_prefix = format!("{}/overlap", self.cfg.topic);
        let overlap_topic = format!("{overlap_prefix}/value");
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic(format!("{overlap_prefix}/+"), 1)
                .with_subscription_id(2333)
                .build()?,
        )?;
        self.wait_for_suback()?;
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic(format!("{overlap_prefix}/#"), 0)
                .with_subscription_id(2333)
                .build()?,
        )?;
        self.wait_for_suback()?;
        let overlap_before = self.received_publishes.len();
        self.engine
            .publish(publish_cmd(&overlap_topic, b"overlap", 2)?)?;
        self.wait_for_topic_message_count_after(&overlap_topic, overlap_before, 2)?;
        for message in self.received_publishes.iter().skip(overlap_before).take(2) {
            if message.qos >= 2
                || !has_property(&message.properties, &Property::SubscriptionIdentifier(2333))
            {
                return Err(format!("overlapping delivery was unexpected: {message:?}").into());
            }
        }

        for retained_topic in [rap_true_topic, rap_false_topic] {
            self.engine.publish(
                PublishCommand::builder()
                    .topic(retained_topic)
                    .payload(Vec::new())
                    .qos(1)
                    .retain(true)
                    .build()?,
            )?;
            self.wait_for_publish_result()?;
        }
        Ok(())
    }

    fn scenario_mqtt_v5_no_local(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic_with_options(&topic, 2, true, false, 0)
                .build()?,
        )?;
        self.wait_for_suback()?;

        let peer_id = format!("{}-peer", self.cfg.client_id);
        let mut peer = self.connect_peer(&peer_id, true, None, Scenario::Connect)?;
        peer.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic_with_options(&topic, 2, true, false, 0)
                .build()?,
        )?;
        peer.wait_for_suback()?;

        self.engine.publish(publish_cmd(&topic, b"from-self", 0)?)?;
        self.flush_outgoing_once()?;
        peer.wait_for_message_after(&topic, b"from-self", 0)?;
        self.drive_for(Duration::from_millis(250))?;
        if self
            .received_publishes
            .iter()
            .any(|message| message.payload == b"from-self")
        {
            return Err("No Local subscription received its own publication".into());
        }

        let before = self.received_publishes.len();
        for index in 0..6 {
            let payload = format!("self-{index}").into_bytes();
            self.engine.publish(publish_cmd(&topic, &payload, 0)?)?;
            self.flush_outgoing_once()?;
        }
        for index in 0..2 {
            let payload = format!("peer-{index}").into_bytes();
            peer.engine.publish(publish_cmd(&topic, &payload, 0)?)?;
            peer.flush_outgoing_once()?;
        }
        self.wait_for_topic_message_count_after(&topic, before, 2)?;
        self.drive_for(Duration::from_millis(250))?;
        let delivered = self.received_publishes.len() - before;
        if delivered != 2 {
            return Err(format!(
                "No Local mixed traffic delivered {delivered} messages, expected 2"
            )
            .into());
        }
        peer.graceful_disconnect()
    }

    fn scenario_mqtt_v5_invalid_packets(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let wildcard_topic = format!("{}/+", self.cfg.topic);
        self.send_publish_on_control(publish_cmd(&wildcard_topic, b"wildcard-topic", 0)?)?;
        self.wait_for_protocol_rejection(0x90)?;

        let mut response = self.connect_peer(
            &format!("{}-response", self.cfg.client_id),
            true,
            None,
            Scenario::Connect,
        )?;
        response.send_publish_on_control(
            PublishCommand::builder()
                .topic(&self.cfg.topic)
                .payload(b"response-topic".to_vec())
                .with_response_topic("response/+")
                .build()?,
        )?;
        response.wait_for_protocol_rejection(0x82)?;

        let mut alias_zero = self.connect_peer(
            &format!("{}-alias-zero", self.cfg.client_id),
            true,
            None,
            Scenario::Connect,
        )?;
        alias_zero.send_publish_on_control(
            PublishCommand::builder()
                .topic(&self.cfg.topic)
                .payload(b"alias-zero".to_vec())
                .with_topic_alias(0)
                .build()?,
        )?;
        alias_zero.wait_for_protocol_rejection(0x94)?;

        let mut alias = self.connect_peer(
            &format!("{}-alias", self.cfg.client_id),
            true,
            None,
            Scenario::Connect,
        )?;
        alias.engine.subscribe(subscribe_cmd(&self.cfg.topic, 2)?)?;
        alias.wait_for_suback()?;
        let first = alias.received_publishes.len();
        alias.send_publish_on_control(
            PublishCommand::builder()
                .topic(&self.cfg.topic)
                .payload(b"alias-first".to_vec())
                .with_topic_alias(233)
                .build()?,
        )?;
        alias.wait_for_message_after(&self.cfg.topic, b"alias-first", first)?;
        alias.send_publish_on_control(
            PublishCommand::builder()
                .topic("")
                .payload(b"alias-reuse".to_vec())
                .with_topic_alias(233)
                .build()?,
        )?;
        alias.wait_for_received_publish_count(first + 2)?;
        alias.graceful_disconnect()?;

        let mut shared = self.connect_peer(
            &format!("{}-shared", self.cfg.client_id),
            true,
            None,
            Scenario::Connect,
        )?;
        shared.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic_with_options("$share/group/topic", 1, true, false, 0)
                .build()?,
        )?;
        shared.wait_for_protocol_rejection(0x82)
    }

    fn scenario_mqtt_v5_batch_subscribe(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topics = ["t1", "t2", "t3"];
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic(topics[0], 1)
                .add_topic(topics[1], 2)
                .add_topic(topics[2], 0)
                .build()?,
        )?;
        expect_reason_codes(
            "authorization SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[0x87, 0x87, 0x87],
        )?;
        self.report.errors.clear();
        self.engine.unsubscribe(UnsubscribeCommand::new(
            None,
            topics.iter().map(|topic| (*topic).to_string()).collect(),
            Vec::new(),
        ))?;
        expect_reason_codes(
            "authorization UNSUBACK",
            &self.wait_for_unsuback()?.reason_codes,
            &[0x11, 0x11, 0x11],
        )
    }

    fn scenario_mqtt_v5_subscribe_max_qos(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.engine.subscribe(subscribe_cmd("t", 2)?)?;
        expect_reason_codes(
            "exact-topic maximum QoS SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[1],
        )?;
        self.engine.subscribe(
            SubscribeCommand::builder()
                .add_topic("glob/sub/topic/#", 1)
                .add_topic("t/+", 2)
                .build()?,
        )?;
        expect_reason_codes(
            "subscription maximum QoS rules SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[0, 2],
        )
    }

    fn scenario_mqtt_v5_max_qos(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let expected = self.cfg.sub_qos;
        let properties = self
            .last_connection_result()?
            .properties
            .as_deref()
            .unwrap_or_default();
        if expected < 2 {
            if !has_property(properties, &Property::MaximumQoS(expected)) {
                return Err(format!("CONNACK omitted Maximum QoS {expected}").into());
            }
        } else if properties
            .iter()
            .any(|property| matches!(property, Property::MaximumQoS(_)))
        {
            return Err("CONNACK included Maximum QoS when the maximum is 2".into());
        }
        for requested in 0..=2 {
            self.engine
                .subscribe(subscribe_cmd(&self.cfg.topic, requested)?)?;
            expect_reason_codes(
                "Maximum QoS SUBACK",
                &self.wait_for_suback()?.reason_codes,
                &[requested.min(expected)],
            )?;
        }
        if expected == 2 {
            return Ok(());
        }
        self.send_publish_on_control(publish_cmd(
            &self.cfg.topic,
            b"unsupported-qos",
            expected + 1,
        )?)?;
        self.wait_for_protocol_rejection(0x9b)
    }

    fn scenario_mqtt_v5_publish_too_large(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let payload = vec![b'a'; 1024];
        self.send_publish_on_control(publish_cmd(&self.cfg.topic, &payload, 1)?)?;
        self.wait_for_protocol_rejection(0x95)
    }

    fn scenario_mqtt_v5_shared_qos2_abort(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let shared_topic = format!("$share/flow/{topic}");
        self.engine.subscribe(subscribe_cmd(&shared_topic, 2)?)?;
        expect_reason_codes(
            "shared QoS 2 SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[2],
        )?;
        let mut publisher = self.connect_peer(
            &format!("{}-publisher", self.cfg.client_id),
            true,
            None,
            Scenario::Connect,
        )?;
        publisher
            .engine
            .publish(publish_cmd(&topic, b"shared-qos2", 2)?)?;
        publisher.flush_outgoing_once()?;
        self.wait_for_message_info(&topic, b"shared-qos2")?;
        self.engine.close_silent();
        publisher.wait_for_publish_result()?;
        publisher.graceful_disconnect()
    }

    fn scenario_mqtt_v5_connack_unavailable(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut result = None;
        self.drive_until("server-busy CONNACK", |events| {
            for event in events {
                if let MqttEvent::Connected(connack) = event {
                    result = Some(connack.clone());
                    return true;
                }
            }
            false
        })?;
        let result = result.ok_or("server-busy CONNACK was not captured")?;
        if result.reason_code != 0x89 {
            return Err(format!(
                "client-id throttle returned {:#04x}, expected Server Busy (0x89)",
                result.reason_code
            )
            .into());
        }
        let properties = result.properties.as_deref().unwrap_or_default();
        if !has_property(properties, &Property::ReasonString("THROTTLED".to_string())) {
            return Err(format!("server-busy CONNACK properties were {properties:?}").into());
        }
        self.report.errors.clear();
        Ok(())
    }

    fn scenario_mqtt_v5_stats_timer(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.engine.ping()?;
        self.drive_until("stats-timer PINGRESP", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::PingResponse(result) if result.success))
        })?;
        self.write_ready_file()?;
        self.drive_for(self.cfg.hold_after_connect)
    }

    fn scenario_mqtt_v5_receive_too_large(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic, 1)?)?;
        expect_reason_codes(
            "maximum-packet-size SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[1],
        )?;
        self.write_ready_file()?;
        let before = self.received_publishes.len();
        self.drive_for(self.cfg.hold_after_connect)?;
        if self.received_publishes.len() != before {
            return Err("delivery larger than Maximum Packet Size reached the client".into());
        }
        Ok(())
    }

    fn scenario_persistent_session_controls(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let client_id = self.cfg.client_id.clone();
        let mut takeover = self.connect_peer(
            &client_id,
            true,
            Some(30),
            Scenario::PersistentSessionControls,
        )?;
        self.wait_for_disconnect_reason(0x8e)?;
        takeover.graceful_disconnect()?;

        let (mut anonymous, connack) =
            self.connect_for_result("", true, Some(30), Scenario::Connect)?;
        if !connack.is_success() {
            return Err(format!("anonymous clean-start CONNECT failed: {connack:?}").into());
        }
        let assigned_id = connack
            .properties
            .as_deref()
            .and_then(|properties| {
                properties.iter().find_map(|property| match property {
                    Property::AssignedClientIdentifier(id) => Some(id.clone()),
                    _ => None,
                })
            })
            .ok_or("persistent anonymous session was not assigned a client id")?;
        anonymous.graceful_disconnect()?;
        let mut assigned_resume =
            self.connect_peer(&assigned_id, false, Some(30), Scenario::Connect)?;
        if !assigned_resume.last_connection_result()?.session_present {
            return Err("assigned client id did not resume its persistent session".into());
        }
        assigned_resume.graceful_disconnect()?;

        let (_without_id, invalid_connack) =
            self.connect_for_result("", false, Some(30), Scenario::Connect)?;
        if invalid_connack.reason_code != 0x85 {
            return Err(format!(
                "empty client id with Clean Start false returned {:#04x}, expected 0x85",
                invalid_connack.reason_code
            )
            .into());
        }

        let cancel_id = format!("{client_id}-cancel");
        let mut cancel = self.connect_peer(
            &cancel_id,
            true,
            Some(30),
            Scenario::PersistentSessionControls,
        )?;
        cancel.disconnect_with_expiry(0)?;
        let mut cancelled = self.connect_peer(
            &cancel_id,
            false,
            Some(30),
            Scenario::PersistentSessionControls,
        )?;
        if cancelled.last_connection_result()?.session_present {
            return Err("DISCONNECT Session Expiry 0 did not cancel persistence".into());
        }
        cancelled.graceful_disconnect()?;

        let persist_id = format!("{client_id}-persist");
        let mut non_persistent = self.connect_peer(
            &persist_id,
            true,
            Some(0),
            Scenario::PersistentSessionControls,
        )?;
        non_persistent.disconnect_with_expiry(30)?;
        let mut not_converted = self.connect_peer(
            &persist_id,
            false,
            Some(30),
            Scenario::PersistentSessionControls,
        )?;
        if not_converted.last_connection_result()?.session_present {
            return Err("DISCONNECT illegally converted a transient session to persistent".into());
        }
        not_converted.graceful_disconnect()
    }

    fn scenario_persistent_offline(&mut self, qos: u8) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let client_id = self.cfg.client_id.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic, qos)?)?;
        expect_reason_codes(
            "persistent offline SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[qos],
        )?;
        self.graceful_disconnect()?;

        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;
        for payload in [b"hello1".as_slice(), b"hello2".as_slice()] {
            publisher
                .engine
                .publish(publish_cmd(&topic, payload, qos)?)?;
            publisher.wait_for_publish_result()?;
        }

        let mut resumed =
            self.connect_peer(&client_id, false, Some(30), Scenario::PersistentOfflineQos1)?;
        if !resumed.last_connection_result()?.session_present {
            return Err("offline subscription session was not resumed".into());
        }
        resumed.wait_for_received_publish_count(2)?;
        let messages = &resumed.received_publishes[..2];
        if messages[0].payload != b"hello1"
            || messages[1].payload != b"hello2"
            || messages.iter().any(|message| message.qos != qos)
        {
            return Err(format!("offline queued messages were unexpected: {messages:?}").into());
        }
        publisher.graceful_disconnect()?;
        resumed.graceful_disconnect()
    }

    fn scenario_persistent_many_qos1(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let prefix = self.cfg.topic.clone();
        let client_id = self.cfg.client_id.clone();
        for filter in [
            format!("{prefix}/t/+/foo"),
            format!("{prefix}/msg/feed/#"),
            format!("{prefix}/loc/+/+/+"),
        ] {
            self.send_subscribe_on_control(subscribe_cmd(&filter, 1)?)?;
            self.wait_for_suback()?;
        }
        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;

        let first_batch = [
            (format!("{prefix}/t/42/foo"), b"M1".to_vec(), 1),
            (format!("{prefix}/t/42/foo"), b"M2".to_vec(), 1),
            (format!("{prefix}/msg/feed/me"), b"M3".to_vec(), 1),
            (format!("{prefix}/loc/1/2/42"), b"M4".to_vec(), 1),
            (format!("{prefix}/t/42/foo"), b"M5".to_vec(), 1),
            (format!("{prefix}/loc/3/4/5"), b"M6".to_vec(), 1),
            (format!("{prefix}/msg/feed/me"), b"M7".to_vec(), 1),
        ];
        self.publish_batch_from_peer_and_wait(&mut publisher, &first_batch, 0)?;
        assert_topicwise_payloads(
            "initial QoS 1 batch",
            &self.received_publishes[..first_batch.len()],
            &first_batch,
        )?;
        let first_messages = self.received_publishes[..first_batch.len()].to_vec();
        for message in first_messages.iter().take(4) {
            let packet_id = message
                .packet_id
                .ok_or("QoS 1 delivery omitted packet id")?;
            let stream = self.stream_or_control(message.stream)?;
            self.engine.puback_on(stream, packet_id)?;
        }
        self.ping_barrier()?;
        self.graceful_disconnect()?;
        std::thread::sleep(Duration::from_millis(500));

        let offline_batch = [
            (format!("{prefix}/loc/3/4/6"), b"M8".to_vec(), 1),
            (format!("{prefix}/t/100/foo"), b"M9".to_vec(), 1),
            (format!("{prefix}/t/100/foo"), b"M10".to_vec(), 1),
            (format!("{prefix}/msg/feed/friend"), b"M11".to_vec(), 1),
            (format!("{prefix}/msg/feed/me"), b"M12".to_vec(), 1),
        ];
        publisher.publish_batch_and_wait(&offline_batch)?;

        let mut resumed = self.connect_peer(&client_id, false, Some(30), Scenario::Connect)?;
        let unacked = &first_messages[4..];
        let expected_count = unacked.len() + offline_batch.len();
        resumed.wait_for_received_publish_count(expected_count)?;
        let messages = &resumed.received_publishes[..expected_count];
        if messages[..unacked.len()].iter().any(|message| !message.dup)
            || messages[unacked.len()..].iter().all(|message| message.dup)
        {
            return Err(format!("QoS 1 replay DUP flags were unexpected: {messages:?}").into());
        }
        for (original, replayed) in unacked.iter().zip(messages.iter()) {
            if original.packet_id != replayed.packet_id
                || original.topic != replayed.topic
                || original.payload != replayed.payload
            {
                return Err(format!(
                    "QoS 1 replay identity changed: original={original:?}, replayed={replayed:?}"
                )
                .into());
            }
        }
        let mut expected_replay = unacked
            .iter()
            .map(|message| (message.topic.clone(), message.payload.clone(), 1))
            .collect::<Vec<_>>();
        expected_replay.extend(offline_batch);
        assert_topicwise_payloads("QoS 1 replay", messages, &expected_replay)?;
        publisher.graceful_disconnect()?;
        resumed.graceful_disconnect()
    }

    fn scenario_persistent_many_qos2(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let client_id = self.cfg.client_id.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic, 2)?)?;
        self.wait_for_suback()?;
        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;
        let first_batch = [
            (b"M1".as_slice(), 1),
            (b"M2".as_slice(), 1),
            (b"M3".as_slice(), 2),
            (b"M4".as_slice(), 2),
            (b"M5".as_slice(), 2),
            (b"M6".as_slice(), 1),
            (b"M7".as_slice(), 2),
            (b"M8".as_slice(), 1),
            (b"M9".as_slice(), 2),
        ]
        .into_iter()
        .map(|(payload, qos)| (topic.clone(), payload.to_vec(), qos))
        .collect::<Vec<_>>();
        self.publish_batch_from_peer_and_wait(&mut publisher, &first_batch, 0)?;
        let first_messages = self.received_publishes[..first_batch.len()].to_vec();
        let first_payloads = first_messages
            .iter()
            .map(|message| message.payload.clone())
            .collect::<Vec<_>>();
        let expected_first_payloads = (1..=9)
            .map(|index| format!("M{index}").into_bytes())
            .collect::<Vec<_>>();
        if first_payloads != expected_first_payloads {
            return Err(format!("initial mixed-QoS order was {first_payloads:?}").into());
        }

        for message in first_messages.iter().filter(|message| message.qos == 1) {
            let stream = self.stream_or_control(message.stream)?;
            self.engine.puback_on(
                stream,
                message
                    .packet_id
                    .ok_or("QoS 1 delivery omitted packet id")?,
            )?;
        }
        let first_qos2 = first_messages
            .iter()
            .filter(|message| message.qos == 2)
            .take(3)
            .cloned()
            .collect::<Vec<_>>();
        for message in &first_qos2 {
            let stream = self.stream_or_control(message.stream)?;
            self.engine.pubrec_on(
                stream,
                message
                    .packet_id
                    .ok_or("QoS 2 delivery omitted packet id")?,
            )?;
        }
        self.ping_barrier()?;
        let first_ids = first_qos2
            .iter()
            .map(|message| message.packet_id.ok_or("QoS 2 delivery omitted packet id"))
            .collect::<Result<Vec<_>, _>>()?;
        for packet_id in &first_ids {
            self.wait_for_pubrel(*packet_id, None)?;
        }
        self.graceful_disconnect()?;
        std::thread::sleep(Duration::from_millis(500));

        let offline_batch = [
            (b"M10".as_slice(), 2),
            (b"M11".as_slice(), 1),
            (b"M12".as_slice(), 2),
        ]
        .into_iter()
        .map(|(payload, qos)| (topic.clone(), payload.to_vec(), qos))
        .collect::<Vec<_>>();
        publisher.publish_batch_and_wait(&offline_batch)?;

        let mut resumed =
            self.connect_peer(&client_id, false, Some(30), Scenario::PersistentManyQos2)?;
        resumed.drive_for(Duration::from_secs(2))?;
        let resumed_pubrels = resumed
            .pubrels
            .iter()
            .map(|pubrel| pubrel.packet_id)
            .collect::<Vec<_>>();
        if resumed_pubrels != first_ids {
            return Err(format!(
                "QoS 2 PUBREL replay was {resumed_pubrels:?}, expected {first_ids:?}"
            )
            .into());
        }

        let duplicates = resumed
            .received_publishes
            .iter()
            .filter(|message| message.dup)
            .cloned()
            .collect::<Vec<_>>();
        if duplicates.is_empty() || duplicates.iter().any(|message| message.qos != 2) {
            return Err(format!("unexpected duplicate replay set: {duplicates:?}").into());
        }
        for duplicate in &duplicates {
            let original = first_messages
                .iter()
                .find(|message| message.payload == duplicate.payload)
                .ok_or_else(|| {
                    format!(
                        "duplicate payload {:?} was not in the initial batch",
                        duplicate.payload
                    )
                })?;
            if duplicate.packet_id != original.packet_id {
                return Err(format!(
                    "packet id changed for {:?}: {:?} -> {:?}",
                    duplicate.payload, original.packet_id, duplicate.packet_id
                )
                .into());
            }
        }

        let newly_pubrec = resumed
            .received_publishes
            .iter()
            .filter(|message| message.qos == 2)
            .take(2)
            .cloned()
            .collect::<Vec<_>>();
        if newly_pubrec.len() != 2 {
            return Err(format!(
                "expected at least two replayed QoS 2 PUBLISH packets, got {:?}",
                resumed.received_publishes
            )
            .into());
        }
        for message in &newly_pubrec {
            let stream = resumed.stream_or_control(message.stream)?;
            let packet_id = message
                .packet_id
                .ok_or("replayed QoS 2 message omitted packet id")?;
            resumed.engine.pubrec_on(stream, packet_id)?;
            let expected_stream = resumed.event_stream(stream);
            resumed.wait_for_pubrel(packet_id, expected_stream)?;
        }
        let newly_pubrec_ids = newly_pubrec
            .iter()
            .filter_map(|message| message.packet_id)
            .collect::<Vec<_>>();
        for packet_id in first_ids.iter().chain(newly_pubrec_ids.iter()) {
            let pubrel = resumed
                .pubrels
                .iter()
                .find(|pubrel| pubrel.packet_id == *packet_id)
                .cloned()
                .ok_or_else(|| format!("PUBREL {packet_id} was not observed"))?;
            let stream = resumed.stream_or_control(pubrel.stream)?;
            resumed.engine.pubcomp_on(stream, *packet_id)?;
        }
        resumed.ping_barrier()?;
        resumed.graceful_disconnect()?;
        std::thread::sleep(Duration::from_millis(500));

        let mut final_resume =
            self.connect_peer(&client_id, false, Some(30), Scenario::PersistentManyQos2)?;
        final_resume.wait_for_received_publish_count(3)?;
        let final_messages = &final_resume.received_publishes[..3];
        let final_payloads = final_messages
            .iter()
            .map(|message| message.payload.clone())
            .collect::<Vec<_>>();
        if final_payloads != [b"M10".to_vec(), b"M11".to_vec(), b"M12".to_vec()] {
            return Err(format!("final QoS replay payloads were {final_payloads:?}").into());
        }
        publisher.graceful_disconnect()?;
        final_resume.graceful_disconnect()
    }

    fn scenario_persistent_clean_start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let client_id = self.cfg.client_id.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic, 1)?)?;
        self.wait_for_suback()?;
        self.graceful_disconnect()?;
        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;
        publisher.engine.publish(publish_cmd(&topic, b"old", 1)?)?;
        publisher.wait_for_publish_result()?;

        let mut clean =
            self.connect_peer(&client_id, true, Some(30), Scenario::PersistentCleanStart)?;
        if clean.last_connection_result()?.session_present {
            return Err("Clean Start did not discard the old session".into());
        }
        clean.send_subscribe_on_control(subscribe_cmd(&topic, 1)?)?;
        clean.wait_for_suback()?;
        clean.drive_for(Duration::from_millis(250))?;
        if clean
            .received_publishes
            .iter()
            .any(|message| message.payload == b"old")
        {
            return Err("message from discarded session was delivered".into());
        }
        publisher
            .engine
            .publish(publish_cmd(&topic, b"current", 1)?)?;
        publisher.wait_for_publish_result()?;
        clean.wait_for_message_after(&topic, b"current", 0)?;
        clean.graceful_disconnect()?;

        let mut resumed =
            self.connect_peer(&client_id, false, Some(30), Scenario::PersistentCleanStart)?;
        if !resumed.last_connection_result()?.session_present {
            return Err("replacement subscription was not persisted".into());
        }
        publisher
            .engine
            .publish(publish_cmd(&topic, b"after-resume", 1)?)?;
        publisher.wait_for_publish_result()?;
        resumed.wait_for_message_after(&topic, b"after-resume", 0)?;
        publisher.graceful_disconnect()?;
        resumed.graceful_disconnect()
    }

    fn scenario_persistent_expiry(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let client_id = self.cfg.client_id.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic, 2)?)?;
        self.wait_for_suback()?;
        self.graceful_disconnect()?;
        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;
        publisher
            .engine
            .publish(publish_cmd(&topic, b"expired", 2)?)?;
        publisher.wait_for_publish_result()?;
        std::thread::sleep(Duration::from_millis(1_500));
        let mut resumed =
            self.connect_peer(&client_id, false, Some(30), Scenario::PersistentExpiry)?;
        if resumed.last_connection_result()?.session_present {
            return Err("expired session was resumed".into());
        }
        resumed.drive_for(Duration::from_millis(500))?;
        if !resumed.received_publishes.is_empty() {
            return Err("queued message from expired session was delivered".into());
        }
        publisher.graceful_disconnect()?;
        resumed.graceful_disconnect()
    }

    fn scenario_persistent_unsubscribe(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = self.cfg.topic.clone();
        let client_id = self.cfg.client_id.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic, 2)?)?;
        expect_reason_codes(
            "persistent unsubscribe SUBACK",
            &self.wait_for_suback()?.reason_codes,
            &[2],
        )?;
        self.send_unsubscribe_on_control(unsubscribe_cmd(&topic)?)?;
        expect_reason_codes(
            "persistent UNSUBACK",
            &self.wait_for_unsuback()?.reason_codes,
            &[0],
        )?;
        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;
        publisher
            .engine
            .publish(publish_cmd(&topic, b"after-unsubscribe", 2)?)?;
        publisher.wait_for_publish_result()?;
        self.drive_for(Duration::from_millis(500))?;
        if !self.received_publishes.is_empty() {
            return Err(format!(
                "message was delivered after unsubscribe: {:?}",
                self.received_publishes
            )
            .into());
        }
        self.graceful_disconnect()?;

        let mut resumed = self.connect_peer(&client_id, false, Some(30), Scenario::Connect)?;
        publisher
            .engine
            .publish(publish_cmd(&topic, b"after-resume", 2)?)?;
        publisher.wait_for_publish_result()?;
        resumed.drive_for(Duration::from_millis(500))?;
        if !resumed.received_publishes.is_empty() {
            return Err(format!(
                "unsubscribed topic was restored on resume: {:?}",
                resumed.received_publishes
            )
            .into());
        }
        publisher.graceful_disconnect()?;
        resumed.graceful_disconnect()
    }

    fn scenario_persistent_unsubscribe_replay(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic1 = format!("{}/unsub", self.cfg.topic);
        let topic2 = format!("{}/sub", self.cfg.topic);
        let client_id = self.cfg.client_id.clone();
        self.send_subscribe_on_control(subscribe_cmd(&topic1, 2)?)?;
        self.wait_for_suback()?;
        self.send_subscribe_on_control(subscribe_cmd(&topic2, 2)?)?;
        self.wait_for_suback()?;
        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;
        let first_batch = vec![
            (topic1.clone(), b"1".to_vec(), 1),
            (topic1.clone(), b"2".to_vec(), 2),
            (topic2.clone(), b"3".to_vec(), 1),
            (topic2.clone(), b"4".to_vec(), 2),
        ];
        self.publish_batch_from_peer_and_wait(&mut publisher, &first_batch, 0)?;
        self.send_unsubscribe_on_control(unsubscribe_cmd(&topic1)?)?;
        self.wait_for_unsuback()?;
        self.graceful_disconnect()?;
        for (payload, qos) in [(b"5".as_slice(), 1), (b"6".as_slice(), 2)] {
            publisher
                .engine
                .publish(publish_cmd(&topic1, payload, qos)?)?;
            publisher.wait_for_publish_result()?;
        }
        let mut resumed = self.connect_peer(&client_id, false, Some(30), Scenario::Connect)?;
        resumed.wait_for_received_publish_count(4)?;
        resumed.drive_for(Duration::from_millis(500))?;
        let messages = &resumed.received_publishes;
        let topic1_payloads = messages
            .iter()
            .filter(|message| message.topic == topic1)
            .map(|message| message.payload.clone())
            .collect::<Vec<_>>();
        let topic2_payloads = messages
            .iter()
            .filter(|message| message.topic == topic2)
            .map(|message| message.payload.clone())
            .collect::<Vec<_>>();
        if messages.len() != 4
            || topic1_payloads != [b"1".to_vec(), b"2".to_vec()]
            || topic2_payloads != [b"3".to_vec(), b"4".to_vec()]
        {
            return Err(format!(
                "unsubscribe replay payloads were topic1={topic1_payloads:?}, \
                 topic2={topic2_payloads:?}, all={messages:?}"
            )
            .into());
        }

        resumed.send_subscribe_on_control(subscribe_cmd(&topic1, 2)?)?;
        expect_reason_codes(
            "resubscribe SUBACK",
            &resumed.wait_for_suback()?.reason_codes,
            &[2],
        )?;
        let fresh_batch = vec![
            (topic1.clone(), b"7".to_vec(), 0),
            (topic1.clone(), b"8".to_vec(), 1),
            (topic1.clone(), b"9".to_vec(), 2),
        ];
        resumed.publish_batch_from_peer_and_wait(&mut publisher, &fresh_batch, 4)?;
        let fresh_payloads = resumed.received_publishes[4..]
            .iter()
            .filter(|message| message.topic == topic1)
            .map(|message| message.payload.clone())
            .collect::<Vec<_>>();
        if fresh_payloads != [b"7".to_vec(), b"8".to_vec(), b"9".to_vec()] {
            return Err(format!("fresh messages after resubscribe were {fresh_payloads:?}").into());
        }
        publisher.graceful_disconnect()?;
        resumed.graceful_disconnect()
    }

    fn scenario_persistent_multiple_matches(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let topic = format!("{}/foo", self.cfg.topic);
        let wildcard = format!("{}/+", self.cfg.topic);
        let client_id = self.cfg.client_id.clone();
        self.send_subscribe_on_control(subscribe_cmd(&wildcard, 2)?)?;
        self.wait_for_suback()?;
        self.send_subscribe_on_control(subscribe_cmd(&topic, 2)?)?;
        self.wait_for_suback()?;
        self.graceful_disconnect()?;
        let mut publisher = self.connect_peer(
            &format!("{client_id}-publisher"),
            true,
            None,
            Scenario::Connect,
        )?;
        publisher
            .engine
            .publish(publish_cmd(&topic, b"two-matches", 2)?)?;
        publisher.wait_for_publish_result()?;
        let mut resumed = self.connect_peer(
            &client_id,
            false,
            Some(30),
            Scenario::PersistentMultipleMatches,
        )?;
        resumed.wait_for_topic_message_count_after(&topic, 0, 2)?;
        if resumed.received_publishes[..2]
            .iter()
            .any(|message| message.qos != 2 || message.payload != b"two-matches")
        {
            return Err("multiple persistent subscription deliveries were unexpected".into());
        }
        publisher.graceful_disconnect()?;
        resumed.graceful_disconnect()
    }

    fn scenario_persistent_sys_messages(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let client_id = self.cfg.client_id.clone();
        let filter = "$SYS/brokers/+/uptime";
        self.send_subscribe_on_control(
            SubscribeCommand::builder()
                .add_topic_with_options(filter, 1, false, false, 2)
                .build()?,
        )?;
        self.wait_for_suback()?;
        self.wait_for_received_publish_count(2)?;
        if self.received_publishes[..2]
            .iter()
            .any(|message| message.qos != 0 || message.retain)
        {
            return Err("system heartbeat delivery metadata was unexpected".into());
        }
        self.graceful_disconnect()?;
        let mut resumed =
            self.connect_peer(&client_id, false, Some(30), Scenario::PersistentSysMessages)?;
        resumed.wait_for_received_publish_count(1)?;
        resumed.graceful_disconnect()
    }

    fn scenario_silent_close(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        std::thread::sleep(Duration::from_secs(16));
        let stream = match self.engine.open_data_stream() {
            Ok(stream) => stream,
            Err(_) => return Ok(()),
        };
        let topic = self.cfg.topic.clone();
        let payload = self.cfg.payload.clone();
        if self
            .engine
            .publish_on(stream, publish_cmd(&topic, &payload, self.cfg.pub_qos)?)
            .is_err()
        {
            return Ok(());
        }
        self.drive_until_for("silent close", Duration::from_secs(3), |events| {
            events.iter().any(|event| {
                matches!(
                    event,
                    MqttEvent::ReconnectNeeded
                        | MqttEvent::TransportClosed { .. }
                        | MqttEvent::Disconnected(_)
                        | MqttEvent::Error(_)
                )
            })
        })
    }

    fn connect_peer(
        &mut self,
        client_id: &str,
        clean_start: bool,
        session_expiry_interval: Option<u32>,
        scenario: Scenario,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut cfg = self.cfg.clone();
        cfg.client_id = client_id.to_string();
        cfg.clean_start = clean_start;
        cfg.session_expiry_interval = session_expiry_interval;
        cfg.scenario = scenario;
        cfg.ready_file = None;
        cfg.hold_after_connect = Duration::ZERO;
        let mut peer = Self::connect(cfg)?;
        let deadline = Instant::now() + self.cfg.timeout;
        let mut connected = false;
        while Instant::now() < deadline {
            let peer_events = peer.step()?;
            connected = peer_events
                .iter()
                .any(|event| matches!(event, MqttEvent::Connected(result) if result.is_success()));
            peer.observe(&peer_events);

            let events = self.step()?;
            self.observe(&events);
            if connected {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        if !connected {
            return Err(format!(
                "timed out connecting peer {client_id}: {:?}",
                peer.report.errors
            )
            .into());
        }
        Ok(peer)
    }

    fn connect_for_result(
        &mut self,
        client_id: &str,
        clean_start: bool,
        session_expiry_interval: Option<u32>,
        scenario: Scenario,
    ) -> Result<(Self, ConnectionResult), Box<dyn std::error::Error>> {
        let mut cfg = self.cfg.clone();
        cfg.client_id = client_id.to_string();
        cfg.clean_start = clean_start;
        cfg.session_expiry_interval = session_expiry_interval;
        cfg.scenario = scenario;
        cfg.ready_file = None;
        cfg.hold_after_connect = Duration::ZERO;
        let mut peer = Self::connect(cfg)?;
        let deadline = Instant::now() + self.cfg.timeout;
        while Instant::now() < deadline {
            let events = peer.step()?;
            let result = events.iter().find_map(|event| match event {
                MqttEvent::Connected(connack) => Some(connack.clone()),
                _ => None,
            });
            peer.observe(&events);
            let current_events = self.step()?;
            self.observe(&current_events);
            if let Some(result) = result {
                return Ok((peer, result));
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!("timed out waiting for CONNACK for client id {client_id:?}").into())
    }

    fn last_connection_result(&self) -> Result<&ConnectionResult, Box<dyn std::error::Error>> {
        self.report
            .connection_results
            .last()
            .ok_or_else(|| "missing CONNACK result".into())
    }

    fn graceful_disconnect(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.engine
            .disconnect_and_close(0, b"scenario peer complete")?;
        self.drive_for(Duration::from_millis(100))
    }

    fn disconnect_with_expiry(
        &mut self,
        session_expiry_interval: u32,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stream = self
            .engine
            .control_stream_id()
            .ok_or("MQTT control stream is not available")?;
        self.engine.send_packet_on(
            stream,
            MqttPacket::Disconnect5(disconnectv5::MqttDisconnect::new(
                0,
                vec![Property::SessionExpiryInterval(session_expiry_interval)],
            )),
        )?;
        self.flush_outgoing_once()?;
        self.drive_for(Duration::from_millis(100))?;
        self.engine.close(0, b"disconnect properties sent")?;
        self.drive_for(Duration::from_millis(100))
    }

    fn send_publish_on_control(
        &mut self,
        mut command: PublishCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if command.qos > 0 && command.packet_id.is_none() {
            command.packet_id = Some(0x7ffe);
        }
        let stream = self
            .engine
            .control_stream_id()
            .ok_or("MQTT control stream is not available")?;
        self.engine
            .send_packet_on(stream, MqttPacket::Publish5(command.to_mqtt_publish()))?;
        Ok(())
    }

    fn stream_or_control(&self, stream: Option<u64>) -> Result<u64, Box<dyn std::error::Error>> {
        stream
            .or_else(|| self.engine.control_stream_id())
            .ok_or_else(|| "MQTT stream is not available".into())
    }

    fn event_stream(&self, stream: u64) -> Option<u64> {
        if Some(stream) == self.engine.control_stream_id() {
            None
        } else {
            Some(stream)
        }
    }

    fn ping_barrier(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.engine.ping()?;
        self.drive_until("PINGRESP acknowledgement barrier", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::PingResponse(result) if result.success))
        })
    }

    fn send_subscribe_on_control(
        &mut self,
        mut command: SubscribeCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let packet_id = command.packet_id.unwrap_or(0x7ffd);
        command.packet_id = Some(packet_id);
        let stream = self
            .engine
            .control_stream_id()
            .ok_or("MQTT control stream is not available")?;
        self.engine.send_packet_on(
            stream,
            MqttPacket::Subscribe5(subscribev5::MqttSubscribe::new(
                packet_id,
                command.subscriptions,
                command.properties,
            )),
        )?;
        Ok(())
    }

    fn send_unsubscribe_on_control(
        &mut self,
        mut command: UnsubscribeCommand,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let packet_id = command.packet_id.unwrap_or(0x7ffc);
        command.packet_id = Some(packet_id);
        let stream = self
            .engine
            .control_stream_id()
            .ok_or("MQTT control stream is not available")?;
        self.engine.send_packet_on(
            stream,
            MqttPacket::Unsubscribe5(unsubscribev5::MqttUnsubscribe::new(
                packet_id,
                command.topics,
                command.properties,
            )),
        )?;
        Ok(())
    }

    fn wait_for_disconnect_reason(
        &mut self,
        expected: u8,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self
            .report
            .disconnect_reasons
            .iter()
            .any(|reason| *reason == Some(expected))
            || self
                .report
                .stream_stop_events
                .iter()
                .any(|event| event.error_code == u64::from(expected))
        {
            return Ok(());
        }
        let deadline = Instant::now() + self.cfg.timeout;
        let mut actual = None;
        while Instant::now() < deadline {
            let events = self.step()?;
            for event in &events {
                if let MqttEvent::Disconnected(reason) = event {
                    actual = *reason;
                    if *reason == Some(expected) {
                        self.observe(&events);
                        return Ok(());
                    }
                }
            }
            self.observe(&events);
            if self
                .report
                .stream_stop_events
                .iter()
                .any(|event| event.error_code == u64::from(expected))
            {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!(
            "expected DISCONNECT {expected:#04x}, got {actual:?}; publish results={:?}, \
             stream closes={}, resets={}, stops={:?}, errors={:?}",
            self.report.publish_results,
            self.report.stream_closed,
            self.report.stream_reset,
            self.report
                .stream_stop_events
                .iter()
                .map(|event| event.error_code)
                .collect::<Vec<_>>(),
            self.report.errors
        )
        .into())
    }

    fn wait_for_protocol_rejection(
        &mut self,
        expected: u8,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let stopped_before = self.report.stream_stop_events.len();
        let deadline = Instant::now() + self.cfg.timeout;
        while Instant::now() < deadline {
            let events = self.step()?;
            let exact_disconnect = events.iter().any(
                |event| matches!(event, MqttEvent::Disconnected(Some(reason)) if *reason == expected),
            );
            self.observe(&events);
            let stopped = self
                .report
                .stream_stop_events
                .iter()
                .skip(stopped_before)
                .any(|event| event.error_code == 0 || event.error_code == u64::from(expected));
            if exact_disconnect || stopped {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!(
            "broker did not reject packet with MQTT reason {expected:#04x} or stop its QUIC stream"
        )
        .into())
    }

    fn wait_for_suback(&mut self) -> Result<SubscribeResult, Box<dyn std::error::Error>> {
        let mut result = None;
        self.drive_until("SUBACK", |events| {
            for event in events {
                if let MqttEvent::Subscribed(suback) = event {
                    result = Some(suback.clone());
                    return true;
                }
            }
            false
        })?;
        result.ok_or_else(|| "SUBACK event was not captured".into())
    }

    fn wait_for_unsuback(&mut self) -> Result<UnsubscribeResult, Box<dyn std::error::Error>> {
        let mut result = None;
        self.drive_until("UNSUBACK", |events| {
            for event in events {
                if let MqttEvent::Unsubscribed(unsuback) = event {
                    result = Some(unsuback.clone());
                    return true;
                }
            }
            false
        })?;
        result.ok_or_else(|| "UNSUBACK event was not captured".into())
    }

    fn wait_for_publish_result(&mut self) -> Result<PublishResult, Box<dyn std::error::Error>> {
        let mut result = None;
        self.drive_until("publish result", |events| {
            for event in events {
                if let MqttEvent::Published(published) = event {
                    result = Some(published.clone());
                    return true;
                }
            }
            false
        })?;
        result.ok_or_else(|| "publish result event was not captured".into())
    }

    fn publish_batch_from_peer_and_wait(
        &mut self,
        publisher: &mut Self,
        messages: &[(String, Vec<u8>, u8)],
        seen_before: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let published_before = publisher.report.published;
        let expected_acks = messages.iter().filter(|(_, _, qos)| *qos > 0).count();
        for (topic, payload, qos) in messages {
            publisher
                .engine
                .publish(publish_cmd(topic, payload, *qos)?)?;
        }
        let deadline = Instant::now() + self.cfg.timeout;
        while Instant::now() < deadline {
            let publisher_events = publisher.step()?;
            publisher.observe(&publisher_events);

            let subscriber_events = self.step()?;
            self.observe(&subscriber_events);
            let published = publisher.report.published - published_before;
            let delivered = self.received_publishes.len() - seen_before;
            if published >= expected_acks && delivered >= messages.len() {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!(
            "timed out publishing batch; acknowledged={}, expected={}, deliveries_before={}, \
             deliveries_now={}, publisher_errors={:?}, subscriber_errors={:?}",
            publisher.report.published - published_before,
            expected_acks,
            seen_before,
            self.received_publishes.len(),
            publisher.report.errors,
            self.report.errors
        )
        .into())
    }

    fn publish_batch_and_wait(
        &mut self,
        messages: &[(String, Vec<u8>, u8)],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let published_before = self.report.published;
        let expected_acks = messages.iter().filter(|(_, _, qos)| *qos > 0).count();
        for (topic, payload, qos) in messages {
            self.engine.publish(publish_cmd(topic, payload, *qos)?)?;
        }
        let deadline = Instant::now() + self.cfg.timeout;
        while Instant::now() < deadline {
            let events = self.step()?;
            self.observe(&events);
            if self.report.published - published_before >= expected_acks {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!(
            "timed out waiting for batch publish acknowledgements; acknowledged={}, expected={}, \
             errors={:?}",
            self.report.published - published_before,
            expected_acks,
            self.report.errors
        )
        .into())
    }

    fn wait_connected(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.drive_until("CONNACK", |events| {
            events
                .iter()
                .any(|event| matches!(event, MqttEvent::Connected(result) if result.is_success()))
        })
    }

    fn wait_for_publish_and_message(
        &mut self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected_topic = topic.to_string();
        let expected_payload = payload.to_vec();
        let mut saw_publish_ack = self.cfg.pub_qos == 0 || self.report.published > 0;
        let mut saw_message = self
            .received_publishes
            .iter()
            .any(|message| message.topic == expected_topic && message.payload == expected_payload);
        if saw_publish_ack && saw_message {
            return Ok(());
        }
        self.drive_until("publish acknowledgement and delivery", |events| {
            if events
                .iter()
                .any(|event| matches!(event, MqttEvent::Published(result) if result.is_success()))
            {
                saw_publish_ack = true;
            }
            for event in events {
                match event {
                    MqttEvent::Published(result) if result.is_success() => {
                        saw_publish_ack = true;
                    }
                    MqttEvent::MessageReceived(message)
                        if message.topic_name == expected_topic
                            && message.payload == expected_payload =>
                    {
                        saw_message = true;
                    }
                    _ => {}
                }
            }
            saw_publish_ack && saw_message
        })
    }

    fn wait_for_message_after(
        &mut self,
        topic: &str,
        payload: &[u8],
        seen_before: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let expected_topic = topic.to_string();
        let expected_payload = payload.to_vec();
        if self
            .received_publishes
            .iter()
            .skip(seen_before)
            .any(|message| message.topic == expected_topic && message.payload == expected_payload)
        {
            return Ok(());
        }
        self.drive_until("new incoming message", |events| {
            events.iter().any(|event| {
                matches!(
                    event,
                    MqttEvent::MessageReceived(message)
                        if message.topic_name == expected_topic
                            && message.payload == expected_payload
                )
            })
        })
    }

    fn wait_for_topic_message_count_after(
        &mut self,
        topic: &str,
        seen_before: usize,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + self.cfg.timeout;
        while Instant::now() < deadline {
            let events = self.step()?;
            self.observe(&events);
            let delivered = self
                .received_publishes
                .iter()
                .skip(seen_before)
                .filter(|message| message.topic == topic)
                .count();
            if delivered >= expected {
                return Ok(());
            }
            if self
                .report
                .errors
                .iter()
                .any(|err| !err.contains("disconnected with reason code"))
            {
                return Err(format!(
                    "failed while waiting for {expected} publishes on {topic}: {:?}",
                    self.report.errors
                )
                .into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        let delivered = self
            .received_publishes
            .iter()
            .skip(seen_before)
            .filter(|message| message.topic == topic)
            .count();
        Err(
            format!("timed out waiting for {expected} publishes on {topic}; got {delivered}")
                .into(),
        )
    }

    fn wait_for_received_publish_count(
        &mut self,
        expected: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + self.cfg.timeout;
        while Instant::now() < deadline {
            if self.received_publishes.len() >= expected {
                return Ok(());
            }
            let events = self.step()?;
            self.observe(&events);
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!(
            "timed out waiting for {expected} incoming PUBLISH packets; received {}: {:?}",
            self.received_publishes.len(),
            self.received_publishes
                .iter()
                .map(|message| (
                    String::from_utf8_lossy(&message.payload).into_owned(),
                    message.dup,
                    message.packet_id
                ))
                .collect::<Vec<_>>()
        )
        .into())
    }

    fn flush_outgoing_once(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let events = self.step()?;
        self.observe(&events);
        if self
            .report
            .errors
            .iter()
            .any(|err| !err.contains("disconnected with reason code"))
        {
            return Err(format!(
                "failed while flushing outgoing bytes: {:?}",
                self.report.errors
            )
            .into());
        }
        Ok(())
    }

    fn wait_for_message_info(
        &mut self,
        topic: &str,
        payload: &[u8],
    ) -> Result<(u16, Option<u64>), Box<dyn std::error::Error>> {
        let expected_topic = topic.to_string();
        let expected_payload = payload.to_vec();
        let mut message_info = self
            .received_publishes
            .iter()
            .find(|message| message.topic == expected_topic && message.payload == expected_payload)
            .and_then(|message| {
                message
                    .packet_id
                    .map(|packet_id| (packet_id, message.stream))
            });
        if let Some(message_info) = message_info {
            return Ok(message_info);
        }
        let mut pending_meta = None;
        self.drive_until("incoming PUBLISH packet id", |events| {
            for event in events {
                match event {
                    MqttEvent::PublishReceived { packet_id, stream } => {
                        pending_meta = Some((*packet_id, *stream));
                    }
                    MqttEvent::MessageReceived(message) => {
                        if message.topic_name == expected_topic
                            && message.payload == expected_payload
                        {
                            let stream = pending_meta.and_then(|(packet_id, stream)| {
                                if packet_id == message.packet_id {
                                    stream
                                } else {
                                    None
                                }
                            });
                            message_info = message.packet_id.map(|packet_id| (packet_id, stream));
                        }
                    }
                    _ => {}
                }
            }
            message_info.is_some()
        })?;
        message_info.ok_or_else(|| "incoming PUBLISH did not carry a packet id".into())
    }

    fn wait_for_pubrel(
        &mut self,
        packet_id: u16,
        expected_stream: Option<u64>,
    ) -> Result<Option<u64>, Box<dyn std::error::Error>> {
        if let Some(pubrel) = self.pubrels.iter().find(|pubrel| {
            pubrel.packet_id == packet_id
                && expected_stream.map_or(true, |stream| Some(stream) == pubrel.stream)
        }) {
            return Ok(pubrel.stream);
        }
        let mut pubrel_stream = None;
        let mut saw_pubrel = false;
        let result = self.drive_until("PUBREL", |events| {
            for event in events {
                if let MqttEvent::PubRelReceived {
                    packet_id: pid,
                    stream,
                } = event
                {
                    let stream_matches = match expected_stream {
                        Some(expected) => Some(expected) == *stream,
                        None => true,
                    };
                    if *pid == packet_id && stream_matches {
                        pubrel_stream = *stream;
                        saw_pubrel = true;
                    }
                }
            }
            saw_pubrel
        });
        if let Err(err) = result {
            return Err(format!(
                "{err}; expected packet id {packet_id}, observed PUBRELs {:?}",
                self.pubrels
            )
            .into());
        }
        Ok(pubrel_stream)
    }

    fn reconnect_and_wait(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.engine.close_silent();
        self.engine.reconnect(Instant::now())?;
        self.report.reconnects += 1;
        self.wait_connected()
    }

    fn start_zero_rtt_reconnect(
        &mut self,
        require_attempted: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.engine.close_silent();
        let zero_rtt = QuicZeroRttConfig {
            session_cache_size: self.cfg.zero_rtt_session_cache_size,
            replay_on_reject: self.cfg.zero_rtt_replay_on_reject,
        };
        let crypto = build_crypto_config(&self.cfg)?;
        self.engine.connect_with_zero_rtt(
            self.server_addr,
            &self.cfg.server_name,
            crypto,
            zero_rtt,
            Instant::now(),
        )?;
        self.report.reconnects += 1;
        if require_attempted && self.engine.zero_rtt_status() != QuicZeroRttStatus::Attempted {
            let events = self.engine.take_events();
            self.observe(&events);
            return Err(format!(
                "0-RTT was not attempted; status {:?}",
                self.engine.zero_rtt_status()
            )
            .into());
        }
        Ok(())
    }

    fn seed_zero_rtt_ticket(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.cfg.scenario.needs_zero_rtt() {
            return Ok(());
        }
        self.start_zero_rtt_reconnect(false)?;
        self.wait_connected()?;
        self.drive_for(Duration::from_millis(1_000))
    }

    fn rebind_socket(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let socket = bind_udp_socket(self.cfg.rebind_addr, self.server_addr)?;
        socket.set_nonblocking(true)?;
        let local_addr = socket.local_addr()?;
        self.socket = socket;
        self.report.rebind_addr = Some(local_addr.to_string());
        Ok(())
    }

    fn drive_until<F>(&mut self, what: &str, predicate: F) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&[MqttEvent]) -> bool,
    {
        self.drive_until_for(what, self.cfg.timeout, predicate)
    }

    fn drive_until_for<F>(
        &mut self,
        what: &str,
        timeout: Duration,
        mut predicate: F,
    ) -> Result<(), Box<dyn std::error::Error>>
    where
        F: FnMut(&[MqttEvent]) -> bool,
    {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            let events = self.step()?;
            let done = predicate(&events);
            self.observe(&events);
            if done {
                return Ok(());
            }
            if self
                .report
                .errors
                .iter()
                .any(|err| !err.contains("disconnected with reason code"))
            {
                return Err(
                    format!("failed while waiting for {what}: {:?}", self.report.errors).into(),
                );
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        Err(format!("timed out waiting for {what}").into())
    }

    fn drive_for(&mut self, duration: Duration) -> Result<(), Box<dyn std::error::Error>> {
        let deadline = Instant::now() + duration;
        while Instant::now() < deadline {
            let events = self.step()?;
            self.observe(&events);
            std::thread::sleep(Duration::from_millis(1));
        }
        Ok(())
    }

    fn step(&mut self) -> Result<Vec<MqttEvent>, Box<dyn std::error::Error>> {
        let now = Instant::now();
        let mut buf = [0u8; 65_535];
        loop {
            match self.socket.recv_from(&mut buf) {
                Ok((len, remote)) if remote == self.server_addr => {
                    self.engine
                        .handle_datagram(buf[..len].to_vec(), remote, now);
                }
                Ok((_len, _remote)) => {}
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err.into()),
            }
        }

        let events = self.engine.handle_tick(now);
        let mut datagrams = self.engine.take_outgoing_datagrams();
        while let Some((dest, bytes)) = datagrams.pop_front() {
            self.socket.send_to(&bytes, dest)?;
        }
        Ok(events)
    }

    fn observe(&mut self, events: &[MqttEvent]) {
        for event in events {
            match event {
                MqttEvent::PublishReceived { packet_id, stream } => {
                    self.pending_publish_meta = Some(PublishMeta {
                        packet_id: *packet_id,
                        stream: *stream,
                    });
                }
                MqttEvent::MessageReceived(message) => {
                    let meta = self.pending_publish_meta.take();
                    self.received_publishes.push(ReceivedPublish {
                        topic: message.topic_name.clone(),
                        payload: message.payload.clone(),
                        qos: message.qos,
                        retain: message.retain,
                        dup: message.dup,
                        properties: message.properties.clone(),
                        packet_id: message.packet_id,
                        stream: meta.and_then(|meta| {
                            if meta.packet_id == message.packet_id {
                                meta.stream
                            } else {
                                None
                            }
                        }),
                    });
                }
                MqttEvent::PubRelReceived { packet_id, stream } => {
                    self.pubrels.push(PubRelSeen {
                        packet_id: *packet_id,
                        stream: *stream,
                    });
                }
                _ => {}
            }
            self.report.observe(event);
        }
    }
}

fn expect_reason_codes(
    what: &str,
    actual: &[u8],
    expected: &[u8],
) -> Result<(), Box<dyn std::error::Error>> {
    if actual == expected {
        Ok(())
    } else {
        Err(format!("{what}: got {actual:?}, expected {expected:?}").into())
    }
}

fn assert_topicwise_payloads(
    what: &str,
    actual: &[ReceivedPublish],
    expected: &[(String, Vec<u8>, u8)],
) -> Result<(), Box<dyn std::error::Error>> {
    if actual.len() != expected.len() {
        return Err(format!(
            "{what}: got {} messages, expected {}; actual={actual:?}",
            actual.len(),
            expected.len()
        )
        .into());
    }
    for (topic, _, _) in expected {
        let actual_for_topic = actual
            .iter()
            .filter(|message| message.topic == *topic)
            .map(|message| (message.payload.as_slice(), message.qos))
            .collect::<Vec<_>>();
        let expected_for_topic = expected
            .iter()
            .filter(|(expected_topic, _, _)| expected_topic == topic)
            .map(|(_, payload, qos)| (payload.as_slice(), *qos))
            .collect::<Vec<_>>();
        if actual_for_topic != expected_for_topic {
            return Err(format!(
                "{what}: topic {topic} order was {actual_for_topic:?}, \
                 expected {expected_for_topic:?}"
            )
            .into());
        }
    }
    Ok(())
}

fn publish_cmd(topic: &str, payload: &[u8], qos: u8) -> Result<PublishCommand, MqttClientError> {
    PublishCommand::builder()
        .topic(topic)
        .payload(payload.to_vec())
        .qos(qos)
        .build()
        .map_err(|err| MqttClientError::ProtocolViolation {
            message: err.to_string(),
        })
}

fn subscribe_cmd(topic: &str, qos: u8) -> Result<SubscribeCommand, MqttClientError> {
    SubscribeCommand::builder()
        .add_topic(topic, qos)
        .build()
        .map_err(|err| MqttClientError::ProtocolViolation {
            message: err.to_string(),
        })
}

fn unsubscribe_cmd(topic: &str) -> Result<UnsubscribeCommand, MqttClientError> {
    Ok(UnsubscribeCommand::new(
        None,
        vec![topic.to_string()],
        Vec::new(),
    ))
}

fn bind_udp_socket(
    requested: Option<SocketAddr>,
    server_addr: SocketAddr,
) -> Result<UdpSocket, std::io::Error> {
    let addr = requested.unwrap_or_else(|| {
        let ip = if server_addr.is_ipv4() {
            std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED)
        } else {
            std::net::IpAddr::V6(std::net::Ipv6Addr::UNSPECIFIED)
        };
        SocketAddr::new(ip, 0)
    });
    UdpSocket::bind(addr)
}

fn make_large_payload(seed: &[u8]) -> Vec<u8> {
    let base = if seed.is_empty() {
        b"x".as_slice()
    } else {
        seed
    };
    let mut payload = Vec::with_capacity(128 * 1024);
    while payload.len() < 128 * 1024 {
        payload.extend_from_slice(base);
    }
    payload.truncate(128 * 1024);
    payload
}

fn tagged_payload(seed: &[u8], tag: &[u8]) -> Vec<u8> {
    let mut payload = Vec::with_capacity(seed.len() + tag.len());
    payload.extend_from_slice(seed);
    payload.extend_from_slice(tag);
    payload
}

fn mqtt_v5_publish_bytes(
    topic: &str,
    payload: &[u8],
    qos: u8,
    packet_id: u16,
) -> Result<Vec<u8>, MqttClientError> {
    if qos > 2 {
        return Err(MqttClientError::ProtocolViolation {
            message: format!("invalid publish QoS {qos}"),
        });
    }

    let topic_len = u16::try_from(topic.len()).map_err(|_| MqttClientError::ProtocolViolation {
        message: "topic is too long".to_string(),
    })?;
    let mut body = Vec::with_capacity(2 + topic.len() + 2 + 1 + payload.len());
    body.extend_from_slice(&topic_len.to_be_bytes());
    body.extend_from_slice(topic.as_bytes());
    if qos > 0 {
        body.extend_from_slice(&packet_id.to_be_bytes());
    }
    body.push(0);
    body.extend_from_slice(payload);

    let mut packet = Vec::with_capacity(1 + 4 + body.len());
    packet.push(0x30 | (qos << 1));
    encode_remaining_length(body.len(), &mut packet)?;
    packet.extend_from_slice(&body);
    Ok(packet)
}

fn encode_remaining_length(mut len: usize, out: &mut Vec<u8>) -> Result<(), MqttClientError> {
    if len > 268_435_455 {
        return Err(MqttClientError::ProtocolViolation {
            message: format!("remaining length too large: {len}"),
        });
    }
    loop {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if len == 0 {
            return Ok(());
        }
    }
}

fn has_property(properties: &[Property], expected: &Property) -> bool {
    properties.iter().any(|property| property == expected)
}

fn mqtt_connect_properties(cfg: &RunConfig) -> Vec<Property> {
    let mut properties = Vec::new();
    if let Some(interval) = cfg
        .session_expiry_interval
        .or_else(|| cfg.scenario.needs_persistent_session().then_some(60))
    {
        properties.push(Property::SessionExpiryInterval(interval));
    }
    if let Some(size) = cfg.maximum_packet_size {
        properties.push(Property::MaximumPacketSize(size));
    }
    if let Some(maximum) = cfg
        .topic_alias_maximum
        .or_else(|| (cfg.scenario == Scenario::SubscribeTopicAlias).then_some(1))
    {
        properties.push(Property::TopicAliasMaximum(maximum));
    }
    if let Some(maximum) = cfg.scenario.receive_maximum() {
        properties.push(Property::ReceiveMaximum(maximum));
    }
    properties
}

fn effective_keep_alive(cfg: &RunConfig) -> u16 {
    if cfg.scenario.needs_manual_keepalive() {
        cfg.keep_alive.clamp(1, 2)
    } else {
        cfg.keep_alive
    }
}

fn keepalive_timeout_window(keep_alive: u16) -> Duration {
    let seconds = u64::from(keep_alive.max(1));
    Duration::from_secs(seconds.saturating_mul(3).saturating_add(1))
}

fn build_crypto_config(cfg: &RunConfig) -> Result<ClientConfig, Box<dyn std::error::Error>> {
    let builder = if cfg.insecure_skip_verify {
        ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(std::sync::Arc::new(InsecureVerifier))
    } else {
        ClientConfig::builder().with_root_certificates(load_roots(cfg)?)
    };

    let mut tls = match (&cfg.cert_file, &cfg.key_file) {
        (Some(cert_file), Some(key_file)) => {
            let certs = CertificateDer::pem_file_iter(cert_file)?.collect::<Result<Vec<_>, _>>()?;
            let key = PrivateKeyDer::from_pem_file(key_file)?;
            builder.with_client_auth_cert(certs, key)?
        }
        _ => builder.with_no_client_auth(),
    };
    tls.alpn_protocols = vec![b"mqtt".to_vec()];
    Ok(tls)
}

fn load_roots(cfg: &RunConfig) -> Result<RootCertStore, Box<dyn std::error::Error>> {
    let mut roots = RootCertStore::empty();
    if let Some(ca_file) = &cfg.ca_file {
        for cert in CertificateDer::pem_file_iter(ca_file)? {
            roots.add(cert?)?;
        }
    } else {
        for cert in rustls_native_certs::load_native_certs()? {
            roots.add(cert)?;
        }
    }
    Ok(roots)
}

#[derive(Debug)]
struct InsecureVerifier;

impl ServerCertVerifier for InsecureVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        vec![
            SignatureScheme::RSA_PKCS1_SHA256,
            SignatureScheme::ECDSA_NISTP256_SHA256,
            SignatureScheme::RSA_PKCS1_SHA384,
            SignatureScheme::ECDSA_NISTP384_SHA384,
            SignatureScheme::RSA_PKCS1_SHA512,
            SignatureScheme::ECDSA_NISTP521_SHA512,
            SignatureScheme::RSA_PSS_SHA256,
            SignatureScheme::RSA_PSS_SHA384,
            SignatureScheme::RSA_PSS_SHA512,
            SignatureScheme::ED25519,
        ]
    }
}

impl fmt::Display for Scenario {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scenario_parser_accepts_known_names() {
        for scenario in Scenario::ALL {
            assert_eq!(Scenario::parse(scenario.as_str()).unwrap(), *scenario);
        }
        assert!(Scenario::parse("missing").is_err());
    }

    #[test]
    fn connect_properties_include_all_configured_values() {
        let cfg = RunConfig {
            session_expiry_interval: Some(30),
            maximum_packet_size: Some(1024),
            topic_alias_maximum: Some(3),
            ..RunConfig::default()
        };
        assert_eq!(
            mqtt_connect_properties(&cfg),
            vec![
                Property::SessionExpiryInterval(30),
                Property::MaximumPacketSize(1024),
                Property::TopicAliasMaximum(3),
            ]
        );
    }
}
