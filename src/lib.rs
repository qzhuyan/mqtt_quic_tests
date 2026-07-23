use flowsdk::mqtt_client::commands::{PublishCommand, SubscribeCommand, UnsubscribeCommand};
use flowsdk::mqtt_client::engine::{
    MqttEvent, QuicMqttEngine, QuicZeroRttConfig, QuicZeroRttStatus,
};
use flowsdk::mqtt_client::opts::MqttClientOptions;
use flowsdk::mqtt_client::MqttClientError;
use flowsdk::mqtt_serde::control_packet::MqttPacket;
use flowsdk::mqtt_serde::mqttv5::common::properties::Property;
use flowsdk::mqtt_serde::mqttv5::connectv5;
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
                if let Some(code) = reason {
                    self.errors
                        .push(format!("disconnected with reason code {code}"));
                }
            }
            MqttEvent::Subscribed(result) => {
                if result.is_success() {
                    self.subscribed += result.successful_subscriptions();
                } else {
                    self.errors
                        .push(format!("subscribe failed: {:?}", result.reason_codes));
                }
            }
            MqttEvent::Unsubscribed(result) => {
                if result.is_success() {
                    self.unsubscribed += 1;
                } else {
                    self.errors
                        .push(format!("unsubscribe failed: {:?}", result.reason_codes));
                }
            }
            MqttEvent::Published(result) => {
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

#[derive(Debug)]
struct ReceivedPublish {
    topic: String,
    payload: Vec<u8>,
    packet_id: Option<u16>,
    stream: Option<u64>,
}

#[derive(Debug)]
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
        self.wait_connected()?;
        self.write_ready_file()?;
        match self.cfg.scenario {
            Scenario::Connect => self.drive_for(self.cfg.hold_after_connect)?,
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
        self.drive_until("PUBREL", |events| {
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
        })?;
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
}
