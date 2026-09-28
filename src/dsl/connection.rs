use super::Transport;
use crate::{bind_udp_socket, build_crypto_config, RunConfig};
use flowsdk::mqtt_client::commands::{PublishCommand, SubscribeCommand, UnsubscribeCommand};
use flowsdk::mqtt_client::engine::{MqttEngine, MqttEvent, QuicMqttEngine};
use flowsdk::mqtt_client::opts::MqttClientOptions;
use flowsdk::mqtt_client::ClientSessionState;
use flowsdk::mqtt_serde::control_packet::MqttPacket;
use flowsdk::mqtt_serde::mqttv5::{pubackv5, pubcompv5, pubrecv5};
use mio::{Events, Interest, Poll, Token};
use std::collections::VecDeque;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};

type Result<T> = std::result::Result<T, String>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Channel {
    Control,
    Data(u64),
}

pub(super) enum Connection {
    Quic {
        engine: Box<QuicMqttEngine>,
        socket: UdpSocket,
        remote: SocketAddr,
        outgoing: VecDeque<(SocketAddr, Vec<u8>)>,
        disconnect_stream: Option<u64>,
    },
    Tcp {
        engine: Box<MqttEngine>,
        socket: mio::net::TcpStream,
        poll: Poll,
        events: Events,
        connecting: bool,
        closed: bool,
        outgoing: VecDeque<Vec<u8>>,
        offset: usize,
    },
}

impl Connection {
    pub fn start(
        cfg: &RunConfig,
        options: MqttClientOptions,
        saved: Option<ClientSessionState>,
        transport: Transport,
        remote: SocketAddr,
    ) -> Result<Self> {
        match transport {
            Transport::Quic => {
                let mut engine = QuicMqttEngine::new(options).map_err(error)?;
                if let Some(saved) = saved {
                    engine
                        .engine_mut()
                        .restore_session_state(saved)
                        .map_err(error)?;
                }
                let socket = bind_udp_socket(cfg.local_bind_addr, remote).map_err(error)?;
                socket.set_nonblocking(true).map_err(error)?;
                let crypto = build_crypto_config(cfg).map_err(error)?;
                engine
                    .connect(remote, &cfg.server_name, crypto, Instant::now())
                    .map_err(error)?;
                Ok(Self::Quic {
                    engine: Box::new(engine),
                    socket,
                    remote,
                    outgoing: VecDeque::new(),
                    disconnect_stream: None,
                })
            }
            Transport::Tcp => {
                if cfg.local_bind_addr.is_some() {
                    return Err("TCP local binding is not implemented".into());
                }
                let mut engine = MqttEngine::new(options);
                if let Some(saved) = saved {
                    engine.restore_session_state(saved).map_err(error)?;
                }
                let mut socket = mio::net::TcpStream::connect(remote).map_err(error)?;
                socket.set_nodelay(true).map_err(error)?;
                let poll = Poll::new().map_err(error)?;
                poll.registry()
                    .register(
                        &mut socket,
                        Token(0),
                        Interest::READABLE | Interest::WRITABLE,
                    )
                    .map_err(error)?;
                Ok(Self::Tcp {
                    engine: Box::new(engine),
                    socket,
                    poll,
                    events: Events::with_capacity(8),
                    connecting: true,
                    closed: false,
                    outgoing: VecDeque::new(),
                    offset: 0,
                })
            }
        }
    }

    pub fn take_events(&mut self) -> Vec<MqttEvent> {
        match self {
            Self::Quic { engine, .. } => engine.take_events(),
            Self::Tcp { engine, .. } => engine.take_events(),
        }
    }

    pub fn snapshot_session(&self) -> Result<ClientSessionState> {
        match self {
            Self::Quic { engine, .. } => engine.engine().snapshot_session(),
            Self::Tcp { engine, .. } => engine.snapshot_session(),
        }
        .map_err(error)
    }

    pub fn step(&mut self) -> Result<Vec<MqttEvent>> {
        let now = Instant::now();
        let mut observed = Vec::new();
        match self {
            Self::Quic {
                engine,
                socket,
                remote,
                outgoing,
                disconnect_stream,
            } => {
                let mut buffer = [0; 65_535];
                for _ in 0..64 {
                    match socket.recv_from(&mut buffer) {
                        Ok((len, peer)) if peer == *remote => {
                            engine.handle_datagram(buffer[..len].to_vec(), peer, now);
                        }
                        Ok(_) => {}
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        Err(err) => return Err(error(err)),
                    }
                }
                observed.extend(engine.handle_tick(now));
                if disconnect_stream.is_some_and(|id| {
                    observed.iter().any(|event| {
                        matches!(event,
                            MqttEvent::StreamClosed { stream_id, by_peer: true, .. }
                                if *stream_id == id
                        )
                    })
                }) {
                    engine.close(0, b"DSL test complete").map_err(error)?;
                    *disconnect_stream = None;
                    observed.extend(engine.handle_tick(now));
                }
                outgoing.extend(engine.take_outgoing_datagrams());
                for _ in 0..64 {
                    let Some((peer, bytes)) = outgoing.front() else {
                        break;
                    };
                    match socket.send_to(bytes, *peer) {
                        Ok(len) if len == bytes.len() => {
                            outgoing.pop_front();
                        }
                        Ok(_) => return Err("partial UDP datagram write".into()),
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        Err(err) => return Err(error(err)),
                    }
                }
                observed.extend(engine.take_events());
            }
            Self::Tcp {
                engine,
                socket,
                poll,
                events,
                connecting,
                closed,
                outgoing,
                offset,
            } => {
                if *closed {
                    return Ok(observed);
                }
                if *connecting {
                    poll.poll(events, Some(Duration::ZERO)).map_err(error)?;
                    if events.is_empty() {
                        return Ok(observed);
                    }
                    if let Some(err) = socket.take_error().map_err(error)? {
                        return Err(error(err));
                    }
                    socket.peer_addr().map_err(error)?;
                    engine.connect().map_err(error)?;
                    *connecting = false;
                }
                let mut buffer = [0; 65_535];
                for _ in 0..64 {
                    match socket.read(&mut buffer) {
                        Ok(0) => {
                            *closed = true;
                            engine.handle_connection_lost();
                            observed.push(MqttEvent::TransportClosed {
                                reason: "TCP EOF".into(),
                                by_peer: true,
                                error_code: None,
                            });
                            break;
                        }
                        Ok(len) => observed.extend(engine.handle_incoming(&buffer[..len])),
                        Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                        Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        Err(err) => return Err(error(err)),
                    }
                }
                if !*closed {
                    observed.extend(engine.handle_tick(now));
                    let bytes = engine.take_outgoing();
                    if !bytes.is_empty() {
                        outgoing.push_back(bytes);
                    }
                    for _ in 0..64 {
                        let Some(bytes) = outgoing.front() else { break };
                        match socket.write(&bytes[*offset..]) {
                            Ok(0) => return Err("TCP write returned zero".into()),
                            Ok(len) => {
                                *offset += len;
                                if *offset == bytes.len() {
                                    outgoing.pop_front();
                                    *offset = 0;
                                }
                            }
                            Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                            Err(err) => return Err(error(err)),
                        }
                    }
                }
                observed.extend(engine.take_events());
            }
        }
        Ok(observed)
    }

    pub fn open_stream(&mut self) -> Result<u64> {
        match self {
            Self::Quic { engine, .. } => engine.open_data_stream().map_err(error),
            Self::Tcp { .. } => Err("open_stream requires QUIC; TCP has one MQTT channel".into()),
        }
    }

    pub fn subscribe(
        &mut self,
        channel: Option<Channel>,
        command: SubscribeCommand,
    ) -> Result<u16> {
        match (self, channel) {
            (Self::Quic { engine, .. }, Some(Channel::Control)) => {
                engine.subscribe_on_control(command)
            }
            (Self::Quic { engine, .. }, Some(Channel::Data(id))) => {
                engine.subscribe_on(id, command)
            }
            (Self::Quic { engine, .. }, None) => engine.subscribe(command),
            (Self::Tcp { engine, .. }, _) => engine.subscribe(command),
        }
        .map_err(error)
    }

    pub fn unsubscribe(
        &mut self,
        channel: Option<Channel>,
        command: UnsubscribeCommand,
    ) -> Result<u16> {
        match (self, channel) {
            (Self::Quic { engine, .. }, Some(Channel::Control)) => {
                engine.unsubscribe_on_control(command)
            }
            (Self::Quic { engine, .. }, Some(Channel::Data(id))) => {
                engine.unsubscribe_on(id, command)
            }
            (Self::Quic { engine, .. }, None) => engine.unsubscribe(command),
            (Self::Tcp { engine, .. }, _) => engine.unsubscribe(command),
        }
        .map_err(error)
    }

    pub fn publish(
        &mut self,
        channel: Option<Channel>,
        command: PublishCommand,
    ) -> Result<Option<u16>> {
        match (self, channel) {
            (Self::Quic { engine, .. }, Some(Channel::Control)) => {
                engine.engine_mut().publish(command)
            }
            (Self::Quic { engine, .. }, Some(Channel::Data(id))) => engine.publish_on(id, command),
            (Self::Quic { engine, .. }, None) => engine.publish(command),
            (Self::Tcp { engine, .. }, _) => engine.publish(command),
        }
        .map_err(error)
    }

    pub fn ping(&mut self) -> Result<()> {
        match self {
            Self::Quic { engine, .. } => engine.ping(),
            Self::Tcp { engine, .. } => engine.try_send_ping(),
        }
        .map_err(error)
    }

    pub fn ack(&mut self, kind: &str, channel: Channel, packet_id: u16) -> Result<()> {
        match self {
            Self::Quic { engine, .. } => {
                let id = match channel {
                    Channel::Control => engine.control_stream_id().ok_or("no control stream")?,
                    Channel::Data(id) => id,
                };
                let packet = match kind {
                    "puback" => {
                        MqttPacket::PubAck5(pubackv5::MqttPubAck::new(packet_id, 0, vec![]))
                    }
                    "pubrec" => {
                        MqttPacket::PubRec5(pubrecv5::MqttPubRec::new(packet_id, 0, vec![]))
                    }
                    "pubcomp" => {
                        MqttPacket::PubComp5(pubcompv5::MqttPubComp::new(packet_id, 0, vec![]))
                    }
                    _ => return Err(format!("unknown ACK kind: {kind}")),
                };
                engine.acknowledge_on(id, packet).map_err(error)
            }
            Self::Tcp { engine, .. } => match kind {
                "puback" => engine.puback(packet_id, 0, vec![]),
                "pubrec" => engine.pubrec(packet_id, 0, vec![]),
                "pubcomp" => engine.pubcomp(packet_id, 0, vec![]),
                _ => return Err(format!("unknown ACK kind: {kind}")),
            }
            .map_err(error),
        }
    }

    pub fn disconnect(&mut self, reason_code: u8) -> Result<()> {
        match self {
            Self::Quic {
                engine,
                disconnect_stream,
                ..
            } => {
                let stream = engine.control_stream_id().ok_or("no control stream")?;
                engine
                    .engine_mut()
                    .try_disconnect_with(reason_code, vec![])
                    .map_err(error)?;
                // CONNECTION_CLOSE can discard even transmitted DISCONNECT bytes.
                // FIN preserves stream ordering; wait for peer shutdown before closing.
                engine.finish_stream(stream).map_err(error)?;
                *disconnect_stream = Some(stream);
                Ok(())
            }
            Self::Tcp { engine, .. } => engine.try_disconnect_with(reason_code, vec![]),
        }
        .map_err(error)
    }

    pub fn flushed(&self) -> bool {
        match self {
            Self::Quic {
                engine, outgoing, ..
            } => engine.disconnect_complete() && outgoing.is_empty(),
            Self::Tcp {
                engine, outgoing, ..
            } => !engine.has_pending_output() && outgoing.is_empty(),
        }
    }

    pub fn connected(&self) -> bool {
        match self {
            Self::Quic { engine, .. } => engine.is_connected(),
            Self::Tcp { engine, .. } => engine.is_connected(),
        }
    }

    pub fn local_addr(&self) -> Result<SocketAddr> {
        match self {
            Self::Quic { socket, .. } => socket.local_addr(),
            Self::Tcp { socket, .. } => socket.local_addr(),
        }
        .map_err(error)
    }

    pub fn stream_count(&self) -> usize {
        match self {
            Self::Quic { engine, .. } => engine.data_stream_count(),
            Self::Tcp { .. } => 0,
        }
    }
}

fn error(err: impl std::fmt::Display) -> String {
    err.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incompatible_checkpoints_are_rejected_before_opening_either_transport() {
        crate::install_crypto_provider();
        let saved = MqttEngine::new(
            MqttClientOptions::builder()
                .peer("127.0.0.1:1")
                .client_id("saved")
                .clean_start(false)
                .build(),
        )
        .snapshot_session()
        .unwrap();
        for transport in [Transport::Quic, Transport::Tcp] {
            for (peer, client_id, clean_start) in [
                ("127.0.0.1:1", "saved", true),
                ("127.0.0.1:1", "wrong-id", false),
                ("127.0.0.1:2", "saved", false),
            ] {
                let options = MqttClientOptions::builder()
                    .peer(peer)
                    .client_id(client_id)
                    .clean_start(clean_start)
                    .build();
                let error = Connection::start(
                    &RunConfig::default(),
                    options,
                    Some(saved.clone()),
                    transport,
                    "127.0.0.1:1".parse().unwrap(),
                )
                .err()
                .expect("invalid state must fail before network I/O");
                assert!(error.contains("session state"), "{error}");
            }
            let mut json = serde_json::to_value(&saved).unwrap();
            json["version"] = serde_json::json!(999);
            let options = MqttClientOptions::builder()
                .peer("127.0.0.1:1")
                .client_id("saved")
                .clean_start(false)
                .build();
            let error = Connection::start(
                &RunConfig::default(),
                options,
                Some(serde_json::from_value(json).unwrap()),
                transport,
                "127.0.0.1:1".parse().unwrap(),
            )
            .err()
            .expect("invalid version must fail");
            assert!(
                error.contains("Unsupported session state version"),
                "{error}"
            );
        }
    }
}
