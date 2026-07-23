use clap::{ArgAction, Parser};
use mqtt_quic_tests::{run, RunConfig, Scenario};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Parser)]
#[command(
    name = "mqtt_quic_test",
    about = "FlowSDK MQTT-over-QUIC test client for EMQX Common Test suites"
)]
struct Cli {
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value_t = 14567)]
    port: u16,

    #[arg(long, default_value = "localhost")]
    server_name: String,

    #[arg(long, default_value = "mqtt-quic-flow-test")]
    client_id: String,

    #[arg(long, default_value = "test/quic/multistream")]
    topic: String,

    #[arg(long, default_value = "hello from flowsdk mqtt_quic_tests")]
    payload: String,

    #[arg(long, default_value_t = Scenario::MultiStream, value_parser = Scenario::parse)]
    scenario: Scenario,

    #[arg(long, default_value_t = 1, value_parser = parse_qos)]
    pub_qos: u8,

    #[arg(long, default_value_t = 1, value_parser = parse_qos)]
    sub_qos: u8,

    #[arg(long, default_value_t = 10_000)]
    timeout_ms: u64,

    #[arg(long, default_value_t = 0)]
    hold_ms: u64,

    #[arg(long, default_value_t = 30)]
    keep_alive: u16,

    #[arg(long, default_value_t = true, value_parser = parse_bool, action = ArgAction::Set)]
    clean_start: bool,

    #[arg(long)]
    session_expiry_interval: Option<u32>,

    #[arg(long)]
    maximum_packet_size: Option<u32>,

    #[arg(long)]
    insecure: bool,

    #[arg(long)]
    ca_file: Option<PathBuf>,

    #[arg(long)]
    cert_file: Option<PathBuf>,

    #[arg(long)]
    key_file: Option<PathBuf>,

    #[arg(long, default_value = "00000000000000000000")]
    malformed_hex: String,

    #[arg(long)]
    ready_file: Option<PathBuf>,

    #[arg(long)]
    local_bind_addr: Option<SocketAddr>,

    #[arg(long)]
    rebind_addr: Option<SocketAddr>,

    #[arg(long, default_value_t = 256)]
    zero_rtt_session_cache_size: usize,

    #[arg(long, default_value_t = true, value_parser = parse_bool, action = ArgAction::Set)]
    zero_rtt_replay_on_reject: bool,

    #[arg(long, default_value_t = 42)]
    stream_error_code: u64,
}

fn main() {
    let cli = Cli::parse();
    match run(cli.into_run_config()) {
        Ok(report) => {
            println!("{}", serde_json::to_string_pretty(&report).unwrap());
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}

impl Cli {
    fn into_run_config(self) -> RunConfig {
        RunConfig {
            host: self.host,
            port: self.port,
            server_name: self.server_name,
            client_id: self.client_id,
            topic: self.topic,
            payload: self.payload.into_bytes(),
            scenario: self.scenario,
            pub_qos: self.pub_qos,
            sub_qos: self.sub_qos,
            timeout: Duration::from_millis(self.timeout_ms),
            keep_alive: self.keep_alive,
            clean_start: self.clean_start,
            session_expiry_interval: self.session_expiry_interval,
            maximum_packet_size: self.maximum_packet_size,
            insecure_skip_verify: self.insecure,
            ca_file: self.ca_file,
            cert_file: self.cert_file,
            key_file: self.key_file,
            malformed_bytes: parse_hex(&self.malformed_hex).unwrap_or_else(|err| {
                eprintln!("invalid --malformed-hex: {err}");
                std::process::exit(2);
            }),
            ready_file: self.ready_file,
            hold_after_connect: Duration::from_millis(self.hold_ms),
            local_bind_addr: self.local_bind_addr,
            rebind_addr: self.rebind_addr,
            zero_rtt_session_cache_size: self.zero_rtt_session_cache_size,
            zero_rtt_replay_on_reject: self.zero_rtt_replay_on_reject,
            stream_error_code: self.stream_error_code,
        }
    }
}

fn parse_qos(value: &str) -> Result<u8, String> {
    let qos = value
        .parse::<u8>()
        .map_err(|err| format!("invalid QoS value: {err}"))?;
    if qos <= 2 {
        Ok(qos)
    } else {
        Err("QoS must be 0, 1, or 2".to_string())
    }
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(format!("invalid boolean: {value}")),
    }
}

fn parse_hex(value: &str) -> Result<Vec<u8>, String> {
    let compact = value
        .chars()
        .filter(|ch| !ch.is_ascii_whitespace() && *ch != ':')
        .collect::<String>();
    if compact.len() % 2 != 0 {
        return Err("hex string must contain an even number of digits".to_string());
    }
    let mut bytes = Vec::with_capacity(compact.len() / 2);
    for idx in (0..compact.len()).step_by(2) {
        bytes.push(
            u8::from_str_radix(&compact[idx..idx + 2], 16)
                .map_err(|err| format!("invalid hex byte at offset {idx}: {err}"))?,
        );
    }
    Ok(bytes)
}
