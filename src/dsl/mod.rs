//! Rhai test scripts over FlowSDK's Sans-I/O QUIC and TCP engines.
mod connection;
mod runtime;
mod session;

use crate::{install_crypto_provider, RunConfig, ScenarioReport};
use rhai::{Array, Blob, Dynamic, Engine, EvalAltResult, Map, NativeCallContext, Scope, INT};
use runtime::{ClientHandle, Inbox, Operation, Runtime, StreamHandle};
use serde::Serialize;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::{Duration, Instant};

include!(concat!(env!("OUT_DIR"), "/scenarios.rs"));

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Transport {
    #[default]
    Quic,
    Tcp,
}

impl Transport {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Quic => "quic",
            Self::Tcp => "tcp",
        }
    }
}

#[derive(Debug, Clone)]
pub struct ScriptOptions {
    pub transport: Transport,
    pub scenario_timeout: Duration,
    pub session_store_dir: Option<std::path::PathBuf>,
}

impl Default for ScriptOptions {
    fn default() -> Self {
        Self {
            transport: Transport::Quic,
            scenario_timeout: Duration::from_secs(120),
            session_store_dir: None,
        }
    }
}

#[derive(Serialize)]
pub struct ScriptReport {
    #[serde(flatten)]
    pub summary: ScenarioReport,
    pub status: &'static str,
    pub runner: &'static str,
    pub dsl_version: u32,
    pub transport: Transport,
    pub assertions: usize,
    pub elapsed_ms: u128,
    pub error: Option<String>,
    pub clients: BTreeMap<String, serde_json::Value>,
    pub recent_events: Vec<serde_json::Value>,
}

impl ScriptReport {
    pub fn exit_code(&self) -> i32 {
        match self.status {
            "passed" => 0,
            "invalid" => 2,
            _ => 1,
        }
    }
}

pub fn bundled(name: &str) -> Option<&'static str> {
    BUNDLED
        .iter()
        .find(|(candidate, _)| *candidate == name)
        .map(|(_, source)| *source)
}

pub fn bundled_names() -> impl Iterator<Item = &'static str> {
    BUNDLED.iter().map(|(name, _)| *name)
}

pub fn check_source(name: &str, source: &str) -> Result<(), String> {
    let rt = Rc::new(RefCell::new(Runtime::new(
        RunConfig::default(),
        Transport::Quic,
        Duration::from_secs(10),
    )));
    let engine = script_engine(rt);
    engine
        .compile_with_scope(
            &script_scope(&RunConfig::default(), Transport::Quic),
            source,
        )
        .map(|_| ())
        .map_err(|e| format!("{name}: {e}"))
}

pub fn run_source(
    name: &str,
    source: &str,
    cfg: RunConfig,
    options: ScriptOptions,
) -> ScriptReport {
    install_crypto_provider();
    // Keep Instant arithmetic and accidental unbounded scripts within a useful test budget.
    let timeout = options.scenario_timeout.min(Duration::from_secs(86_400));
    let rt = Rc::new(RefCell::new(Runtime::new(
        cfg.clone(),
        options.transport,
        timeout,
    )));
    let engine = script_engine(rt.clone());
    rt.borrow_mut().session_store = session::SessionStore::new(options.session_store_dir);
    let mut scope = script_scope(&cfg, options.transport);
    let (mut status, mut error) = match engine.compile_with_scope(&scope, source) {
        Err(err) => ("invalid", Some(format!("{name}: {err}"))),
        Ok(mut ast) => {
            ast.set_source(name);
            match engine.run_ast_with_scope(&mut scope, &ast) {
                Err(err) => ("failed", Some(format!("{name}: {err}"))),
                Ok(()) => ("passed", None),
            }
        }
    };
    let mut runtime = rt.borrow_mut();
    if status == "passed" {
        if let Err(err) = runtime.finish() {
            status = "failed";
            error = Some(format!("{name}: {err}"));
        }
    }
    runtime.cleanup();
    for client in &mut runtime.clients {
        client.report.scenario = name.into();
    }
    let clients = runtime
        .clients
        .iter()
        .map(|c| {
            (
                c.name.clone(),
                serde_json::to_value(&c.report).expect("serializable client report"),
            )
        })
        .collect();
    let mut summary = runtime
        .clients
        .iter_mut()
        .find(|c| c.name == "main")
        .map(|c| std::mem::take(&mut c.report))
        .unwrap_or_else(|| ScenarioReport::new(&cfg));
    summary.scenario = name.into();
    if let Some(err) = &error {
        summary.errors.push(err.clone());
    }
    ScriptReport {
        summary,
        status,
        runner: "rhai",
        dsl_version: 1,
        transport: options.transport,
        assertions: runtime.assertions,
        elapsed_ms: runtime.started.elapsed().as_millis(),
        error,
        clients,
        recent_events: runtime
            .events
            .iter()
            .rev()
            .take(20)
            .rev()
            .map(|e| json_value(&Dynamic::from_map(e.data.clone())))
            .collect(),
    }
}

fn script_scope(cfg: &RunConfig, transport: Transport) -> Scope<'static> {
    let mut values = Map::new();
    values.insert("host".into(), cfg.host.clone().into());
    values.insert("port".into(), INT::from(cfg.port).into());
    values.insert("client_id".into(), cfg.client_id.clone().into());
    values.insert("topic".into(), cfg.topic.clone().into());
    values.insert("payload".into(), Dynamic::from_blob(cfg.payload.clone()));
    values.insert("pub_qos".into(), INT::from(cfg.pub_qos).into());
    values.insert("sub_qos".into(), INT::from(cfg.sub_qos).into());
    values.insert(
        "timeout_ms".into(),
        (cfg.timeout.as_millis().min(INT::MAX as u128) as INT).into(),
    );
    values.insert(
        "hold_ms".into(),
        (cfg.hold_after_connect.as_millis().min(INT::MAX as u128) as INT).into(),
    );
    values.insert("transport".into(), transport.as_str().into());
    let mut scope = Scope::new();
    scope.push_constant("cfg", values);
    scope
}

fn script_engine(runtime: Rc<RefCell<Runtime>>) -> Engine {
    let mut engine = Engine::new();
    engine.set_optimization_level(rhai::OptimizationLevel::None);
    engine
        .set_fail_on_invalid_map_property(true)
        .set_max_operations(2_000_000)
        .set_max_call_levels(64)
        .set_max_string_size(1024 * 1024)
        .set_max_array_size(1024 * 1024)
        .set_max_map_size(256);
    // Imports and eval are reserved for the future scoped module loader.
    engine
        .disable_symbol("eval")
        .disable_symbol("import")
        .disable_symbol("export");
    engine.on_print(|s| eprintln!("{s}"));
    engine.on_debug(|s, source, pos| eprintln!("{}:{pos}: {s}", source.unwrap_or("script")));
    let deadline = runtime.borrow().deadline;
    engine.on_progress(move |_| {
        (Instant::now() >= deadline).then(|| Dynamic::from("scenario deadline exceeded"))
    });
    engine.register_type_with_name::<ClientHandle>("Client");
    engine.register_type_with_name::<StreamHandle>("Stream");
    engine.register_type_with_name::<Operation>("Operation");
    engine.register_type_with_name::<Inbox>("Inbox");
    engine.register_fn("==", |a: StreamHandle, b: StreamHandle| a == b);
    engine.register_fn("!=", |a: StreamHandle, b: StreamHandle| a != b);
    engine.register_fn("bytes", |s: &str| -> Blob { s.as_bytes().to_vec() });
    engine.register_fn("text", |b: Blob| -> Result<String, Box<EvalAltResult>> {
        String::from_utf8(b).map_err(|e| e.to_string().into())
    });
    engine.register_fn("hex", |s: &str| -> Result<Blob, Box<EvalAltResult>> {
        let bytes: Vec<_> = s
            .bytes()
            .filter(|b| !b.is_ascii_whitespace() && *b != b':')
            .collect();
        if bytes.len() % 2 != 0 {
            return Err("hex requires an even number of ASCII digits".into());
        }
        bytes
            .chunks_exact(2)
            .map(|pair| {
                let high = char::from(pair[0])
                    .to_digit(16)
                    .ok_or("invalid hex digit")?;
                let low = char::from(pair[1])
                    .to_digit(16)
                    .ok_or("invalid hex digit")?;
                Ok((high * 16 + low) as u8)
            })
            .collect::<Result<Blob, &str>>()
            .map_err(Into::into)
    });

    macro_rules! bind {
        ($name:literal, |$rt:ident, $($arg:ident: $ty:ty),*| $body:expr) => {{
            let shared = runtime.clone();
            engine.register_fn($name, move |ctx: NativeCallContext, $($arg: $ty),*| {
                let mut $rt = shared.borrow_mut();
                // Keep `?` inside this callback so every native failure reaches the ledger.
                #[allow(clippy::redundant_closure_call)]
                let result: runtime::Result<_> = (|| $body)();
                result.map_err(|message| {
                    let err = Box::new(EvalAltResult::ErrorRuntime(message.into(), ctx.call_position()));
                    $rt.failures.push(err.to_string());
                    err
                })
            });
        }};
    }
    bind!("require_dsl", |rt, version: INT| {
        if version == 1 {
            Ok(())
        } else {
            Err(format!("unsupported DSL version {version} (supported: 1)"))
        }
    });
    bind!("client", |rt, label: &str| rt
        .create_client(label, Map::new()));
    bind!("client", |rt, label: &str, options: Map| rt
        .create_client(label, options));
    bind!("connect", |rt, c: &mut ClientHandle| rt.connect(*c));
    bind!("open_stream", |rt, c: &mut ClientHandle, name: &str| rt
        .open_stream(*c, name));
    bind!("control_stream", |rt, c: &mut ClientHandle| rt
        .control_stream(*c));
    bind!("subscribe", |rt,
                        c: &mut ClientHandle,
                        topic: &str,
                        qos: INT| rt
        .subscribe(*c, None, topic, qos));
    bind!("subscribe", |rt,
                        c: &mut ClientHandle,
                        stream: StreamHandle,
                        topic: &str,
                        qos: INT| rt.subscribe(
        *c,
        Some(stream),
        topic,
        qos
    ));
    bind!("unsubscribe", |rt, c: &mut ClientHandle, topic: &str| rt
        .unsubscribe(*c, None, topic));
    bind!("unsubscribe", |rt,
                          c: &mut ClientHandle,
                          stream: StreamHandle,
                          topic: &str| rt
        .unsubscribe(*c, Some(stream), topic));
    bind!("publish", |rt, c: &mut ClientHandle, options: Map| rt
        .publish(*c, None, options));
    bind!("publish", |rt,
                      c: &mut ClientHandle,
                      stream: StreamHandle,
                      options: Map| rt.publish(
        *c,
        Some(stream),
        options
    ));
    bind!("ping", |rt, c: &mut ClientHandle| rt.ping(*c));
    bind!("puback", |rt, c: &mut ClientHandle, message: Map| rt
        .ack(*c, "puback", message));
    bind!("pubrec", |rt, c: &mut ClientHandle, message: Map| rt
        .ack(*c, "pubrec", message));
    bind!("pubcomp", |rt, c: &mut ClientHandle, message: Map| rt
        .ack(*c, "pubcomp", message));
    bind!("watch", |rt, c: &mut ClientHandle, kind: &str| rt
        .watch(*c, kind));
    bind!("expect_ok", |rt, op: Operation| rt.wait_operation(op, true));
    bind!("expect_result", |rt, op: Operation| rt
        .wait_operation(op, false));
    bind!("expect", |rt, inbox: Inbox, matcher: Map, timeout: INT| {
        Ok(rt
            .expect(inbox, matcher, 1, millis(timeout)?, false)?
            .remove(0))
    });
    bind!("expect_none", |rt,
                          inbox: Inbox,
                          matcher: Map,
                          duration: INT| {
        rt.expect(inbox, matcher, 0, millis(duration)?, true)
            .map(|_| ())
    });
    bind!("expect_count", |rt,
                           inbox: Inbox,
                           matcher: Map,
                           count: INT,
                           duration: INT| {
        let count = usize::try_from(count).map_err(|_| "count must be nonnegative")?;
        Ok(rt
            .expect(inbox, matcher, count, millis(duration)?, true)?
            .into_iter()
            .map(Dynamic::from_map)
            .collect::<Array>())
    });
    bind!("poll_for", |rt, duration: INT| rt
        .poll_for(millis(duration)?));
    bind!("ready", |rt, c: ClientHandle| rt.ready(c));
    bind!("snapshot", |rt, c: &mut ClientHandle| rt.snapshot(*c));
    bind!("disconnect", |rt, c: &mut ClientHandle| rt
        .disconnect(*c, Map::new()));
    bind!("disconnect", |rt, c: &mut ClientHandle, options: Map| rt
        .disconnect(*c, options));
    bind!("save_session", |rt, c: &mut ClientHandle, key: &str| rt
        .save_session(*c, key));
    bind!("restore_session", |rt, c: &mut ClientHandle, key: &str| rt
        .restore_session(*c, key));
    bind!("delete_session", |rt, key: &str| rt.delete_session(key));
    bind!("close_silent", |rt, c: &mut ClientHandle| rt
        .close_silent(*c));
    bind!("assert_eq", |rt, actual: Dynamic, expected: Dynamic| {
        rt.assertions += 1;
        if equal(&actual, &expected) {
            Ok(())
        } else {
            Err(format!(
                "assert_eq failed: actual {}, expected {}",
                json_value(&actual),
                json_value(&expected)
            ))
        }
    });
    bind!("assert_ne", |rt, actual: Dynamic, expected: Dynamic| {
        rt.assertions += 1;
        if !equal(&actual, &expected) {
            Ok(())
        } else {
            Err(format!(
                "assert_ne failed: both values are {}",
                json_value(&actual)
            ))
        }
    });
    bind!("assert", |rt, condition: bool| {
        rt.assertions += 1;
        if condition {
            Ok(())
        } else {
            Err("assertion failed".into())
        }
    });
    bind!("assert", |rt, condition: bool, message: &str| {
        rt.assertions += 1;
        if condition {
            Ok(())
        } else {
            Err(message.into())
        }
    });
    bind!("fail", |rt, message: &str| {
        rt.assertions += 1;
        Err::<(), String>(message.into())
    });
    engine
}

fn millis(value: INT) -> Result<Duration, String> {
    if !(0..=86_400_000).contains(&value) {
        return Err("duration must be 0..86400000 milliseconds".into());
    }
    Ok(Duration::from_millis(value as u64))
}

fn equal(a: &Dynamic, b: &Dynamic) -> bool {
    if a.type_id() != b.type_id() {
        return false;
    }
    if let Some(bytes) = a.clone().try_cast::<Blob>() {
        return b.clone().try_cast::<Blob>() == Some(bytes);
    }
    if let Some(value) = a.clone().try_cast::<StreamHandle>() {
        return b.clone().try_cast::<StreamHandle>() == Some(value);
    }
    if let Some(values) = a.clone().try_cast::<Array>() {
        let other = b.clone().cast::<Array>();
        return values.len() == other.len() && values.iter().zip(&other).all(|(a, b)| equal(a, b));
    }
    if let Some(values) = a.clone().try_cast::<Map>() {
        let other = b.clone().cast::<Map>();
        return values.len() == other.len()
            && values
                .iter()
                .all(|(k, v)| other.get(k).is_some_and(|o| equal(v, o)));
    }
    match (
        rhai::serde::from_dynamic::<serde_json::Value>(a),
        rhai::serde::from_dynamic::<serde_json::Value>(b),
    ) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

fn json_value(value: &Dynamic) -> serde_json::Value {
    if let Some(bytes) = value.clone().try_cast::<Blob>() {
        return serde_json::json!(bytes);
    }
    if let Some(stream) = value.clone().try_cast::<StreamHandle>() {
        return serde_json::json!({
            "client": stream.client, "generation": stream.generation,
            "channel": match stream.channel { connection::Channel::Control => "control", _ => "data" },
            "stream_id": match stream.channel { connection::Channel::Data(id) => Some(id), _ => None },
        });
    }
    if let Some(map) = value.clone().try_cast::<Map>() {
        return serde_json::Value::Object(
            map.iter()
                .map(|(k, v)| (k.to_string(), json_value(v)))
                .collect(),
        );
    }
    if let Some(array) = value.clone().try_cast::<Array>() {
        return serde_json::Value::Array(array.iter().map(json_value).collect());
    }
    rhai::serde::from_dynamic(value)
        .unwrap_or_else(|_| serde_json::Value::String(format!("{value:?}")))
}
