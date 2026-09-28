use mqtt_quic_tests::dsl::{self, ScriptOptions};
use mqtt_quic_tests::RunConfig;

fn script(source: &str) -> dsl::ScriptReport {
    dsl::run_source(
        "test.rhai",
        source,
        RunConfig::default(),
        ScriptOptions::default(),
    )
}

#[test]
fn bundled_scenarios_compile() {
    for name in dsl::bundled_names() {
        dsl::check_source(name, dsl::bundled(name).unwrap()).unwrap();
    }
    for name in [
        "connect",
        "pubsub",
        "multistream",
        "unsubscribe",
        "duplicate-subscribe",
        "persistent-no-will",
        "persistent-will-delay-equal",
        "persistent-will-delay-zero",
        "persistent-will-delay-expiry",
        "mqtt-v5-session",
        "session-checkpoint",
        "session-restore",
        "session-store-qos1",
        "session-store-qos2",
    ] {
        assert!(dsl::bundled(name).is_some(), "missing {name}");
    }
    dsl::check_source(
        "two-clients.rhai",
        include_str!("../examples/two-clients.rhai"),
    )
    .unwrap();
}

#[test]
fn assertions_compare_bytes_maps_and_arrays() {
    let report = script(
        r#"
        require_dsl(1);
        let data = blob(3, 0);
        data[1] = 255;
        data[2] = 1;
        assert_eq(hex("00 ff 01"), data);
        assert_eq(bytes("hello"), bytes("hello"));
        assert_eq(#{ a: [1, true, "x"] }, #{ a: [1, true, "x"] });
        assert_ne(bytes("hello"), bytes("world"));
        assert(text(bytes("hello")) == "hello");
    "#,
    );
    assert_eq!(report.status, "passed", "{:?}", report.error);
    assert_eq!(report.assertions, 5);
}

#[test]
fn failure_reports_actual_expected_and_source_line() {
    let report = script("require_dsl(1);\nassert_eq(1, 2);");
    assert_eq!(report.exit_code(), 1);
    let error = report.error.unwrap();
    assert!(error.contains("test.rhai"), "{error}");
    assert!(error.contains("line 2"), "{error}");
    assert!(error.contains("actual 1, expected 2"), "{error}");
}

#[test]
fn catching_an_assertion_does_not_turn_it_into_success() {
    let report = script("try { assert(false); } catch (err) { } assert(true);");
    assert_eq!(report.status, "failed");
}

#[test]
fn empty_script_is_not_a_passing_test() {
    let report = script("let x = 1;");
    assert!(report.error.unwrap().contains("no assertions"));
}

#[test]
fn client_reports_keep_the_external_script_name() {
    let report = script(r#"let c = client("main"); assert(true);"#);
    assert_eq!(report.clients["main"]["scenario"], "test.rhai");
    assert_eq!(report.summary.scenario, "test.rhai");
}

#[test]
fn syntax_and_unknown_configuration_are_errors() {
    assert_eq!(script("let = ;").exit_code(), 2);
    assert!(script("require_dsl(99);")
        .error
        .unwrap()
        .contains("unsupported DSL"));
    assert!(script(r#"client("main", #{ atuo_ack: false });"#)
        .error
        .unwrap()
        .contains("atuo_ack"));
    let error = script("assert_eq(cfg.typo, 1);").error.unwrap();
    assert!(error.contains("typo"), "{error}");
}

#[test]
fn hex_rejects_odd_digits_after_removing_separators() {
    let report = script(r#"assert_eq(hex("00: f"), bytes("x"));"#);
    assert_eq!(report.status, "failed");
    assert!(report
        .error
        .unwrap()
        .contains("even number of ASCII digits"));
}

#[test]
fn malformed_hex_fails_without_panicking_on_unicode() {
    assert_eq!(
        script(r#"assert_eq(hex("é"), bytes("x"));"#).status,
        "failed"
    );
}

#[test]
fn pure_script_loop_obeys_the_scenario_deadline() {
    let report = dsl::run_source(
        "loop.rhai",
        "loop { }",
        RunConfig::default(),
        ScriptOptions {
            scenario_timeout: std::time::Duration::from_millis(10),
            ..Default::default()
        },
    );
    assert_eq!(report.status, "failed");
}

#[test]
fn tcp_and_quic_scripts_use_the_same_configuration() {
    let report = dsl::run_source(
        "tcp.rhai",
        r#"assert_eq(cfg.transport, "tcp");"#,
        RunConfig::default(),
        ScriptOptions {
            transport: dsl::Transport::Tcp,
            ..Default::default()
        },
    );
    assert_eq!(report.status, "passed", "{:?}", report.error);
}

#[test]
fn missing_checkpoint_is_a_failure_not_a_fresh_connection() {
    let report = script(
        r#"
        let c = client("main", #{ clean_start: false });
        c.restore_session("missing");
        assert(true);
    "#,
    );
    assert_eq!(report.status, "failed");
    assert!(report.error.unwrap().contains("does not exist"));
}
