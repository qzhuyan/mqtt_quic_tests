require_dsl(1);
let c = client("main", #{ clean_start: true });
assert_eq(expect_ok(c.connect()).session_present, false);
assert_eq(c.save_session("live").has_session, true);
let disconnects = c.watch("disconnect_received");
let closed = c.watch("disconnected");
let stopped = c.watch("stream_stopped");
let transport_closed = c.watch("transport_closed");
let takeover = client("takeover", #{ client_id: cfg.client_id, clean_start: false });
takeover.restore_session("live");
assert_eq(expect_ok(takeover.connect()).session_present, true);
if cfg.transport == "quic" {
    // EMQX signals takeover by stopping the control stream with the MQTT reason.
    expect(stopped, #{ error_code: 0x8e }, cfg.timeout_ms);
    expect(closed, #{ reason_code: () }, cfg.timeout_ms);
    expect(transport_closed, #{ by_peer: true }, cfg.timeout_ms);
    expect(closed, #{ reason_code: () }, cfg.timeout_ms);
} else {
    expect(disconnects, #{ reason_code: 0x8e }, cfg.timeout_ms);
    expect(transport_closed, #{ by_peer: true }, cfg.timeout_ms);
}
c.close_silent();
takeover.disconnect();

let fresh = client("fresh", #{ clean_start: false });
assert_eq(expect_ok(fresh.connect()).session_present, false);
fresh.disconnect();

let persistent = client("persistent", #{ clean_start: true, session_expiry_interval: 60 });
assert_eq(expect_ok(persistent.connect()).session_present, false);
persistent.disconnect();
let saved = persistent.save_session("persistent");
assert_eq(saved.session_expiry_interval, 60);
let resumed = client("resumed", #{
    client_id: saved.client_id, clean_start: false, session_expiry_interval: 60
});
resumed.restore_session("persistent");
assert_eq(expect_ok(resumed.connect()).session_present, true);
resumed.disconnect();

let stale = client("stale", #{ clean_start: true, session_expiry_interval: 60 });
assert_eq(expect_ok(stale.connect()).session_present, false);
let stale_state = stale.save_session("stale");
stale.close_silent();
let replacement = client("replacement", #{
    client_id: stale_state.client_id, clean_start: false, session_expiry_interval: 60
});
replacement.restore_session("stale");
assert_eq(expect_ok(replacement.connect()).session_present, true);
replacement.disconnect();

let assigned = client("assigned", #{ client_id: "", clean_start: true });
let result = expect_ok(assigned.connect());
let assigned_id = "";
for property in result.properties {
    if "AssignedClientIdentifier" in property { assigned_id = property.AssignedClientIdentifier; }
}
assert_ne(assigned_id, "");
assigned.disconnect();
delete_session("live");
delete_session("persistent");
delete_session("stale");
