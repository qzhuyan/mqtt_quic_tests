require_dsl(1);
let c = client("main", #{ clean_start: true, session_expiry_interval: 60, auto_ack: false });
assert_eq(expect_ok(c.connect()).session_present, false);
assert_eq(expect_ok(c.subscribe(c.control_stream(), cfg.topic, 2)).reason_codes, [2]);
let messages = c.watch("message");
let releases = c.watch("pubrel");
let publisher = client("publisher", #{ clean_start: true, session_expiry_interval: 0 });
expect_ok(publisher.connect());
expect_ok(publisher.publish(#{ topic: cfg.topic, payload: cfg.payload, qos: 2 }));
let original = expect(messages, #{ topic: cfg.topic }, cfg.timeout_ms);
assert_eq(original.payload, cfg.payload);
assert_eq(original.qos, 2);
c.pubrec(original);
expect(releases, #{ packet_id: original.packet_id }, cfg.timeout_ms);
c.save_session("subscriber");
c.close_silent();

let resumed = client("resumed", #{
    client_id: cfg.client_id, clean_start: false, session_expiry_interval: 60, auto_ack: false
});
resumed.restore_session("subscriber");
let replay = resumed.watch("pubrel");
let deliveries = resumed.watch("message");
assert_eq(expect_ok(resumed.connect()).session_present, true);
let release = expect(replay, #{ packet_id: original.packet_id }, cfg.timeout_ms);
resumed.pubcomp(release);
expect_none(deliveries, #{ topic: cfg.topic }, 100);
expect_none(replay, #{ packet_id: original.packet_id }, 100);
resumed.disconnect();
publisher.disconnect();
delete_session("subscriber");
