require_dsl(1);
let c = client("main", #{ clean_start: true, session_expiry_interval: 60, auto_ack: false });
assert_eq(expect_ok(c.connect()).session_present, false);
assert_eq(expect_ok(c.subscribe(c.control_stream(), cfg.topic, 1)).reason_codes, [1]);
let messages = c.watch("message");
let publisher = client("publisher", #{ clean_start: true, session_expiry_interval: 0 });
expect_ok(publisher.connect());
expect_ok(publisher.publish(#{ topic: cfg.topic, payload: cfg.payload, qos: 1 }));
let original = expect(messages, #{ topic: cfg.topic }, cfg.timeout_ms);
assert_eq(original.payload, cfg.payload);
assert_eq(original.qos, 1);
c.save_session("subscriber");
c.close_silent();

let resumed = client("resumed", #{
    client_id: cfg.client_id, clean_start: false, session_expiry_interval: 60, auto_ack: false
});
resumed.restore_session("subscriber");
let replay = resumed.watch("message");
assert_eq(expect_ok(resumed.connect()).session_present, true);
let message = expect(replay, #{ topic: cfg.topic }, cfg.timeout_ms);
assert_eq(message.payload, original.payload);
assert_eq(message.packet_id, original.packet_id);
assert_eq(message.qos, 1);
assert_eq(message.dup, true);
resumed.puback(message);
expect_none(replay, #{ topic: cfg.topic }, 100);
resumed.disconnect();
publisher.disconnect();
delete_session("subscriber");
