require_dsl(1);
let c = client("main");
expect_ok(c.connect());
ready(c);
let sub = c.open_stream("subscriber");
let sender = c.open_stream("publisher");
assert_eq(expect_ok(c.subscribe(sub, cfg.topic, cfg.sub_qos)).reason_codes, [cfg.sub_qos]);
let inbox = c.watch("message");
expect_ok(c.publish(sender, #{ topic: cfg.topic, payload: cfg.payload, qos: cfg.pub_qos }));
let msg = expect(inbox, #{ stream: sub, topic: cfg.topic }, cfg.timeout_ms);
assert_eq(msg.payload, cfg.payload);
assert_eq(msg.qos, min(cfg.pub_qos, cfg.sub_qos));
assert_eq(msg.retain, false);
expect_none(inbox, #{ topic: cfg.topic }, 100);

