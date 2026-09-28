require_dsl(1);
let c = client("main");
expect_ok(c.connect());
ready(c);
let first = c.open_stream("first");
let second = c.open_stream("second");
let sender = c.open_stream("publisher");
let one = c.subscribe(first, cfg.topic, cfg.sub_qos);
let two = c.subscribe(second, cfg.topic, cfg.sub_qos);
assert_eq(expect_ok(one).reason_codes, [cfg.sub_qos]);
assert_eq(expect_ok(two).reason_codes, [cfg.sub_qos]);
let inbox = c.watch("message");
expect_ok(c.publish(sender, #{ topic: cfg.topic, payload: cfg.payload, qos: cfg.pub_qos }));
for stream in [first, second] {
    let msg = expect(inbox, #{ topic: cfg.topic, stream: stream }, cfg.timeout_ms);
    assert_eq(msg.payload, cfg.payload);
    assert_eq(msg.qos, min(cfg.pub_qos, cfg.sub_qos));
}
expect_none(inbox, #{ topic: cfg.topic }, 250);

