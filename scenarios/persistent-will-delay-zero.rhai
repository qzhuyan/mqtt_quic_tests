require_dsl(1);
let observer = client("observer", #{ session_expiry_interval: 0, will: () });
expect_ok(observer.connect());
assert_eq(expect_ok(observer.subscribe(cfg.topic, 2)).reason_codes, [2]);
let inbox = observer.watch("message");
let c = client("main", #{
    clean_start: true, session_expiry_interval: 1,
    will: #{ topic: cfg.topic, payload: bytes("will message"), qos: 1, delay_interval: 0 }
});
expect_ok(c.connect());
expect_none(inbox, #{ topic: cfg.topic }, 100);
c.disconnect(#{ reason_code: 0x80 });
let message = expect(inbox, #{ topic: cfg.topic }, 10000);
assert_eq(message.payload, bytes("will message"));
assert_eq(message.qos, 1);
assert_eq(message.retain, false);
expect_none(inbox, #{ topic: cfg.topic }, 100);
observer.disconnect();
