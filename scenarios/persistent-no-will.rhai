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
c.disconnect();
expect_none(inbox, #{ topic: cfg.topic }, 5000);
assert_eq(observer.snapshot().connected, true);
observer.disconnect();
