require_dsl(1);
let c = client("main");
expect_ok(c.connect());
ready(c);
poll_for(cfg.hold_ms);
c.disconnect();

