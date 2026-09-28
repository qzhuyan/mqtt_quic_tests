require_dsl(1);
let c = client("main");
assert_eq(expect_ok(c.connect()).session_present, false);
assert_eq(c.save_session("main").has_session, true);
ready(c);
poll_for(cfg.hold_ms);
c.disconnect();
c.save_session("main");
