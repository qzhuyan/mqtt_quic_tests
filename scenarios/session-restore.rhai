require_dsl(1);
let c = client("main");
assert_eq(c.restore_session("main").has_session, true);
assert_eq(expect_ok(c.connect()).session_present, true);
// Read-only checkpoint use permits the CT test's successive quiescent takeovers.
ready(c);
poll_for(cfg.hold_ms);
c.disconnect();
