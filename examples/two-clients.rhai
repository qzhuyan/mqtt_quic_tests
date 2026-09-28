require_dsl(1);
let receiver = client("main", #{ auto_ack: false });
let publisher = client("publisher");
let receiver_connect = receiver.connect();
let publisher_connect = publisher.connect();
expect_ok(receiver_connect);
expect_ok(publisher_connect);

let channel = receiver.control_stream();
assert_eq(expect_ok(receiver.subscribe(channel, cfg.topic, 2)).reason_codes, [2]);
let messages = receiver.watch("message");
let releases = receiver.watch("pubrel");
for index in 0..4 {
    let payload = bytes("message-" + index.to_string());
    let sent = publisher.publish(#{ topic: cfg.topic, payload: payload, qos: 2 });
    let message = expect(messages, #{ topic: cfg.topic, stream: channel }, cfg.timeout_ms);
    assert_eq(message.payload, payload);
    assert_eq(message.qos, 2);
    receiver.pubrec(message);
    let release = expect(releases, #{
        packet_id: message.packet_id, stream: message.stream
    }, cfg.timeout_ms);
    receiver.pubcomp(release);
    expect_ok(sent);
}
expect_none(messages, #{ topic: cfg.topic }, 100);
receiver.disconnect();
publisher.disconnect();

