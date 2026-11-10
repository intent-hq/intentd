use super::*;

fn link() -> NodeInviteLink {
    NodeInviteLink {
        hosts: vec!["192.168.1.10".into(), "fe80::1%en0".into()],
        port: 5181,
        fingerprint: "AA:BB".into(),
        invite_id: "inv-1".into(),
        secret: "s&=%".into(),
        tc_address: Some("tc123".into()),
    }
}

#[test]
fn a_link_round_trips_and_never_has_a_token() {
    let uri = link().to_uri();
    assert!(uri.starts_with("intent://node?v=1&host=192.168.1.10,fe80::1%25en0&port=5181&fp=AA:BB&invite=inv-1&secret="), "{uri}");
    assert!(uri.ends_with("&tc=tc123"), "{uri}");
    assert!(!uri.contains("token="), "{uri}");
    assert_eq!(NodeInviteLink::parse(&uri), Some(link()));
}

#[test]
fn a_tunnel_only_link_has_an_empty_host() {
    let mut tunnel = link();
    tunnel.hosts.clear();
    let uri = tunnel.to_uri();
    assert!(uri.contains("?v=1&host=&port="), "{uri}");
    assert_eq!(NodeInviteLink::parse(&uri), Some(tunnel));
}

#[test]
fn malformed_links_are_refused() {
    let good = link().to_uri();
    for bad in [
        "intent://pair?v=1&host=1.2.3.4&port=1&fp=AA&token=t".to_owned(),
        good.replace("v=1", "v=2"),
        good.replace("&secret=s%26%3D%25", ""),
        good.replace("&invite=inv-1", "&invite="),
        good.replace("&port=5181", "&port=0"),
        good.replace("&fp=AA:BB", ""),
        format!("{good}&secret=again"),
        "intent://node?v=1&host=&port=1&fp=AA&invite=i&secret=s".to_owned(),
        good.replace("secret=s", "secret=%zz"),
    ] {
        assert_eq!(NodeInviteLink::parse(&bad), None, "{bad}");
    }
}

#[test]
fn debug_never_shows_the_secret() {
    let shown = format!("{:?}", link());
    assert!(!shown.contains("s&=%"), "{shown}");
    let frame = ClaimFrame {
        invite_id: "i".into(),
        secret: "hidden-secret".into(),
        backend_id: "b".into(),
        backend_name: "B".into(),
        backend_fingerprint: None,
        protocol: ProtocolRange { min: 7, max: 17 },
    };
    assert!(!format!("{frame:?}").contains("hidden-secret"));
    let reply = ClaimReply::Claimed {
        credential_id: "c".into(),
        token: "hidden-token".into(),
        machine: ClaimedMachine {
            daemon_id: "d".into(),
            name: "n".into(),
            platform: AgentPlatform::host().unwrap(),
            port: 1,
            hosts: vec![],
            tc_address: None,
            fingerprint: "AA".into(),
        },
        machine_version: "1.0.0".into(),
        protocol: ProtocolRange { min: 7, max: 17 },
    };
    assert!(!format!("{reply:?}").contains("hidden-token"));
}

#[test]
fn claim_frames_are_closed() {
    let frame = r#"{"type":"claim","inviteId":"i","secret":"s","backendId":"b","backendName":"B","protocol":{"min":7,"max":17}}"#;
    let decoded = ClaimFrame::decode(frame).expect("a valid claim");
    assert_eq!(
        ClaimFrame::decode(&decoded.encode().unwrap()),
        Some(decoded)
    );
    for bad in [
        frame.replace(r#""type":"claim""#, r#""type":"other""#),
        frame.replace(r#""type":"claim","#, ""),
        frame.replace(r#""protocol""#, r#""extra":1,"protocol""#),
        frame.replace(r#""secret":"s","#, ""),
    ] {
        assert_eq!(ClaimFrame::decode(&bad), None, "{bad}");
    }
    let refused: ClaimReply =
        serde_json::from_str(r#"{"type":"claimRefused","code":"node-invite-used"}"#).unwrap();
    assert_eq!(refused, ClaimReply::refused(codes::USED));
    assert!(serde_json::from_str::<ClaimReply>(
        r#"{"type":"claimRefused","code":"x","extra":true}"#
    )
    .is_err());
}
