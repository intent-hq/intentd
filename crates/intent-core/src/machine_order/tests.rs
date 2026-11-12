use super::*;

fn machine(id: &str) -> MachineOrderKey {
    MachineOrderKey::Machine {
        node_id: id.to_owned(),
    }
}

fn cloud(id: &str) -> MachineOrderKey {
    MachineOrderKey::Cloud {
        cloud_machine_id: id.to_owned(),
    }
}

fn rows(local: bool) -> MachineOrderRows {
    MachineOrderRows {
        local: local.then(|| "Laptop".to_owned()),
        machines: vec![
            ("m-a".into(), "Alpha".into()),
            ("m-b".into(), "Beta".into()),
        ],
        entries: vec![("e2b".into(), "E2B".into())],
    }
}

fn keys(list: &[MachineOrderRow]) -> Vec<MachineOrderKey> {
    list.iter().map(|row| row.key.clone()).collect()
}

#[test]
fn default_order_keeps_this_computer_first_then_machines_then_entries() {
    let list = ordered(&[], &rows(true));
    assert_eq!(
        keys(&list),
        vec![
            MachineOrderKey::Local,
            machine("m-a"),
            machine("m-b"),
            cloud("e2b")
        ]
    );
    assert_eq!(list[0].name, "Laptop");
}

#[test]
fn saved_order_wins_drops_removed_rows_and_appends_new_ones() {
    let saved = vec![
        cloud("e2b"),
        machine("m-gone"),
        machine("m-b"),
        MachineOrderKey::Local,
    ];
    let list = ordered(&saved, &rows(true));
    assert_eq!(
        keys(&list),
        vec![
            cloud("e2b"),
            machine("m-b"),
            MachineOrderKey::Local,
            machine("m-a")
        ]
    );
}

#[test]
fn a_missing_local_row_goes_first_and_an_orchestrator_has_none() {
    let saved = vec![machine("m-b"), machine("m-a"), cloud("e2b")];
    assert_eq!(
        keys(&ordered(&saved, &rows(true)))[0],
        MachineOrderKey::Local
    );
    let with_local = vec![MachineOrderKey::Local, machine("m-b")];
    let list = ordered(&with_local, &rows(false));
    assert_eq!(
        keys(&list),
        vec![machine("m-b"), machine("m-a"), cloud("e2b")]
    );
}

#[test]
fn names_follow_the_live_rows() {
    let mut live = rows(true);
    live.machines[0].1 = "Renamed".into();
    let list = ordered(&[machine("m-a")], &live);
    assert_eq!(list[1].name, "Renamed");
}

#[test]
fn set_order_must_name_exactly_the_current_rows() {
    let current = ordered(&[], &rows(true));
    let exact = vec![
        cloud("e2b"),
        machine("m-b"),
        machine("m-a"),
        MachineOrderKey::Local,
    ];
    assert!(names_exactly(&exact, &current));
    let missing = vec![cloud("e2b"), machine("m-b"), machine("m-a")];
    assert!(!names_exactly(&missing, &current));
    let duplicate = vec![
        cloud("e2b"),
        machine("m-b"),
        machine("m-b"),
        MachineOrderKey::Local,
    ];
    assert!(!names_exactly(&duplicate, &current));
    let unknown = vec![
        cloud("e2b"),
        machine("m-b"),
        machine("m-x"),
        MachineOrderKey::Local,
    ];
    assert!(!names_exactly(&unknown, &current));
    let orchestrator = ordered(&[], &rows(false));
    let with_local = vec![MachineOrderKey::Local, machine("m-a"), machine("m-b")];
    assert!(!names_exactly(&with_local, &orchestrator));
}

#[test]
fn keys_use_the_documented_wire_shape() {
    let parsed: Vec<MachineOrderKey> = serde_json::from_str(
        r#"[{"kind":"local"},{"kind":"machine","nodeId":"m-a"},{"kind":"cloud","cloudMachineId":"e2b"}]"#,
    )
    .unwrap();
    assert_eq!(
        parsed,
        vec![MachineOrderKey::Local, machine("m-a"), cloud("e2b")]
    );
    let row = serde_json::to_value(&ordered(&[], &rows(true))[1]).unwrap();
    assert_eq!(
        row,
        serde_json::json!({ "kind": "machine", "nodeId": "m-a", "name": "Alpha" })
    );
    assert!(serde_json::from_str::<MachineOrderKey>(r#"{"kind":"machine"}"#).is_err());
    assert!(parse_saved(Some("not json")).is_empty());
}
