//! Ordering/conservation controls under an explicitly fake retained authority.
use super::model::{Fault, Handle, IoClass, Operation, Owner, Phase, TestAuthority};
use std::sync::{Arc, Mutex};

fn ready() -> (Arc<Mutex<TestAuthority>>, Owner, Handle) {
    let backing = TestAuthority::fresh([1; 32]);
    let mut owner = Owner::acquire(backing.clone()).unwrap();
    let op = owner.begin(Operation::Bootstrap, 256).unwrap();
    let io = owner.start_io(&op, IoClass::Open, 128).unwrap();
    owner.complete_io(&io, 128, true).unwrap();
    let handle = owner.opened_handle(&io).unwrap();
    owner.finish(&op).unwrap();
    (backing, owner, handle)
}

#[test]
fn artifact_owner_subleases_credit_without_double_charging_or_borrowing_recovery() {
    let (backing, mut owner, _main_handle) = ready();
    let before = owner.snapshot();
    let normal_free = backing.lock().unwrap().budget - before.0 - before.3 - before.4;
    assert!(owner.begin(Operation::Normal, normal_free + 1).is_err());
    assert_eq!(owner.snapshot(), before);
    let op = owner.begin(Operation::Normal, normal_free).unwrap();
    let reserved = owner.snapshot();
    let io = owner.start_io(&op, IoClass::Overwrite, 100).unwrap();
    let pending = owner.snapshot();
    assert_eq!(reserved.1 + reserved.2, pending.1 + pending.2);
    assert!(owner.start_io(&op, IoClass::Overwrite, 1).is_err());
    assert!(owner.finish(&op).is_err());
    owner.complete_io(&io, 40, false).unwrap();
    let completed = owner.snapshot();
    assert_eq!(completed.0 - before.0, 40);
    assert_eq!(completed.2, normal_free - 40);
    owner.finish(&op).unwrap();
    assert_eq!(owner.snapshot().4, before.4);
    assert!(owner.start_io(&op, IoClass::Overwrite, 1).is_err());
}

#[test]
fn artifact_owner_rejects_late_completion_from_previous_io_in_same_operation() {
    let (_, mut owner, _main_handle) = ready();
    let op = owner.begin(Operation::Normal, 200).unwrap();
    let first = owner.start_io(&op, IoClass::Overwrite, 100).unwrap();
    owner.complete_io(&first, 10, false).unwrap();
    let second = owner.start_io(&op, IoClass::Preallocate, 100).unwrap();
    let pending = owner.snapshot();
    assert!(
        owner.complete_io(&first, 10, false).is_err(),
        "an earlier IO completion settled the next physical IO"
    );
    assert_eq!(owner.snapshot(), pending);
    assert!(owner.finish(&op).is_err());
    owner.complete_io(&second, 20, false).unwrap();
    owner.finish(&op).unwrap();
}

#[test]
fn artifact_owner_overwrite_preallocation_partial_failures_keep_all_credit() {
    for kind in [IoClass::Overwrite, IoClass::Preallocate] {
        let (_, mut owner, _main_handle) = ready();
        let before = owner.snapshot().0;
        let op = owner.begin(Operation::Normal, 200).unwrap();
        let io = owner.start_io(&op, kind, 150).unwrap();
        owner.partial_failure(&io, 70).unwrap();
        let failed = owner.snapshot();
        assert_eq!((failed.0 - before, failed.1, failed.2), (70, 80, 50));
        assert_eq!(failed.5, Phase::Quarantined);
        assert!(owner.finish(&op).is_err());
        assert!(owner.begin(Operation::Normal, 1).is_err());
        assert!(owner.close().is_err());
        owner.settle_failed_io(&io).unwrap();
        owner.finish(&op).unwrap();
        assert_eq!(owner.snapshot().0 - before, 200);
        assert_eq!(owner.snapshot().5, Phase::Quarantined);
    }
}

#[test]
fn artifact_owner_cancelled_anonymous_io_and_failed_close_do_not_refund() {
    let (backing, mut owner, main_handle) = ready();
    let op = owner.begin(Operation::Normal, 200).unwrap();
    let io = owner.start_io(&op, IoClass::Anonymous, 100).unwrap();
    owner.cancel(&op).unwrap();
    assert_eq!(owner.snapshot().1, 100);
    assert!(backing
        .lock()
        .unwrap()
        .confirm_test_reclamation(true)
        .is_err());
    // A late successful IO still belongs to the cancelled owner.
    owner.complete_io(&io, 75, true).unwrap();
    let anonymous = owner.opened_handle(&io).unwrap();
    owner.finish(&op).unwrap();
    let retained = owner.snapshot().0;
    owner.close_handle(&anonymous, false).unwrap();
    assert!(owner.close().is_err());
    owner.close_handle(&anonymous, true).unwrap();
    assert!(owner.close().is_err()); // main handle is still live
    owner.close_handle(&main_handle, true).unwrap();
    owner.close().unwrap();
    assert_eq!(owner.snapshot().0, retained);
    assert!(backing
        .lock()
        .unwrap()
        .confirm_test_reclamation(false)
        .is_err());
    let receipt = backing
        .lock()
        .unwrap()
        .confirm_test_reclamation(true)
        .unwrap();
    owner.retire(&receipt).unwrap();
    assert_eq!(owner.snapshot().0, 0);
    // Bookkeeping/root grant and withheld capacity are not silently returned.
    assert!(owner.snapshot().3 > 0 && owner.snapshot().4 > 0);
    assert_eq!(owner.snapshot().5, Phase::Retired);
}

#[test]
fn artifact_owner_recovery_needs_proved_allowance_not_just_inequality() {
    let (backing, mut owner, _main_handle) = ready();
    let op = owner.begin(Operation::Normal, 100).unwrap();
    owner.cancel(&op).unwrap();
    owner.finish(&op).unwrap();
    assert!(owner.begin(Operation::Recovery, 100).is_err());
    backing.lock().unwrap().recovery_bound = Some(513);
    assert!(owner.begin(Operation::Recovery, 512).is_err());
    assert_eq!(owner.snapshot().5, Phase::Quarantined);
    // A TEST-ONLY bounded recovery plan is supplied by the retained authority.
    backing.lock().unwrap().recovery_bound = Some(100);
    let before = owner.snapshot();
    let recovery = owner.begin(Operation::Recovery, 100).unwrap();
    assert_eq!(owner.snapshot().4, before.4 - 100);
    let io = owner.start_io(&recovery, IoClass::Overwrite, 100).unwrap();
    owner.complete_io(&io, 30, false).unwrap();
    owner.finish(&recovery).unwrap();
    assert_eq!(owner.snapshot().4, before.4);
    assert_eq!(owner.snapshot().5, Phase::Acquired);
    assert!(owner.begin(Operation::Normal, 1).is_err());
}

#[test]
fn artifact_owner_restart_preserves_unsettled_charge_and_invalidates_permits() {
    let (backing, mut owner, _main_handle) = ready();
    let old = owner.begin(Operation::Normal, 200).unwrap();
    let io = owner.start_io(&old, IoClass::Preallocate, 100).unwrap();
    assert!(Owner::acquire(backing.clone()).is_err());
    let before = owner.snapshot();
    drop(owner); // dropping a caller does not terminate the authority's lease
    assert!(Owner::acquire(backing.clone()).is_err());
    backing.lock().unwrap().previous_process_retired();
    let mut reopened = Owner::acquire(backing).unwrap();
    assert_eq!(reopened.snapshot().0, before.0 + before.1 + before.2);
    assert_eq!(reopened.snapshot().5, Phase::Quarantined);
    assert!(reopened.complete_io(&io, 0, false).is_err());
    assert!(reopened.begin(Operation::Normal, 1).is_err());
}

#[test]
fn artifact_owner_checkpoint_faults_preserve_prior_reservations_before_restart() {
    for fault in [Fault::BeforeCheckpoint, Fault::TornCheckpoint] {
        let (backing, mut owner, _main_handle) = ready();
        let op = owner.begin(Operation::Normal, 200).unwrap();
        let io = owner.start_io(&op, IoClass::Overwrite, 150).unwrap();
        let before = owner.snapshot();
        backing.lock().unwrap().fault = Some(fault);
        assert!(owner.complete_io(&io, 80, false).is_err());
        assert_eq!(owner.snapshot().5, Phase::Quarantined);
        drop(owner);
        backing.lock().unwrap().previous_process_retired();
        let reopened = Owner::acquire(backing).unwrap();
        assert_eq!(reopened.snapshot().0, before.0 + before.1 + before.2);
        assert_eq!(reopened.snapshot().5, Phase::Quarantined);
    }
}

#[test]
fn artifact_owner_bootstrap_record_is_precharged_and_arithmetic_checked() {
    let backing = TestAuthority::fresh([2; 32]);
    backing.lock().unwrap().bookkeeping = 1;
    assert!(Owner::acquire(backing).is_err());
    let backing = TestAuthority::fresh([3; 32]);
    backing.lock().unwrap().recovery = u64::MAX;
    assert!(Owner::acquire(backing).is_err());
    let (_, mut owner, _main_handle) = ready();
    let before = owner.snapshot();
    assert!(owner.begin(Operation::Normal, u64::MAX).is_err());
    assert_eq!(owner.snapshot(), before);
}

#[test]
fn artifact_owner_retirement_receipt_cannot_cross_arena_or_epoch() {
    let (backing, mut first, main_handle) = ready();
    first.close_handle(&main_handle, true).unwrap();
    first.close().unwrap();
    let receipt = backing
        .lock()
        .unwrap()
        .confirm_test_reclamation(true)
        .unwrap();
    let other = TestAuthority::fresh([9; 32]);
    let mut second = Owner::acquire(other).unwrap();
    second.close().unwrap();
    assert!(second.retire(&receipt).is_err());
    let receipt = backing
        .lock()
        .unwrap()
        .confirm_test_reclamation(true)
        .unwrap();
    drop(first);
    backing.lock().unwrap().previous_process_retired();
    let mut reopened = Owner::acquire(backing).unwrap();
    reopened.close().unwrap();
    assert!(reopened.retire(&receipt).is_err());
}

#[test]
fn artifact_owner_accounts_existing_backing_before_bootstrap_or_restart() {
    let backing = TestAuthority::fresh([7; 32]);
    backing.lock().unwrap().existing_charge = 3000;
    let mut owner = Owner::acquire(backing.clone()).unwrap();
    assert_eq!(owner.snapshot().0, 3000);
    assert!(owner.begin(Operation::Bootstrap, 201).is_err());
    let op = owner.begin(Operation::Bootstrap, 200).unwrap();
    owner.cancel(&op).unwrap();
    owner.finish(&op).unwrap();
    drop(owner);
    backing.lock().unwrap().previous_process_retired();
    let reopened = Owner::acquire(backing.clone()).unwrap();
    assert_eq!(reopened.snapshot().0, 3200);
    drop(reopened);
    backing.lock().unwrap().previous_process_retired();
    backing.lock().unwrap().existing_charge = 3201;
    assert!(Owner::acquire(backing).is_err());
}

#[test]
fn artifact_owner_permits_cannot_cross_backing_identity() {
    for domain in [[11; 32], [12; 32]] {
        let mut first = Owner::acquire(TestAuthority::fresh([11; 32])).unwrap();
        let mut second = Owner::acquire(TestAuthority::fresh(domain)).unwrap();
        let first_op = first.begin(Operation::Bootstrap, 200).unwrap();
        let second_op = second.begin(Operation::Bootstrap, 200).unwrap();
        let first_io = first.start_io(&first_op, IoClass::Open, 100).unwrap();
        let second_io = second.start_io(&second_op, IoClass::Open, 100).unwrap();
        let before = second.snapshot();
        assert!(
            second.complete_io(&first_io, 100, true).is_err(),
            "cross-arena IO permit admitted"
        );
        assert!(
            second.cancel(&first_op).is_err(),
            "cross-arena operation permit admitted"
        );
        assert_eq!(second.snapshot(), before);
        second.complete_io(&second_io, 100, true).unwrap();
    }
}

#[test]
fn artifact_owner_failed_recovery_remains_quarantined() {
    for cancelled in [false, true] {
        let (backing, mut owner, _main_handle) = ready();
        let op = owner.begin(Operation::Normal, 100).unwrap();
        owner.cancel(&op).unwrap();
        owner.finish(&op).unwrap();
        backing.lock().unwrap().recovery_bound = Some(100);
        let recovery = owner.begin(Operation::Recovery, 100).unwrap();
        let io = owner.start_io(&recovery, IoClass::Overwrite, 100).unwrap();
        if cancelled {
            owner.cancel(&recovery).unwrap();
        } else {
            owner.partial_failure(&io, 20).unwrap();
        }
        owner.settle_failed_io(&io).unwrap();
        owner.finish(&recovery).unwrap();
        assert_eq!(
            owner.snapshot().5,
            Phase::Quarantined,
            "failed recovery reopened arena"
        );
        assert_eq!(
            owner.snapshot().4,
            412,
            "failed recovery replenished withheld credit"
        );
        assert!(owner.begin(Operation::Bootstrap, 1).is_err());
    }
}

#[test]
fn artifact_owner_duplicate_close_cannot_retire_another_live_handle() {
    let (_, mut owner, main_handle) = ready();
    let op = owner.begin(Operation::Normal, 100).unwrap();
    let io = owner.start_io(&op, IoClass::Anonymous, 100).unwrap();
    owner.complete_io(&io, 50, true).unwrap();
    let anonymous = owner.opened_handle(&io).unwrap();
    owner.finish(&op).unwrap();
    owner.close_handle(&main_handle, true).unwrap();
    let before = owner.snapshot();
    assert!(
        owner.close_handle(&main_handle, true).is_err(),
        "replayed close retired the other live handle"
    );
    assert_eq!(owner.snapshot(), before);
    assert!(owner.close().is_err());
    let (_, mut other, _) = ready();
    let other_before = other.snapshot();
    assert!(other.close_handle(&anonymous, true).is_err());
    assert_eq!(other.snapshot(), other_before);
    owner.close_handle(&anonymous, true).unwrap();
    owner.close().unwrap();
}

#[test]
fn artifact_owner_retirement_receipt_requires_retained_authority_instance() {
    let (backing, mut first, first_handle) = ready();
    let (_, mut second, second_handle) = ready();
    first.close_handle(&first_handle, true).unwrap();
    first.close().unwrap();
    second.close_handle(&second_handle, true).unwrap();
    second.close().unwrap();
    let receipt = backing
        .lock()
        .unwrap()
        .confirm_test_reclamation(true)
        .unwrap();
    let before = second.snapshot();
    assert!(
        second.retire(&receipt).is_err(),
        "foreign retirement receipt refunded retained allocation"
    );
    assert_eq!(second.snapshot(), before);
}
