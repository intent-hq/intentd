//! Prepared allocation ownership, deliberately without a production backing.
//! The fixture model proves ordering/conservation, not physical byte coverage.
use super::{Error, Result};

#[cfg(test)]
mod model;
#[cfg(test)]
mod tests;

#[cfg(test)]
type PendingOpen = std::sync::Arc<std::sync::Mutex<Option<(model::Permit, model::IoPermit)>>>;

#[derive(Clone)]
pub(super) struct OpenAdmission {
    #[cfg(not(test))]
    unavailable: std::convert::Infallible,
    #[cfg(test)]
    handle: std::sync::Arc<std::sync::Mutex<Option<model::Handle>>>,
    #[cfg(test)]
    owner: std::sync::Arc<std::sync::Mutex<model::Owner>>,
    #[cfg(test)]
    pending: PendingOpen,
}

impl OpenAdmission {
    pub(super) fn production() -> Result<Self> {
        // No available allocator proves metadata/overwrite/recovery coverage.
        // Neither a page cap nor a client/fixture token can create this authority.
        Err(Error::Internal(
            "Artifact allocation backing authority is unavailable".into(),
        ))
    }

    // This fresh per-fixture authority proves only boundary ordering. Reopening
    // this SQLite fixture or dropping a failed initializer does not preserve its
    // modeled allocation ownership. Retained-authority restart controls instead
    // reuse the same TestAuthority explicitly in model tests. Neither is a
    // production allocation service or filesystem durability demonstration.
    #[cfg(test)]
    pub(super) fn test_only(path: &std::path::Path) -> Self {
        use sha2::{Digest, Sha256};
        let domain: [u8; 32] = Sha256::digest(path.as_os_str().as_encoded_bytes()).into();
        let authority = model::TestAuthority::fresh(domain);
        let owner = model::Owner::acquire(authority).unwrap();
        Self {
            owner: std::sync::Arc::new(std::sync::Mutex::new(owner)),
            pending: std::sync::Arc::default(),
            handle: std::sync::Arc::default(),
        }
    }

    pub(super) fn before_open(&self) -> Result<()> {
        #[cfg(test)]
        {
            let mut owner = self.owner.lock().unwrap();
            // Explicit model credits only. This does not meter the following
            // SQLite file allocations or make fixture backing production-ready.
            let op = owner.begin(model::Operation::Bootstrap, 256)?;
            let io = owner.start_io(&op, model::IoClass::Open, 256)?;
            *self.pending.lock().unwrap() = Some((op, io));
            Ok(())
        }
        #[cfg(not(test))]
        {
            match self.unavailable {}
        }
    }

    pub(super) fn opened(&self) -> Result<()> {
        #[cfg(test)]
        {
            let mut owner = self.owner.lock().unwrap();
            let (op, io) = self
                .pending
                .lock()
                .unwrap()
                .take()
                .ok_or_else(|| Error::Internal("Missing fixture open permit".into()))?;
            owner.complete_io(&io, 256, true)?;
            *self.handle.lock().unwrap() = Some(owner.opened_handle(&io)?);
            owner.finish(&op)?;
            Ok(())
        }
        #[cfg(not(test))]
        {
            match self.unavailable {}
        }
    }

    pub(super) fn closed(&self) {
        #[cfg(not(test))]
        match self.unavailable {}
        #[cfg(test)]
        {
            let mut owner = self.owner.lock().unwrap();
            if matches!(
                owner.snapshot().5,
                model::Phase::Closed | model::Phase::Retired
            ) {
                return;
            }
            // Closure of the actual pool settles the fixture handle. It does
            // not issue a physical retirement receipt or refund retained charge.
            let handle = self.handle.lock().unwrap().take().unwrap();
            owner.close_handle(&handle, true).unwrap();
            owner.close().unwrap();
        }
    }
}
