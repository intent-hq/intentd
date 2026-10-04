//! Prepared lifecycle ownership. No shipping route or source capability registration.
pub mod clock;
pub mod metadata;

mod connection;
mod delivery;
mod owner;
pub(crate) mod registry;
pub use connection::{Open, SourceConnection};
pub use delivery::{Delivery, SourceWriter};
fn map_error(error: crate::Error) -> intent_core::note_source_session::SessionError {
    use intent_core::{
        note_page::NotePageError as P, note_source_session::SessionError as S, Error,
    };
    match error {
        Error::Internal(_) => S::Uncertain,
        Error::NotFound(_) | Error::Forbidden(_) => S::NotFound,
        Error::NotePage(P::Expired) => S::Expired,
        Error::NotePage(P::Stale) => S::Stale,
        Error::NotePage(P::Budget) => S::Budget,
        _ => S::Unavailable,
    }
}

#[cfg(test)]
mod tests;
