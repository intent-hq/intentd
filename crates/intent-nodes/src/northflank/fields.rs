//! The field names a `#[derive(Deserialize)]` struct reads, as serde names
//! them (after `rename_all`), so the contract test can check the fixtures
//! cover every field intentd reads without a hand-kept list.

use std::cell::RefCell;

use serde::de::{self, Deserialize, Visitor};

#[derive(Debug)]
struct Stop;

impl std::fmt::Display for Stop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("fields recorded")
    }
}

impl std::error::Error for Stop {}

impl de::Error for Stop {
    fn custom<T: std::fmt::Display>(_: T) -> Self {
        Self
    }
}

struct Recorder<'a>(&'a RefCell<&'static [&'static str]>);

impl<'de> de::Deserializer<'de> for Recorder<'_> {
    type Error = Stop;

    fn deserialize_any<V: Visitor<'de>>(self, _: V) -> Result<V::Value, Stop> {
        Err(Stop)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _: &'static str,
        fields: &'static [&'static str],
        _: V,
    ) -> Result<V::Value, Stop> {
        *self.0.borrow_mut() = fields;
        Err(Stop)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map enum identifier ignored_any
    }
}

/// The fields `T` reads.
pub(super) fn fields_of<'de, T: Deserialize<'de>>() -> &'static [&'static str] {
    let fields = RefCell::new(&[][..]);
    let _ = T::deserialize(Recorder(&fields));
    fields.into_inner()
}

/// Fields intentd reads of one route's responses.
pub(super) struct Read {
    /// `METHOD /path`, `*` for any one segment, without the query.
    pub route: &'static str,
    /// JSON pointer of the items read, from the body's `data` (or from the
    /// body itself with `envelope`); `*` walks every array element.
    pub at: &'static str,
    pub envelope: bool,
    /// Compare objects and arrays whole; else only their JSON type, as a
    /// serde struct's nested items have their own [`Read`].
    pub exact: bool,
    /// JSON pointers into each item.
    pub fields: Vec<String>,
}

impl Read {
    pub fn serde(route: &'static str, at: &'static str, fields: &[&str]) -> Self {
        Self {
            route,
            at,
            envelope: false,
            exact: false,
            fields: fields.iter().map(|f| format!("/{f}")).collect(),
        }
    }

    pub fn pointers(route: &'static str, at: &'static str, pointers: &[&str]) -> Self {
        Self {
            route,
            at,
            envelope: false,
            exact: true,
            fields: pointers.iter().map(|p| (*p).to_owned()).collect(),
        }
    }

    /// Fields of the envelope's `pagination`.
    pub fn envelope(route: &'static str, fields: &[&str]) -> Self {
        Self {
            route,
            at: "/pagination",
            envelope: true,
            exact: false,
            fields: fields.iter().map(|f| format!("/{f}")).collect(),
        }
    }
}
