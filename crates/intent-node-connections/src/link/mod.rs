//! The link halves of the remote feature: state and code the machine's
//! connector holds. The domain halves in `intent-svc-remote` find homes and
//! machines and call in here.

pub mod checkout;
pub mod forge_token;
pub mod gate;
pub mod home_scripts;
pub mod home_state;
pub mod homes;
pub mod terminals;
