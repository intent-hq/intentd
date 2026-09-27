use super::*;
use crate::WorkspaceApi;

struct OrdinaryApi;
impl WorkspaceApi for OrdinaryApi {}

#[test]
fn existing_api_has_no_repository_read_connection_for_either_entry() {
    let api: &dyn WorkspaceApi = &OrdinaryApi;
    for entry in [
        RepositoryWireEntry::Bearer,
        RepositoryWireEntry::AdmittedLocal,
    ] {
        assert!(api.repository_read_connection(entry).is_none());
    }
}
