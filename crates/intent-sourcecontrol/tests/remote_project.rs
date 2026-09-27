//! Offline fixtures: configured bindings are inputs, never discovered credentials.
use intent_sourcecontrol::remote_project::{
    CanonicalRemoteProject, CanonicalRemoteResolver, RemoteInstance, RemoteProvider,
    RemoteTransportMapping, UnresolvedRemote,
};
use intent_sourcecontrol::GitlabInstance;

fn gitlab(root: &str) -> RemoteInstance {
    RemoteInstance::gitlab(GitlabInstance::parse(root).unwrap())
}

fn resolver(instance: &RemoteInstance, roots: &[&str]) -> CanonicalRemoteResolver {
    CanonicalRemoteResolver::new(
        vec![instance.clone()],
        roots
            .iter()
            .map(|root| RemoteTransportMapping::new(instance.clone(), root).unwrap())
            .collect(),
    )
    .unwrap()
}

fn target(root: &str, project: &str) -> CanonicalRemoteProject {
    CanonicalRemoteProject {
        provider: RemoteProvider::Gitlab,
        instance_base_url: root.into(),
        project_path: project.into(),
    }
}

#[test]
fn public_github_catalog_resolves_standard_transports_to_one_identity() {
    let r = resolver(&RemoteInstance::github_com(), &[]);
    let expected = CanonicalRemoteProject {
        provider: RemoteProvider::Github,
        instance_base_url: "https://github.com".into(),
        project_path: "owner/repo".into(),
    };
    for input in [
        "https://GitHub.com/Owner/Repo.git",
        "HTTPS://GITHUB.COM:443/Owner/Repo.GiT/",
        "https://www.github.com/owner/repo",
        "http://github.com:80/OWNER/REPO.git",
        "http://www.github.com/owner/repo",
        "ssh://git@github.com/Owner/Repo.git",
        "ssh://git@GitHub.com:22/Owner/Repo.git",
        "git@github.com:Owner/Repo.git",
        "ssh://git@ssh.github.com:443/owner/repo.git",
    ] {
        assert_eq!(r.resolve(input).unwrap(), expected, "{input}");
    }
}

#[test]
fn github_catalog_does_not_guess_ports_hosts_users_or_nested_projects() {
    let r = resolver(&RemoteInstance::github_com(), &[]);
    for input in [
        "https://github.com:8443/owner/repo.git",
        "https://github.com.evil.test/owner/repo.git",
        "https://github.com./owner/repo.git",
        "ssh://git@github.com:443/owner/repo.git",
        "ssh://git@ssh.github.com:22/owner/repo.git",
        "git@ssh.github.com:owner/repo.git",
        "ssh://another@github.com/owner/repo.git",
        "github.com:owner/repo.git",
        "git@work:owner/repo.git",
    ] {
        assert_eq!(
            r.resolve(input),
            Err(UnresolvedRemote::UnknownInstance),
            "{input}"
        );
    }
    assert_eq!(
        r.resolve("https://github.com/owner/sub/repo.git"),
        Err(UnresolvedRemote::InvalidRemote)
    );
}

#[test]
fn gitlab_https_preserves_full_instance_nested_project_and_path_case() {
    let i = gitlab("https://GIT.Example:8443/Forge/");
    let r = resolver(&i, &[]);
    for input in [
        "https://git.example:8443/Forge/Team/Sub/App.git",
        "https://GIT.EXAMPLE:8443/Forge/Team/Sub/App/",
    ] {
        assert_eq!(
            r.resolve(input).unwrap(),
            target("https://git.example:8443/Forge", "Team/Sub/App")
        );
    }
    assert_ne!(
        r.resolve("https://git.example:8443/Forge/Team/Sub/App")
            .unwrap(),
        r.resolve("https://git.example:8443/Forge/team/sub/app")
            .unwrap()
    );
}

#[test]
fn gitlab_requires_explicit_http_and_ssh_mappings() {
    let i = gitlab("https://git.example:8443/forge");
    let urls = [
        "http://git.example:8080/forge/team/sub/app.git",
        "ssh://git@git.example:2222/repos/team/sub/app.git",
        "git@git.example:team/sub/app.git",
    ];
    let unbound = resolver(&i, &[]);
    let bound = resolver(
        &i,
        &[
            "http://git.example:8080/forge/",
            "ssh://git@git.example:2222/repos/",
            "git@git.example:",
        ],
    );
    for url in urls {
        assert_eq!(unbound.resolve(url), Err(UnresolvedRemote::UnknownInstance));
        assert_eq!(
            bound.resolve(url).unwrap(),
            target("https://git.example:8443/forge", "team/sub/app")
        );
    }
}

#[test]
fn logical_port_prefix_and_transport_prefix_are_not_interchangeable() {
    let i = gitlab("https://git.example:8443/Forge");
    let r = resolver(&i, &["ssh://git@work:2222/repos/"]);
    for url in [
        "https://git.example/Forge/team/app",
        "https://git.example:8443/forge/team/app",
        "https://git.example:8443/ForgeElse/team/app",
        "http://git.example:8443/Forge/team/app",
        "ssh://git@work/repos/team/app",
        "ssh://git@work:2222/repotwo/team/app",
        "ssh://git@work:2222/reposElse/team/app",
        "ssh://user@work:2222/repos/team/app",
        "ssh://work:2222/repos/team/app",
        "git@work:repos/team/app",
    ] {
        assert_eq!(
            r.resolve(url),
            Err(UnresolvedRemote::UnknownInstance),
            "{url}"
        );
    }
}

#[test]
fn ssh_scp_relative_and_absolute_paths_require_distinct_bindings() {
    let i = gitlab("git.example");
    let relative = resolver(&i, &["git@work:repos/"]);
    let absolute = resolver(&i, &["git@work:/repos/"]);
    let ssh = resolver(&i, &["ssh://git@work/repos/"]);
    let urls = [
        "git@work:repos/team/app.git",
        "git@work:/repos/team/app.git",
        "ssh://git@work/repos/team/app.git",
    ];
    for (index, r) in [relative, absolute, ssh].iter().enumerate() {
        for (other, url) in urls.iter().enumerate() {
            if index == other {
                assert_eq!(
                    r.resolve(url).unwrap(),
                    target("https://git.example", "team/app")
                );
            } else {
                assert_eq!(r.resolve(url), Err(UnresolvedRemote::UnknownInstance));
            }
        }
    }
}

#[test]
fn aliases_and_duplicate_bindings_collapse_only_equal_canonical_targets() {
    let i = gitlab("https://git.example:8443/forge");
    let r = resolver(
        &i,
        &["https://clone.example/git/", "git@work:", "git@work:"],
    );
    for url in [
        "https://clone.example/git/team/sub/app.git",
        "git@work:team/sub/app.git",
    ] {
        assert_eq!(
            r.resolve(url).unwrap(),
            target("https://git.example:8443/forge", "team/sub/app")
        );
    }
}

#[test]
fn conflicting_explicit_mappings_never_choose_first_or_last() {
    let a = gitlab("https://git.example/one");
    let b = gitlab("https://git.example/two");
    let mappings = vec![
        RemoteTransportMapping::new(a.clone(), "git@work:").unwrap(),
        RemoteTransportMapping::new(b.clone(), "git@work:").unwrap(),
    ];
    for mappings in [mappings.clone(), mappings.into_iter().rev().collect()] {
        let r = CanonicalRemoteResolver::new(vec![a.clone(), b.clone()], mappings).unwrap();
        assert_eq!(
            r.resolve("git@work:team/app.git"),
            Err(UnresolvedRemote::AmbiguousMapping)
        );
    }
}

#[test]
fn overlapping_logical_roots_do_not_guess_the_installation_boundary() {
    let r = CanonicalRemoteResolver::new(
        vec![gitlab("git.example"), gitlab("https://git.example/forge")],
        vec![],
    )
    .unwrap();
    assert_eq!(
        r.resolve("https://git.example/forge/team/app.git"),
        Err(UnresolvedRemote::AmbiguousMapping)
    );
    assert_eq!(
        r.resolve("https://git.example/team/app.git").unwrap(),
        target("https://git.example", "team/app")
    );
}

#[test]
fn aliases_cannot_introduce_an_unregistered_instance() {
    let mapping = RemoteTransportMapping::new(gitlab("private.example"), "git@work:").unwrap();
    assert!(matches!(
        CanonicalRemoteResolver::new(vec![], vec![mapping]),
        Err(UnresolvedRemote::UnknownInstance)
    ));
    let empty = CanonicalRemoteResolver::new(vec![], vec![]).unwrap();
    assert_eq!(
        empty.resolve("https://github.com/team/app.git"),
        Err(UnresolvedRemote::UnknownInstance)
    );
}

#[test]
fn same_project_names_on_distinct_ports_prefixes_and_providers_stay_distinct() {
    let r = CanonicalRemoteResolver::new(
        vec![
            RemoteInstance::github_com(),
            gitlab("https://git.example:8443/one"),
            gitlab("https://git.example:9443/one"),
            gitlab("https://git.example:8443/two"),
        ],
        vec![],
    )
    .unwrap();
    let mut results = Vec::new();
    for url in [
        "https://github.com/team/app.git",
        "https://git.example:8443/one/team/app.git",
        "https://git.example:9443/one/team/app.git",
        "https://git.example:8443/two/team/app.git",
    ] {
        let result = r.resolve(url).unwrap();
        assert!(!results.contains(&result));
        results.push(result);
    }
}

#[test]
fn malformed_paths_and_credentials_cannot_be_normalized_into_a_target() {
    let r = resolver(&RemoteInstance::github_com(), &[]);
    for input in [
        "",
        " https://github.com/team/app.git",
        "https://github.com/team/app.git\n",
        "https://github.com/team/../team/app.git",
        "https://github.com/./team/app.git",
        "https://github.com//team/app.git",
        "https://github.com/team/app.git//",
        "https://github.com/team/%61pp.git",
        "https://github.com/team/%2e%2e/app.git",
        "https://github.com/team%2fapp.git",
        "https://github.com/team/app.git?access_token=canary",
        "https://github.com/team/app.git#canary",
        "https://github.com/team/.git",
        "https://github.com/team/app\\.git",
        "https://github.com:/team/app.git",
        "https://github.com:0/team/app.git",
        "https://github.com:65536/team/app.git",
        "https://user@evil.test@github.com/team/app.git",
        "ssh://git:canary@github.com/team/app.git",
        "https://github.com/team/-/app.git",
    ] {
        assert_eq!(
            r.resolve(input),
            Err(UnresolvedRemote::InvalidRemote),
            "{input}"
        );
    }
}

#[test]
fn unsupported_local_and_remote_helper_forms_remain_unresolved() {
    let r = resolver(&RemoteInstance::github_com(), &[]);
    for input in [
        "file:///tmp/owner/repo.git",
        "/tmp/a@github.com:owner/repo.git",
        "./github.com:owner/repo.git",
        "../github.com:owner/repo.git",
        "C:/owner/repo.git",
        "git://github.com/owner/repo.git",
        "ftp://github.com/owner/repo.git",
        "ext::ssh canary",
        "owner/repo.git",
    ] {
        assert!(r.resolve(input).is_err(), "{input}");
    }
}

#[test]
fn credential_bearing_remote_results_and_errors_never_expose_userinfo() {
    let r = resolver(&RemoteInstance::github_com(), &[]);
    let result = r
        .resolve("https://canary-user:canary-token@github.com/team/app.git")
        .unwrap();
    assert_eq!(result.project_path, "team/app");
    assert_eq!(result.instance_base_url, "https://github.com");
    assert!(!format!("{result:?}").contains("canary"));
    for input in [
        "https://canary-user:canary-token@foreign.example/team/app.git",
        "https://github.com/a@canary:github.com/team/app.git",
        "https://github.com/team/app.git?token=canary",
        "ssh://git:canary@github.com/team/app.git",
    ] {
        let err = r.resolve(input).unwrap_err();
        assert!(!format!("{err:?} {err}").contains("canary"));
        assert!(!format!("{err:?} {err}").contains(input));
    }
    assert!(RemoteTransportMapping::new(
        RemoteInstance::github_com(),
        "https://canary@clone.example/"
    )
    .is_err());
    let mapping =
        RemoteTransportMapping::new(RemoteInstance::github_com(), "git@canary-alias:").unwrap();
    assert!(!format!("{mapping:?}").contains("canary"));
}

#[test]
fn explicit_ipv6_endpoints_and_effective_default_ports_are_preserved() {
    let i = gitlab("https://[::1]:8443/forge");
    let r = resolver(&i, &["ssh://git@[::1]:2222/repos/", "git@[::1]:"]);
    for input in [
        "https://[::1]:8443/forge/team/app.git",
        "ssh://git@[::1]:2222/repos/team/app.git",
        "git@[::1]:team/app.git",
    ] {
        assert_eq!(
            r.resolve(input).unwrap(),
            target("https://[::1]:8443/forge", "team/app")
        );
    }
    let default = resolver(&gitlab("https://git.example:443/forge"), &[]);
    for input in [
        "https://git.example/forge/team/app",
        "https://git.example:443/forge/team/app",
    ] {
        assert_eq!(
            default.resolve(input).unwrap(),
            target("https://git.example/forge", "team/app")
        );
    }
}

#[test]
fn only_the_terminal_clone_suffix_is_removed_without_namespace_truncation() {
    let r = resolver(&gitlab("git.example"), &[]);
    assert_eq!(
        r.resolve("https://git.example/group.git/sub/App.git")
            .unwrap(),
        target("https://git.example", "group.git/sub/App")
    );
    assert_eq!(
        r.resolve("https://git.example/group/sub/App.Git").unwrap(),
        target("https://git.example", "group/sub/App.Git")
    );
}
