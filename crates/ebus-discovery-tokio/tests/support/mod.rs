//! Helpers shared by the end-to-end tests.

/// Skip a test whose resource is missing, with `why` on stderr; with
/// `EBUS_REQUIRE_E2E` set (the CI job that provides every resource), fail
/// instead.
pub fn skip(why: &str) {
    if std::env::var_os("EBUS_REQUIRE_E2E").is_some() {
        panic!("EBUS_REQUIRE_E2E is set, but {why}");
    }
    eprintln!("skipping: {why}");
}
