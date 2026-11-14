//! Which matrix paths a test runs, per `SAMPLEZ_REQUIRE_PATHS`. Shared by
//! the unit tests and, through `#[path]`, the integration tests, so it
//! names `Path` through the including module.

use std::env::VarError;
use std::io::Write;

use super::Path;

/// Paths named in `list`, comma separated, case and spaces ignored.
pub(crate) fn parse_paths(list: &str) -> Result<Vec<Path>, String> {
    list.split(',')
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .map(|name| match name.to_ascii_lowercase().as_str() {
            "scalar" => Ok(Path::Scalar),
            "avx2" => Ok(Path::Avx2),
            _ => Err(format!("SAMPLEZ_REQUIRE_PATHS names unknown path {name:?}")),
        })
        .collect()
}

/// Whether a test of `path` runs on this CPU.
///
/// A path missing from the CPU fails the test when the environment
/// variable `SAMPLEZ_REQUIRE_PATHS` lists it, and a name there that is not
/// a path fails every path test. Otherwise a missing path is reported on
/// stderr, past libtest's output capture, and the test does not run it.
pub(crate) fn path_runs(test: &str, path: Path) -> bool {
    let required = match std::env::var("SAMPLEZ_REQUIRE_PATHS") {
        Ok(list) => parse_paths(&list).unwrap_or_else(|e| panic!("{test}: {e}")),
        Err(VarError::NotPresent) => Vec::new(),
        Err(VarError::NotUnicode(_)) => panic!("{test}: SAMPLEZ_REQUIRE_PATHS is not unicode"),
    };
    if path.is_supported() {
        return true;
    }
    assert!(
        !required.contains(&path),
        "{test}: SAMPLEZ_REQUIRE_PATHS requires the {path:?} path, which this CPU does not support"
    );
    let _ = writeln!(
        std::io::stderr(),
        "{test}: {path:?} path NOT RUN, this CPU does not support it"
    );
    false
}
