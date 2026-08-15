//! WASM module versioning for third-party plugin workflows.
//!
//! This module provides the versioning primitives used by the module loader
//! and registry:
//!
//! * [`ModuleVersion`] — a normalized semantic version. Module versions are
//!   **immutable**: once a version is registered its bytes must not change.
//! * [`VersionSelector`] — how a caller wants a version resolved (exact pin,
//!   latest, or a semver constraint).
//! * [`resolve_version`] — picks a concrete version from a set of available
//!   versions given a selector.
//! * [`content_hash`] — a content-addressed SHA-256 digest of module bytes,
//!   used for idempotent reloads and tamper detection.
//! * [`ApiFingerprint`] / [`CompatibilityPolicy`] — reason about whether two
//!   module versions are safe to swap without breaking callers, without
//!   re-implementing a full schema diff.

use crate::error::{Error, Result};
use chrono::{DateTime, Utc};
use semver::{Version, VersionReq};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;
use std::str::FromStr;

/// A normalized semantic version for a WASM module.
///
/// Always stored in canonical `semver` form. Construction via [`ModuleVersion::parse`]
/// validates the string and rejects malformed input, so any [`ModuleVersion`]
/// in the system is guaranteed well-formed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ModuleVersion(Version);

impl ModuleVersion {
    /// Parse and normalize a version string (e.g. `"1.2.3"`, `"0.1.0-rc.1"`).
    pub fn parse(s: &str) -> Result<Self> {
        Version::parse(s.trim())
            .map(ModuleVersion)
            .map_err(|e| Error::InvalidInput(format!("invalid module version {:?}: {}", s, e)))
    }

    /// The underlying `semver::Version`.
    pub fn as_semver(&self) -> &Version {
        &self.0
    }

    /// The canonical version string (e.g. `"1.2.3"`).
    pub fn canonical(&self) -> String {
        self.0.to_string()
    }

    pub fn major(&self) -> u64 {
        self.0.major
    }

    pub fn minor(&self) -> u64 {
        self.0.minor
    }

    pub fn patch(&self) -> u64 {
        self.0.patch
    }

    /// Whether this version carries a pre-release tag (e.g. `-rc.1`).
    pub fn is_prerelease(&self) -> bool {
        !self.0.pre.is_empty()
    }
}

impl fmt::Display for ModuleVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for ModuleVersion {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self> {
        ModuleVersion::parse(s)
    }
}

impl From<Version> for ModuleVersion {
    fn from(v: Version) -> Self {
        ModuleVersion(v)
    }
}

/// How a caller wants a particular module's version resolved.
#[derive(Debug, Clone, Default)]
pub enum VersionSelector {
    /// An exact pinned version (e.g. `"1.2.3"`).
    Exact(ModuleVersion),
    /// The latest available version, preferring stable releases over
    /// pre-releases. Falls back to pre-releases only when no stable version
    /// exists.
    #[default]
    Latest,
    /// A semver constraint (e.g. `"^1.2"`, `">=1.0.0, <2.0.0"`). Pre-releases
    /// match only when the constraint explicitly references a pre-release tag,
    /// matching cargo/semver semantics.
    Constraint(VersionReq),
}

impl VersionSelector {
    /// Parse a selector from user input.
    ///
    /// * `"latest"` or `"*"` → [`VersionSelector::Latest`]
    /// * a valid exact version → [`VersionSelector::Exact`]
    /// * anything else is treated as a semver [`VersionReq`] constraint
    pub fn parse(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.eq_ignore_ascii_case("latest") || s == "*" {
            return Ok(VersionSelector::Latest);
        }
        if let Ok(v) = ModuleVersion::parse(s) {
            return Ok(VersionSelector::Exact(v));
        }
        VersionReq::parse(s)
            .map(VersionSelector::Constraint)
            .map_err(|e| Error::InvalidInput(format!("invalid version selector {:?}: {}", s, e)))
    }
}

impl fmt::Display for VersionSelector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VersionSelector::Exact(v) => write!(f, "{}", v),
            VersionSelector::Latest => write!(f, "latest"),
            VersionSelector::Constraint(r) => write!(f, "{}", r),
        }
    }
}

/// Resolve a selector against a set of available versions.
///
/// Returns `None` when no available version satisfies the selector. The input
/// slice need not be sorted.
pub fn resolve_version(
    available: &[ModuleVersion],
    selector: &VersionSelector,
) -> Option<ModuleVersion> {
    match selector {
        VersionSelector::Exact(v) => available.iter().find(|a| a == &v).cloned(),
        VersionSelector::Latest => {
            let has_stable = available.iter().any(|v| !v.is_prerelease());
            available
                .iter()
                .filter(|v| !has_stable || !v.is_prerelease())
                .max()
                .cloned()
        }
        VersionSelector::Constraint(req) => available
            .iter()
            .filter(|v| req.matches(&v.0))
            .max()
            .cloned(),
    }
}

/// SHA-256 content hash of module bytes (lowercase hex).
///
/// Used for idempotent reloads (same bytes → same module) and for detecting
/// tampering when a version is re-registered with different bytes.
pub fn content_hash(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

/// A stable fingerprint of a module's import/export surface.
///
/// Two versions that expose the same function signatures (names + types) are
/// considered API-compatible even if their internal implementation differs.
/// This is intentionally coarse: it captures the *call surface* that engine
/// and workflow code depends on, not internal behaviour.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApiFingerprint(pub String);

impl ApiFingerprint {
    /// Build a fingerprint from a module's exports and imports.
    ///
    /// `exports` are `(name, type)`, `imports` are `(module, name, type)`.
    /// Entries are sorted before hashing so ordering is irrelevant.
    pub fn from_exports_imports(
        exports: &[(String, String)],
        imports: &[(String, String, String)],
    ) -> Self {
        let mut e: Vec<&(String, String)> = exports.iter().collect();
        e.sort();
        let mut i: Vec<&(String, String, String)> = imports.iter().collect();
        i.sort();

        let mut s = String::new();
        for (n, t) in &e {
            s.push_str(&format!("E:{}:{};", n, t));
        }
        for (m, n, t) in &i {
            s.push_str(&format!("I:{}:{}:{};", m, n, t));
        }
        ApiFingerprint(content_hash(s.as_bytes()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Policy describing which module versions may be swapped for one another
/// without breaking callers.
#[derive(Debug, Clone)]
pub struct CompatibilityPolicy {
    /// Versions that are declared mutually compatible (a "compatibility group").
    /// Any two members of this set are considered compatible in either
    /// direction.
    pub declared_compatible: BTreeSet<ModuleVersion>,
    /// If `true`, two versions sharing a major number are compatible by
    /// default (the conventional semver contract: majors break, minors/patch
    /// are additive and backward-compatible).
    pub same_major_compatible: bool,
    /// If `true`, two versions with an identical [`ApiFingerprint`] are
    /// compatible even across major boundaries.
    pub identical_fingerprint_compatible: bool,
}

impl Default for CompatibilityPolicy {
    fn default() -> Self {
        CompatibilityPolicy {
            declared_compatible: BTreeSet::new(),
            same_major_compatible: true,
            identical_fingerprint_compatible: true,
        }
    }
}

impl CompatibilityPolicy {
    /// Whether `candidate` is safe to use in place of `existing`.
    ///
    /// `existing_fp` / `candidate_fp` are optional API fingerprints; when
    /// `identical_fingerprint_compatible` is set they are consulted to allow
    /// cross-major swaps that did not change the call surface.
    pub fn is_compatible(
        &self,
        existing: &ModuleVersion,
        candidate: &ModuleVersion,
        existing_fp: Option<&ApiFingerprint>,
        candidate_fp: Option<&ApiFingerprint>,
    ) -> bool {
        if existing == candidate {
            return true;
        }

        if !self.declared_compatible.is_empty()
            && self.declared_compatible.contains(existing)
            && self.declared_compatible.contains(candidate)
        {
            return true;
        }

        if self.same_major_compatible && existing.major() == candidate.major() {
            return true;
        }

        if self.identical_fingerprint_compatible {
            if let (Some(a), Some(b)) = (existing_fp, candidate_fp) {
                if a == b {
                    return true;
                }
            }
        }

        false
    }
}

/// Rich metadata describing one loaded module version.
#[derive(Debug, Clone)]
pub struct ModuleVersionInfo {
    pub name: String,
    pub version: ModuleVersion,
    pub size_bytes: usize,
    pub loaded_at: DateTime<Utc>,
    /// Content-addressed SHA-256 of the module bytes.
    pub hash: String,
    /// Fingerprint of the module's import/export surface.
    pub fingerprint: ApiFingerprint,
}

impl ModuleVersionInfo {
    pub fn cache_key(&self) -> String {
        format!("{}@{}", self.name, self.version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_normalizes_and_rejects_garbage() {
        let v = ModuleVersion::parse("1.2.3").unwrap();
        assert_eq!(v.canonical(), "1.2.3");
        assert_eq!(v.major(), 1);
        assert!(!v.is_prerelease());

        let pre = ModuleVersion::parse("1.2.3-rc.1").unwrap();
        assert!(pre.is_prerelease());

        assert!(ModuleVersion::parse("not-a-version").is_err());
        assert!(ModuleVersion::parse("v1.2").is_err());
    }

    #[test]
    fn resolve_exact_finds_pin() {
        let versions = vec![
            ModuleVersion::parse("1.0.0").unwrap(),
            ModuleVersion::parse("1.2.0").unwrap(),
            ModuleVersion::parse("2.0.0").unwrap(),
        ];
        let sel = VersionSelector::parse("1.2.0").unwrap();
        assert_eq!(
            resolve_version(&versions, &sel),
            Some(ModuleVersion::parse("1.2.0").unwrap())
        );
        // missing exact version
        assert_eq!(
            resolve_version(&versions, &VersionSelector::parse("1.5.0").unwrap()),
            None
        );
    }

    #[test]
    fn resolve_latest_prefers_stable() {
        let versions = vec![
            ModuleVersion::parse("1.0.0").unwrap(),
            ModuleVersion::parse("1.2.0-rc.1").unwrap(),
            ModuleVersion::parse("1.1.0").unwrap(),
        ];
        let latest = resolve_version(&versions, &VersionSelector::Latest).unwrap();
        assert_eq!(latest, ModuleVersion::parse("1.1.0").unwrap());

        // only prereleases available -> they win
        let pre_only = vec![
            ModuleVersion::parse("1.2.0-rc.1").unwrap(),
            ModuleVersion::parse("1.2.0-rc.2").unwrap(),
        ];
        let latest_pre = resolve_version(&pre_only, &VersionSelector::Latest).unwrap();
        assert_eq!(latest_pre, ModuleVersion::parse("1.2.0-rc.2").unwrap());
    }

    #[test]
    fn resolve_constraint_respects_semver() {
        let versions = vec![
            ModuleVersion::parse("1.0.0").unwrap(),
            ModuleVersion::parse("1.5.0").unwrap(),
            ModuleVersion::parse("2.0.0").unwrap(),
            ModuleVersion::parse("2.1.0-rc.1").unwrap(),
        ];
        // caret: >=1.0.0, <2.0.0
        let caret = resolve_version(&versions, &VersionSelector::parse("^1.0.0").unwrap()).unwrap();
        assert_eq!(caret, ModuleVersion::parse("1.5.0").unwrap());

        // prerelease excluded unless constraint names it
        let any_two =
            resolve_version(&versions, &VersionSelector::parse(">=2.0.0").unwrap()).unwrap();
        assert_eq!(any_two, ModuleVersion::parse("2.0.0").unwrap());

        let rc =
            resolve_version(&versions, &VersionSelector::parse(">=2.1.0-rc.1").unwrap()).unwrap();
        assert_eq!(rc, ModuleVersion::parse("2.1.0-rc.1").unwrap());
    }

    #[test]
    fn content_hash_is_stable_and_distinct() {
        let a = content_hash(b"module-a");
        let b = content_hash(b"module-b");
        let a2 = content_hash(b"module-a");
        assert_eq!(a, a2);
        assert_ne!(a, b);
        assert_eq!(a.len(), 64);
    }

    #[test]
    fn fingerprint_ignores_order() {
        let fp1 = ApiFingerprint::from_exports_imports(
            &[("run".into(), "func".into())],
            &[("host".into(), "log".into(), "func".into())],
        );
        let fp2 = ApiFingerprint::from_exports_imports(
            &[("run".into(), "func".into())],
            &[("host".into(), "log".into(), "func".into())],
        );
        let fp3 = ApiFingerprint::from_exports_imports(
            &[("run".into(), "func".into())],
            &[("host".into(), "log".into(), "func2".into())],
        );
        assert_eq!(fp1, fp2);
        assert_ne!(fp1, fp3);
    }

    #[test]
    fn compatibility_policy_defaults() {
        let policy = CompatibilityPolicy::default();
        let v1 = ModuleVersion::parse("1.0.0").unwrap();
        let v1_1 = ModuleVersion::parse("1.1.0").unwrap();
        let v2 = ModuleVersion::parse("2.0.0").unwrap();

        // same major -> compatible
        assert!(policy.is_compatible(&v1, &v1_1, None, None));
        // different major, no fingerprint info -> incompatible
        assert!(!policy.is_compatible(&v1, &v2, None, None));

        // identical fingerprint across majors -> compatible
        let fp = ApiFingerprint("abc".into());
        assert!(policy.is_compatible(&v1, &v2, Some(&fp), Some(&fp)));

        // different fingerprint across majors -> incompatible
        assert!(!policy.is_compatible(&v1, &v2, Some(&fp), Some(&ApiFingerprint("def".into()))));

        // exact same version always compatible
        assert!(policy.is_compatible(&v1, &v1, None, None));
    }

    #[test]
    fn compatibility_policy_declared_group() {
        let mut policy = CompatibilityPolicy {
            declared_compatible: BTreeSet::new(),
            same_major_compatible: false,
            identical_fingerprint_compatible: false,
        };
        let a = ModuleVersion::parse("1.0.0").unwrap();
        let b = ModuleVersion::parse("3.0.0").unwrap();
        // not declared yet, different major, no fp -> incompatible
        assert!(!policy.is_compatible(&a, &b, None, None));
        policy.declared_compatible.insert(a.clone());
        policy.declared_compatible.insert(b.clone());
        assert!(policy.is_compatible(&a, &b, None, None));
    }
}
