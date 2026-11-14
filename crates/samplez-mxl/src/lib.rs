//! MXL flow input and output for the samplez engine, on libmxl v1.1.0.
//!
//! libmxl is loaded with [`mxl::load_api`] and every call goes through the
//! `mxl-sys` function table it returns. The upstream `mxl` crate API is not
//! used: it keeps the instance and reader handles private, and the
//! synchronization group and `mxlIsFlowActive` calls need them.
//!
//! Functions documented as RT make no Rust allocation and take no lock of
//! their own; the libmxl calls they make are listed on each. Everything
//! else runs on the supervisor side.

use std::ffi::{CStr, CString};
use std::fmt;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

pub mod input;
pub mod output;
pub mod time;

pub use mxl::MxlApi;
pub use mxl_sys::Status;

/// The only sample rate samplez reads and writes.
pub const SAMPLE_RATE: i64 = 48_000;

#[derive(Debug)]
pub enum Error {
    /// The library could not be loaded.
    Load(String),
    /// The loaded library is not libmxl 1.1, the version of the headers
    /// the bindings come from.
    Version(String),
    /// A libmxl call returned a status other than `MXL_STATUS_OK`.
    Mxl { call: &'static str, status: Status },
    /// An id, a flow definition or a flow's configuration is not what
    /// samplez accepts.
    Format(String),
    /// The input's read window needs `required` samples of readable
    /// history, the flow offers `buffer_length / 2`.
    Window { required: u64, buffer_length: u64 },
    /// The output's block size is longer than the `buffer_length / 2`
    /// samples libmxl lets a writer open at once.
    Block { block: u64, buffer_length: u64 },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Load(m) => write!(f, "loading libmxl: {m}"),
            Error::Version(v) => write!(f, "libmxl version {v} is not 1.1"),
            Error::Mxl { call, status } => write!(f, "{call} returned status {status}"),
            Error::Format(m) => write!(f, "{m}"),
            Error::Window {
                required,
                buffer_length,
            } => write!(
                f,
                "read window needs {required} samples of history, the flow's ring of \
                 {buffer_length} offers half of it"
            ),
            Error::Block {
                block,
                buffer_length,
            } => write!(
                f,
                "block of {block} samples is longer than half the flow's ring of \
                 {buffer_length}"
            ),
        }
    }
}

impl std::error::Error for Error {}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) fn check(call: &'static str, status: Status) -> Result<()> {
    if status == mxl_sys::MXL_STATUS_OK {
        Ok(())
    } else {
        Err(Error::Mxl { call, status })
    }
}

/// Loads libmxl from `path` and checks that it is version 1.1.
pub fn load(path: &Path) -> Result<Arc<MxlApi>> {
    let api = mxl::load_api(path).map_err(|e| Error::Load(format!("{}: {e}", path.display())))?;
    let mut v = mxl_sys::VersionType::default();
    // SAFETY: `v` is a valid, writable VersionType for the call.
    check("mxlGetVersion", unsafe { api.get_version(&mut v) })?;
    if (v.major, v.minor) != (1, 1) {
        let full = if v.full.is_null() {
            format!("{}.{}.{}", v.major, v.minor, v.bugfix)
        } else {
            // SAFETY: libmxl sets `full` to a NUL-terminated string it owns
            // for the life of the library.
            unsafe { CStr::from_ptr(v.full) }
                .to_string_lossy()
                .into_owned()
        };
        return Err(Error::Version(full));
    }
    Ok(api)
}

/// An MXL instance on one domain, created without options. Readers,
/// writers and groups hold an `Arc` to it, so they are released before
/// `mxlDestroyInstance` runs. Dropping the last `Arc` destroys the
/// instance, which must not happen on the RT thread.
pub struct Instance {
    api: Arc<MxlApi>,
    raw: mxl_sys::Instance,
    /// Serialises reader and group creation and release on this instance.
    lock: Mutex<()>,
}

// SAFETY: the handle is a plain pointer to a libmxl instance, usable from
// any thread.
unsafe impl Send for Instance {}
// SAFETY: in libmxl v1.1.0 reader and writer creation and release take the
// instance's own mutex; mxlIsFlowActive, mxlGetFlowDef and
// mxlGarbageCollectFlows touch no instance state. Group creation and
// release modify the instance's group list without that mutex, reader
// release walks that list and removes the reader from every group, and a
// group wait reorders the group's reader list without any lock. samplez
// serialises those calls by ownership: groups exist only inside a
// `Generation`, which keeps its instance private and is used by one thread
// at a time. `lock` additionally serialises reader and group creation and
// release, all of which run on the supervisor side.
unsafe impl Sync for Instance {}

impl Instance {
    pub fn new(api: &Arc<MxlApi>, domain: &Path) -> Result<Arc<Instance>> {
        let c_domain = CString::new(domain.as_os_str().as_encoded_bytes())
            .map_err(|_| Error::Format(format!("path {} contains NUL", domain.display())))?;
        // SAFETY: `c_domain` is NUL-terminated and outlives the call; libmxl
        // accepts a null options pointer.
        let raw = unsafe { api.create_instance(c_domain.as_ptr(), std::ptr::null()) };
        if raw.is_null() {
            // libmxl returns null without a status.
            return Err(Error::Mxl {
                call: "mxlCreateInstance",
                status: mxl_sys::MXL_ERR_UNKNOWN,
            });
        }
        Ok(Arc::new(Instance {
            api: Arc::clone(api),
            raw,
            lock: Mutex::new(()),
        }))
    }

    /// Held around reader and group creation and release.
    pub(crate) fn lock(&self) -> MutexGuard<'_, ()> {
        self.lock.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn api(&self) -> &MxlApi {
        &self.api
    }

    pub fn raw(&self) -> mxl_sys::Instance {
        self.raw
    }

    /// `mxlIsFlowActive`: `None` for `MXL_ERR_FLOW_NOT_FOUND`. libmxl
    /// v1.1.0 returns that status for any failure to open the flow's data
    /// file, not only for a missing one: a flow whose data file this
    /// process cannot open reads as absent.
    pub fn is_flow_active(&self, flow_id: &str) -> Result<Option<bool>> {
        let id = id_cstring(flow_id)?;
        let mut active = false;
        // SAFETY: `id` is NUL-terminated and `active` writable for the call.
        let status = unsafe { self.api.is_flow_active(self.raw, id.as_ptr(), &mut active) };
        if status == mxl_sys::MXL_ERR_FLOW_NOT_FOUND {
            return Ok(None);
        }
        check("mxlIsFlowActive", status)?;
        Ok(Some(active))
    }

    /// `mxlGarbageCollectFlows`: removes every flow of the domain no writer
    /// holds.
    pub fn garbage_collect(&self) -> Status {
        // SAFETY: the instance handle is live for the call.
        unsafe { self.api.garbage_collect_flows(self.raw) }
    }

    /// `mxlGetFlowDef`.
    pub fn flow_def(&self, flow_id: &str) -> Result<String> {
        let id = id_cstring(flow_id)?;
        let mut size = 0;
        // SAFETY: a null buffer asks libmxl for the required size, which it
        // writes to `size`.
        let status = unsafe {
            self.api
                .get_flow_def(self.raw, id.as_ptr(), std::ptr::null_mut(), &mut size)
        };
        if status != mxl_sys::MXL_ERR_INVALID_ARG || size == 0 {
            check("mxlGetFlowDef", status)?;
        }
        let mut buf = vec![0u8; size];
        // SAFETY: `buf` holds `size` writable bytes, the size libmxl asked
        // for.
        check("mxlGetFlowDef", unsafe {
            self.api
                .get_flow_def(self.raw, id.as_ptr(), buf.as_mut_ptr().cast(), &mut size)
        })?;
        let def = CStr::from_bytes_until_nul(&buf)
            .map_err(|_| Error::Format(format!("flow {flow_id}: definition without NUL")))?;
        Ok(def.to_string_lossy().into_owned())
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        // SAFETY: `raw` came from mxlCreateInstance; every reader, writer and
        // group holds an Arc to this instance, so all are released.
        unsafe { self.api.destroy_instance(self.raw) };
    }
}

/// Accepts the lowercase canonical UUID form of NMOS ids (IS-04 schema
/// pattern), which MXL uses for flow ids.
pub fn validate_id(id: &str) -> Result<()> {
    let ok = id.len() == 36
        && id.bytes().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(&c),
        });
    if ok {
        Ok(())
    } else {
        Err(Error::Format(format!("{id:?} is not a lowercase UUID")))
    }
}

pub(crate) fn id_cstring(id: &str) -> Result<CString> {
    validate_id(id)?;
    Ok(CString::new(id).expect("a validated UUID has no NUL"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_are_lowercase_canonical_uuids() {
        assert!(validate_id("5fbec3b1-1b0f-417d-9059-8b94a47197ed").is_ok());
        assert!(validate_id("5FBEC3B1-1B0F-417D-9059-8B94A47197ED").is_err());
        assert!(validate_id("5fbec3b11b0f417d90598b94a47197ed").is_err());
        assert!(validate_id("5fbec3b1-1b0f-417d-9059-8b94a47197e").is_err());
        assert!(validate_id("5fbec3b1-1b0f-417d-9059-8b94a47197eg").is_err());
        assert!(validate_id("5fbec3b1x1b0f-417d-9059-8b94a47197ed").is_err());
    }
}
