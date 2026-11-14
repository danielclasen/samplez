//! Shared helpers for the tests against the real libmxl, loaded from
//! `SAMPLEZ_TEST_LIBMXL` (default `/opt/mxl/lib/libmxl.so`). A missing
//! library fails the tests.
//!
//! [`RawWriter`] and [`RawReader`] call the `mxl-sys` table directly, so
//! samplez's read path is checked against an independent writer and its
//! write path against an independent reader.

#![allow(dead_code)]

use std::collections::hash_map::RandomState;
use std::ffi::CString;
use std::hash::BuildHasher;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use samplez_mxl::input::{InputConfig, Timing};
use samplez_mxl::{Instance, MxlApi, Status};

pub const B: u64 = 48;

pub const TIMING: Timing = Timing {
    block: B,
    margin: 576,
    max_catchup_blocks: 2,
};

pub fn api() -> Arc<MxlApi> {
    let path = std::env::var("SAMPLEZ_TEST_LIBMXL")
        .unwrap_or_else(|_| "/opt/mxl/lib/libmxl.so".to_owned());
    assert!(Path::new(&path).is_file(), "libmxl not found at {path}");
    samplez_mxl::load(Path::new(&path)).unwrap()
}

/// A fresh MXL domain directory, removed on drop.
pub struct Domain(pub PathBuf);

impl Domain {
    pub fn new() -> Domain {
        static N: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "samplez-mxl-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir(&dir).unwrap();
        Domain(dir)
    }

    /// Names in the domain directory starting with `.mxl-tmp-`.
    pub fn tmp_dirs(&self) -> Vec<String> {
        std::fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|n| n.starts_with(".mxl-tmp-"))
            .collect()
    }

    pub fn flow_dir(&self, flow_id: &str) -> PathBuf {
        self.0.join(format!("{flow_id}.mxl-flow"))
    }
}

impl Drop for Domain {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub fn new_id() -> String {
    let r = RandomState::new();
    let x = (u128::from(r.hash_one(1u8)) << 64) | u128::from(r.hash_one(2u8));
    let h = format!("{x:032x}");
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

/// The known test value of sample `index` of `channel`: exact in f32.
pub fn sample(channel: usize, index: u64) -> f32 {
    (index % 1_000_003) as f32 + channel as f32 * 0.25
}

/// [`sample`] over block `k` read with `offset`, planar.
pub fn expected(channels: usize, k: u64, offset: i64) -> Vec<f32> {
    let start = (k * B).checked_add_signed(offset).unwrap();
    (0..channels)
        .flat_map(|c| (start..start + B).map(move |i| sample(c, i)))
        .collect()
}

pub fn input(flow_id: &str, channels: u32, offset: i64) -> InputConfig<'_> {
    InputConfig {
        flow_id,
        channels,
        offset,
    }
}

/// A libmxl writer created with raw calls on an instance of its own, with
/// libmxl's default batch hints.
pub struct RawWriter {
    instance: Arc<Instance>,
    raw: mxl_sys::FlowWriter,
    channels: usize,
}

// SAFETY: test-only; the writer is used by one thread at a time.
unsafe impl Send for RawWriter {}

impl RawWriter {
    pub fn create(api: &Arc<MxlApi>, domain: &Path, flow_id: &str, channels: u32) -> RawWriter {
        let instance = Instance::new(api, domain).unwrap();
        let def = CString::new(format!(
            r#"{{"id":"{flow_id}","label":"raw writer","format":"urn:x-nmos:format:audio",
            "media_type":"audio/float32","sample_rate":{{"numerator":48000}},
            "channel_count":{channels},"bit_depth":32,
            "tags":{{"urn:x-nmos:tag:grouphint/v1.0":["raw writer:Audio"]}}}}"#
        ))
        .unwrap();
        let mut raw = std::ptr::null_mut();
        let mut created = false;
        // SAFETY: valid instance, NUL-terminated definition, writable outputs.
        let s = unsafe {
            api.create_flow_writer(
                instance.raw(),
                def.as_ptr(),
                std::ptr::null(),
                &mut raw,
                std::ptr::null_mut(),
                &mut created,
            )
        };
        assert_eq!(s, mxl_sys::MXL_STATUS_OK);
        assert!(created);
        RawWriter {
            instance,
            raw,
            channels: channels as usize,
        }
    }

    /// Writes `[end - count, end)` with `f(channel, index)` and commits.
    pub fn write(&mut self, end: u64, count: usize, f: impl Fn(usize, u64) -> f32) {
        let mut s = mxl_sys::MutableWrappedMultiBufferSlice::default();
        // SAFETY: live writer, writable slice.
        let status = unsafe {
            self.instance
                .api()
                .flow_writer_open_samples(self.raw, end, count, &mut s)
        };
        assert_eq!(status, mxl_sys::MXL_STATUS_OK);
        assert_eq!(s.count, self.channels);
        let mut index = end - count as u64;
        for frag in s.base.fragments {
            let n = frag.size / 4;
            for ch in 0..s.count {
                // SAFETY: libmxl returned writable fragments inside the
                // mapped ring; channel `ch` is `ch * stride` bytes on.
                let out = unsafe {
                    std::slice::from_raw_parts_mut(
                        frag.pointer.cast::<u8>().add(ch * s.stride).cast::<f32>(),
                        n,
                    )
                };
                for (i, v) in out.iter_mut().enumerate() {
                    *v = f(ch, index + i as u64);
                }
            }
            index += n as u64;
        }
        // SAFETY: live writer with samples open.
        let status = unsafe { self.instance.api().flow_writer_commit_samples(self.raw) };
        assert_eq!(status, mxl_sys::MXL_STATUS_OK);
    }

    /// Writes blocks `ks` with [`sample`].
    pub fn blocks(&mut self, ks: std::ops::Range<u64>) {
        for k in ks {
            self.write((k + 1) * B, B as usize, sample);
        }
    }
}

impl Drop for RawWriter {
    fn drop(&mut self) {
        // SAFETY: released once, on the instance that created it.
        unsafe {
            self.instance
                .api()
                .release_flow_writer(self.instance.raw(), self.raw)
        };
    }
}

/// A libmxl reader opened with raw calls on an instance of its own.
pub struct RawReader {
    instance: Arc<Instance>,
    raw: mxl_sys::FlowReader,
}

impl RawReader {
    pub fn open(api: &Arc<MxlApi>, domain: &Path, flow_id: &str) -> RawReader {
        let instance = Instance::new(api, domain).unwrap();
        let id = CString::new(flow_id).unwrap();
        let mut raw = std::ptr::null_mut();
        // SAFETY: valid instance, NUL-terminated id, writable out pointer.
        let s = unsafe {
            api.create_flow_reader(instance.raw(), id.as_ptr(), std::ptr::null(), &mut raw)
        };
        assert_eq!(s, mxl_sys::MXL_STATUS_OK);
        RawReader { instance, raw }
    }

    pub fn head(&self) -> u64 {
        let mut info = mxl_sys::FlowRuntimeInfo::default();
        // SAFETY: live reader, writable info.
        let s = unsafe {
            self.instance
                .api()
                .flow_reader_get_runtime_info(self.raw, &mut info)
        };
        assert_eq!(s, mxl_sys::MXL_STATUS_OK);
        info.headIndex
    }

    /// The samples of `[end - count, end)` per channel, as libmxl serves
    /// them.
    pub fn samples(&self, end: u64, count: usize) -> Result<Vec<Vec<f32>>, Status> {
        let mut s = mxl_sys::WrappedMultiBufferSlice::default();
        // SAFETY: live reader, writable slice.
        let status = unsafe {
            self.instance
                .api()
                .flow_reader_get_samples_non_blocking(self.raw, end, count, &mut s)
        };
        if status != mxl_sys::MXL_STATUS_OK {
            return Err(status);
        }
        Ok((0..s.count)
            .map(|ch| {
                s.base
                    .fragments
                    .iter()
                    .flat_map(|f| {
                        // SAFETY: libmxl returned fragments inside the mapped
                        // ring; channel `ch` is `ch * stride` bytes on.
                        unsafe {
                            std::slice::from_raw_parts(
                                f.pointer.cast::<u8>().add(ch * s.stride).cast::<f32>(),
                                f.size / 4,
                            )
                        }
                        .to_vec()
                    })
                    .collect()
            })
            .collect())
    }
}

impl Drop for RawReader {
    fn drop(&mut self) {
        // SAFETY: released once, on the instance that created it.
        unsafe {
            self.instance
                .api()
                .release_flow_reader(self.instance.raw(), self.raw)
        };
    }
}
