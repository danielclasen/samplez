//! Reading MXL audio flows: input generations built on the supervisor
//! side, the RT block read, failed-input identification and the poller for
//! a pruned input.
//!
//! None of the types here may be dropped on the RT thread: dropping a
//! reader or group releases it in libmxl (munmap, instance mutex), and
//! dropping a generation destroys its instance.

#![deny(clippy::disallowed_methods, clippy::disallowed_types)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, Sender, SyncSender, TryRecvError, channel, sync_channel};
use std::time::Instant;

use mxl_sys::{
    MXL_DATA_FORMAT_AUDIO, MXL_ERR_FLOW_INVALID, MXL_ERR_FLOW_NOT_FOUND, MXL_ERR_INVALID_ARG,
    MXL_ERR_OUT_OF_RANGE_TOO_EARLY, MXL_ERR_OUT_OF_RANGE_TOO_LATE, MXL_STATUS_OK,
    WrappedMultiBufferSlice,
};

use crate::lookup::{Backoff, Look, Lookup, exists, flow_dir};
use crate::time::{RATE, timestamp_to_index};
use crate::{Error, Instance, MxlApi, Result, Status, check, id_cstring};

/// Consecutive late blocks with a frozen head after which
/// [`Input::identify`] probes an input for validity.
#[deprecated(note = "only the deprecated `Input::identify` probes")]
pub const FROZEN_PROBE_BLOCKS: u32 = 10;

/// The x86_64 base page size. A larger page is touched more than once.
const PAGE: usize = 4096;

/// How long a polled head must keep advancing before the input is added
/// back.
pub const RECOVERY_NS: u64 = 1_000_000_000;

/// The block timeline shared by every input.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// Block size B in samples.
    pub block: u64,
    /// Read delay `readerMarginSamples`.
    pub margin: u64,
    pub max_catchup_blocks: u64,
    /// `resyncBudgetMs` in samples, which a resync replay reads past the
    /// read window, with more than one voter; else 0.
    pub resync_budget: u64,
}

/// The most channels a flow selection may name: every index is below it.
pub const MAX_FLOW_CHANNELS: u32 = 64;

/// One input slot's configuration.
#[derive(Clone, Copy, Debug)]
pub struct InputConfig<'a> {
    pub flow_id: &'a str,
    /// The input's legs.
    pub channels: u32,
    /// `flowChannels`: the 0-based flow channel each leg reads, in leg
    /// order, so `channels` long; an index may repeat. The flow needs at
    /// least the highest index + 1 channels. Without it the input reads
    /// every channel of a flow of exactly `channels`, in order.
    pub flow_channels: Option<&'a [u32]>,
    /// `offsetSamples`.
    pub offset: i64,
}

/// Test-only: called with the flow id before each reader open, under the
/// instance's lock as libmxl's own open runs, so that a test can make an
/// open take as long as a slow one does.
#[cfg(feature = "test-hooks")]
#[allow(clippy::disallowed_types, reason = "a test hook, supervisor side")]
static OPEN_DELAY: std::sync::Mutex<Option<fn(&str)>> = std::sync::Mutex::new(None);

/// Test-only: sets the hook [`FlowReader::open`] calls before each open.
#[cfg(feature = "test-hooks")]
#[allow(clippy::disallowed_methods, reason = "a test hook, supervisor side")]
pub fn set_open_delay(hook: Option<fn(&str)>) {
    *OPEN_DELAY.lock().unwrap_or_else(|e| e.into_inner()) = hook;
}

#[cfg(feature = "test-hooks")]
#[allow(clippy::disallowed_methods, reason = "a test hook, supervisor side")]
fn open_delay(flow_id: &str) {
    let hook = *OPEN_DELAY.lock().unwrap_or_else(|e| e.into_inner());
    if let Some(hook) = hook {
        hook(flow_id);
    }
}

/// Test-only: called with the flow id before each reader open; an open
/// it answers true for fails as one of a flow not found does.
#[cfg(feature = "test-hooks")]
pub type OpenFailure = fn(&str) -> bool;

/// Test-only: the [`OpenFailure`] hook set.
#[cfg(feature = "test-hooks")]
#[allow(clippy::disallowed_types, reason = "a test hook, supervisor side")]
static OPEN_FAILURE: std::sync::Mutex<Option<OpenFailure>> = std::sync::Mutex::new(None);

/// Test-only: sets the hook that makes [`FlowReader::open`] fail.
#[cfg(feature = "test-hooks")]
#[allow(clippy::disallowed_methods, reason = "a test hook, supervisor side")]
pub fn set_open_failure(hook: Option<OpenFailure>) {
    *OPEN_FAILURE.lock().unwrap_or_else(|e| e.into_inner()) = hook;
}

#[cfg(feature = "test-hooks")]
#[allow(clippy::disallowed_methods, reason = "a test hook, supervisor side")]
fn open_fails(flow_id: &str) -> bool {
    let hook = *OPEN_FAILURE.lock().unwrap_or_else(|e| e.into_inner());
    hook.is_some_and(|hook| hook(flow_id))
}

/// One reference to libmxl's reader for a flow on an instance. libmxl
/// shares one reader object per flow id within an instance and counts
/// references to it; dropping this releases one reference, the last one
/// unmaps the flow and removes the reader from every group of the
/// instance. Must not be dropped on the RT thread.
///
/// Shared between threads ([`Sync`]): every method taking `&self` makes
/// only libmxl's const reads of the reader, and only Drop releases it. A
/// method added here must keep to that, or the `Sync` impl is unsound.
pub struct FlowReader {
    instance: Arc<Instance>,
    raw: mxl_sys::FlowReader,
}

// SAFETY: the reader handle stays valid until Drop; one thread at a time
// owns the wrapper, and libmxl's read calls touch only the reader and its
// shared memory.
unsafe impl Send for FlowReader {}

// SAFETY: two generations may hold one reader, the RT thread reading it
// while the supervisor builds the next one. The calls made through `&self`
// are libmxl's reads only: mxlFlowReaderGetConfigInfo,
// mxlFlowReaderGetRuntimeInfo and mxlFlowReaderGetSamplesNonBlocking
// (lib/src/flow.cpp:311, 334 and 553 of the pinned mxl revision 8f3f109).
// Each reaches the reader through to_FlowReader and touches no instance
// state. The first two call the const getFlowConfigInfo and
// getFlowRuntimeInfo; the third calls getSamples, an override that is not
// const, which only checks _flowData and calls the const getSamplesImpl
// (lib/internal/src/PosixContinuousFlowReader.cpp:87-91). The class has no
// mutable member (lib/internal/src/PosixContinuousFlowReader.hpp), so none
// of them writes the reader: they read the flow's shared memory. The one
// call that changes the reader, mxlReleaseFlowReader (flow.cpp:172,
// Instance::releaseReader at lib/internal/src/Instance.cpp:142), is made
// only in Drop, which has the reader to itself.
unsafe impl Sync for FlowReader {}

impl FlowReader {
    /// `mxlCreateFlowReader`.
    pub fn open(instance: &Arc<Instance>, flow_id: &str) -> Result<FlowReader> {
        let id = id_cstring(flow_id)?;
        let mut raw = std::ptr::null_mut();
        #[allow(clippy::disallowed_methods, reason = "supervisor side")]
        let _lock = instance.lock();
        #[cfg(feature = "test-hooks")]
        open_delay(flow_id);
        #[cfg(feature = "test-hooks")]
        if open_fails(flow_id) {
            return Err(Error::Mxl {
                call: "mxlCreateFlowReader",
                status: MXL_ERR_FLOW_NOT_FOUND,
            });
        }
        // SAFETY: valid instance, NUL-terminated id, a null options pointer
        // and a writable out pointer.
        check("mxlCreateFlowReader", unsafe {
            instance.api().create_flow_reader(
                instance.raw(),
                id.as_ptr(),
                std::ptr::null(),
                &mut raw,
            )
        })?;
        Ok(FlowReader {
            instance: Arc::clone(instance),
            raw,
        })
    }

    /// The libmxl handle, for a call the caller makes itself. On a reader
    /// another thread may hold, that call must be one of the reads the
    /// `Sync` impl names, and never mxlReleaseFlowReader.
    pub fn raw(&self) -> mxl_sys::FlowReader {
        self.raw
    }

    /// `mxlFlowReaderGetConfigInfo`.
    pub fn config(&self) -> Result<mxl_sys::FlowConfigInfo> {
        let mut info = mxl_sys::FlowConfigInfo::default();
        // SAFETY: live reader, writable info.
        check("mxlFlowReaderGetConfigInfo", unsafe {
            self.instance
                .api()
                .flow_reader_get_config_info(self.raw, &mut info)
        })?;
        Ok(info)
    }

    /// RT. The head index from `mxlFlowReaderGetRuntimeInfo`.
    pub fn head(&self) -> std::result::Result<u64, Status> {
        let mut info = mxl_sys::FlowRuntimeInfo::default();
        // SAFETY: live reader, writable info.
        let status = unsafe {
            self.instance
                .api()
                .flow_reader_get_runtime_info(self.raw, &mut info)
        };
        if status == MXL_STATUS_OK {
            Ok(info.headIndex)
        } else {
            Err(status)
        }
    }

    /// RT. `mxlFlowReaderGetSamplesNonBlocking(end, count)`. The slices
    /// point into the flow's ring and are not to be held.
    fn samples(&self, end: u64, count: usize, slice: &mut WrappedMultiBufferSlice) -> Status {
        // SAFETY: live reader, writable slice.
        unsafe {
            self.instance
                .api()
                .flow_reader_get_samples_non_blocking(self.raw, end, count, slice)
        }
    }

    /// RT, failure path. `mxlFlowReaderGetSamplesNonBlocking(head + 1, 1)`:
    /// a range beyond the head, for which libmxl runs its flow validity
    /// check (one `stat()` and path allocations).
    fn probe(&self, head: u64) -> Status {
        let Some(end) = head.checked_add(1) else {
            return MXL_ERR_INVALID_ARG;
        };
        let mut slice = WrappedMultiBufferSlice::default();
        // The validity check is the failure path's documented exception;
        // RealtimeSanitizer allows this call only.
        crate::rtsan::allow(|| self.samples(end, 1, &mut slice))
    }
}

impl Drop for FlowReader {
    fn drop(&mut self) {
        #[allow(clippy::disallowed_methods, reason = "supervisor side")]
        let _lock = self.instance.lock();
        crate::note_release();
        // SAFETY: `raw` came from mxlCreateFlowReader on this instance and
        // this reference is released exactly once, here.
        unsafe {
            self.instance
                .api()
                .release_flow_reader(self.instance.raw(), self.raw)
        };
    }
}

/// A libmxl flow synchronization group. libmxl v1.1.0 locks neither the
/// group nor, for group creation and release, the instance's group list,
/// so a group exists only inside a [`Generation`], which keeps its instance
/// private. Must not be dropped on the RT thread.
pub(crate) struct SyncGroup {
    instance: Arc<Instance>,
    raw: mxl_sys::FlowSynchronizationGroup,
}

// SAFETY: the group handle stays valid until Drop and one thread at a time
// owns the wrapper; libmxl's group has no lock of its own, so its use is
// serialised by that ownership.
unsafe impl Send for SyncGroup {}

impl SyncGroup {
    /// `mxlCreateFlowSynchronizationGroup`.
    fn new(instance: &Arc<Instance>) -> Result<SyncGroup> {
        let mut raw = std::ptr::null_mut();
        #[allow(clippy::disallowed_methods, reason = "supervisor side")]
        let _lock = instance.lock();
        // SAFETY: valid instance, writable out pointer.
        check("mxlCreateFlowSynchronizationGroup", unsafe {
            instance
                .api()
                .create_flow_synchronization_group(instance.raw(), &mut raw)
        })?;
        Ok(SyncGroup {
            instance: Arc::clone(instance),
            raw,
        })
    }

    /// `mxlFlowSynchronizationGroupAddReader`. The reader must belong to the
    /// group's instance: libmxl removes a released reader only from the
    /// groups of its own instance.
    fn add(&mut self, reader: &FlowReader) -> Result<()> {
        if !Arc::ptr_eq(&self.instance, &reader.instance) {
            return Err(Error::Format(
                "reader and group belong to different instances".to_owned(),
            ));
        }
        // SAFETY: live group and reader on the same instance.
        check("mxlFlowSynchronizationGroupAddReader", unsafe {
            self.instance
                .api()
                .flow_synchronization_group_add_reader(self.raw, reader.raw)
        })
    }

    /// RT. `mxlFlowSynchronizationGroupWaitForDataAt`: OK once every
    /// reader's head reached the index of `timestamp`. Otherwise it blocks
    /// on a futex until `timeout_ns` passed, then returns
    /// `MXL_ERR_OUT_OF_RANGE_TOO_EARLY`, or `MXL_ERR_FLOW_INVALID` when the
    /// first reader found behind is on a replaced flow: libmxl checks
    /// validity only once the timeout expired.
    fn wait(&self, timestamp: u64, timeout_ns: u64) -> Status {
        // SAFETY: live group; its readers are live while this wrapper's
        // generation holds them.
        unsafe {
            self.instance
                .api()
                .flow_synchronization_group_wait_for_data_at(self.raw, timestamp, timeout_ns)
        }
    }
}

impl Drop for SyncGroup {
    fn drop(&mut self) {
        #[allow(clippy::disallowed_methods, reason = "supervisor side")]
        let _lock = self.instance.lock();
        // SAFETY: `raw` came from mxlCreateFlowSynchronizationGroup on this
        // instance and is released exactly once, here.
        unsafe {
            self.instance
                .api()
                .release_flow_synchronization_group(self.instance.raw(), self.raw)
        };
    }
}

/// The group wait that preceded one block, passed to [`Input::read`] and
/// [`Input::identify`] for that block only.
#[derive(Debug, PartialEq, Eq)]
pub enum GroupWait {
    /// `MXL_STATUS_OK`: the group checked every reader's head.
    Ok,
    /// The wait returned this status.
    Failed(Status),
    /// No wait preceded this block (catch-up).
    Skipped,
}

/// One input generation: a reader per attached slot and per flow a
/// re-route leaves, and an instance of its own whose group holds the
/// readers opened on it. A reader may be opened on another instance and
/// shared with other generations ([`Generation::insert`]); each reader
/// keeps its instance alive. Built on the supervisor side and handed to
/// the RT thread whole. Must not be dropped on the RT thread.
pub struct Generation {
    /// Holds the readers opened on the generation's own instance; one
    /// shared from another generation or opened on its own instance is in
    /// no group, which libmxl ties to one instance. The engine waits on
    /// [`Generation::behind`], not on the group.
    /// With the instance, none for a [`Generation::bare`] one.
    group: Option<SyncGroup>,
    inputs: Vec<Option<Arc<Input>>>,
    /// Per slot, the reader of the flow a re-route leaves, read until its
    /// crossfade ends.
    outgoing: Vec<Option<Arc<Input>>>,
    max_offset: i64,
    instance: Option<Arc<Instance>>,
}

/// An attached input slot.
pub struct Input {
    reader: FlowReader,
    block: u64,
    /// The flow channel each leg reads.
    legs: Vec<usize>,
    /// The flow channels the read needs: the highest of `legs` + 1.
    width: usize,
    offset: i64,
}

impl Generation {
    /// A generation with `slots` empty input slots, on a new instance.
    pub fn new(api: &Arc<MxlApi>, domain: &std::path::Path, slots: usize) -> Result<Generation> {
        let instance = Instance::new(api, domain)?;
        Ok(Generation {
            group: Some(SyncGroup::new(&instance)?),
            instance: Some(instance),
            ..Generation::bare(slots)
        })
    }

    /// A generation with `slots` empty input slots and no instance of its
    /// own: it makes no libmxl call, and takes only readers opened
    /// elsewhere ([`Generation::open`], [`Generation::insert`]).
    pub fn bare(slots: usize) -> Generation {
        Generation {
            group: None,
            inputs: (0..slots).map(|_| None).collect(),
            outgoing: (0..slots).map(|_| None).collect(),
            max_offset: 0,
            instance: None,
        }
    }

    /// libmxl's API, of a generation made with [`Generation::new`].
    pub fn api(&self) -> &MxlApi {
        self.instance
            .as_ref()
            .expect("a generation with an instance")
            .api()
    }

    /// True when both generations use the same libmxl instance.
    pub fn same_instance(&self, other: &Generation) -> bool {
        match (&self.instance, &other.instance) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            _ => false,
        }
    }

    /// Opens a reader reference for `slot`, checks the flow and adds the
    /// reader to the group. Refuses a flow whose configuration or
    /// definition is not 48 kHz float32 audio with the channels `cfg`
    /// reads (see [`InputConfig::flow_channels`]), or whose readable
    /// history is shorter than the read window
    /// `(maxCatchupBlocks + 2)B + margin + |offset|`. Returns the margin the
    /// producer needs when `timing.margin` is below
    /// `offset + maxCommitBatchSizeHint + B`.
    pub fn attach(
        &mut self,
        slot: usize,
        cfg: &InputConfig,
        timing: &Timing,
    ) -> Result<Option<u64>> {
        self.add(slot, cfg, timing, false)
    }

    /// [`Generation::attach`] for the flow `slot` leaves in a re-route: the
    /// reader is in the group and read by [`Generation::outgoing`].
    pub fn attach_outgoing(
        &mut self,
        slot: usize,
        cfg: &InputConfig,
        timing: &Timing,
    ) -> Result<Option<u64>> {
        self.add(slot, cfg, timing, true)
    }

    fn add(
        &mut self,
        slot: usize,
        cfg: &InputConfig,
        timing: &Timing,
        outgoing: bool,
    ) -> Result<Option<u64>> {
        if slot >= self.inputs.len() {
            return Err(Error::Format(format!("no input slot {slot}")));
        }
        let instance = (self.instance.as_ref())
            .ok_or_else(|| Error::Format("a generation without an instance".to_owned()))?;
        let (input, short) = Generation::open(instance, cfg, timing)?;
        self.insert(slot, Arc::new(input), outgoing)?;
        Ok(short)
    }

    /// Opens a reader of `cfg`'s flow on `instance`, a generation's
    /// ([`Generation::instance`]), and checks the flow as
    /// [`Generation::attach`] does, without adding it to the generation:
    /// [`Generation::insert`] does, on the thread that builds it. Makes only
    /// instance-level libmxl calls, so it may run on any thread meanwhile.
    /// Returns the input and the margin its producer needs, as `attach`
    /// does.
    pub fn open(
        instance: &Arc<Instance>,
        cfg: &InputConfig,
        timing: &Timing,
    ) -> Result<(Input, Option<u64>)> {
        let legs = legs(cfg)?;
        let reader = FlowReader::open(instance, cfg.flow_id)?;
        let info = reader.config()?;
        // SAFETY: both union members are structs of integers, so any bytes
        // are a valid value; libmxl fills `continuous` for audio, and the
        // values are used only once the format is checked to be audio.
        let cont = unsafe { info.__bindgen_anon_1.continuous };
        let rate = info.common.grainRate;
        if info.common.format != MXL_DATA_FORMAT_AUDIO
            || rate.denominator == 0
            || i128::from(rate.numerator) * i128::from(RATE.denominator)
                != i128::from(RATE.numerator) * i128::from(rate.denominator)
            || !fits(cfg, cont.channelCount)
        {
            return Err(Error::Format(format!(
                "flow {}: format {}, rate {}/{}, {} channels; expected audio, 48000/1, {}",
                cfg.flow_id,
                info.common.format,
                rate.numerator,
                rate.denominator,
                cont.channelCount,
                wanted(cfg)
            )));
        }
        check_flow_def(&instance.flow_def(cfg.flow_id)?, cfg)?;

        let buffer_length = u64::from(cont.bufferLength);
        let required = (timing.max_catchup_blocks + 2) * timing.block
            + timing.margin
            + cfg.offset.unsigned_abs();
        if required > buffer_length / 2 {
            return Err(Error::Window {
                required,
                buffer_length,
            });
        }
        let replay = required + timing.resync_budget;
        if replay > buffer_length / 2 {
            return Err(Error::History {
                required: replay,
                buffer_length,
            });
        }
        let needed = i128::from(cfg.offset)
            + i128::from(info.common.maxCommitBatchSizeHint)
            + i128::from(timing.block);
        let short = (i128::from(timing.margin) < needed).then_some(needed as u64);

        let input = Input {
            reader,
            block: timing.block,
            width: legs.iter().max().map_or(0, |&c| c + 1),
            legs,
            offset: cfg.offset,
        };
        Ok((input, short))
    }

    /// Puts `input` ([`Generation::open`]) into `slot`, or as the flow
    /// `slot` leaves in a re-route with `outgoing`: one opened on this
    /// generation's instance also into the group. Another generation may
    /// hold the same reader, the RT thread reading it meanwhile
    /// ([`FlowReader`]).
    pub fn insert(&mut self, slot: usize, input: Arc<Input>, outgoing: bool) -> Result<()> {
        if slot >= self.inputs.len() {
            return Err(Error::Format(format!("no input slot {slot}")));
        }
        if let (Some(instance), Some(group)) = (&self.instance, &mut self.group)
            && Arc::ptr_eq(instance, &input.reader.instance)
        {
            group.add(&input.reader)?;
        }
        if outgoing {
            self.outgoing[slot] = Some(input);
        } else {
            self.inputs[slot] = Some(input);
        }
        self.max_offset = self
            .inputs
            .iter()
            .chain(&self.outgoing)
            .flatten()
            .map(|i| i.offset)
            .max()
            .unwrap_or(0);
        Ok(())
    }

    pub fn input(&self, slot: usize) -> Option<&Input> {
        self.inputs.get(slot)?.as_deref()
    }

    /// The reader of the flow `slot` leaves in a re-route.
    pub fn outgoing(&self, slot: usize) -> Option<&Input> {
        self.outgoing.get(slot)?.as_deref()
    }

    /// [`Generation::input`] or, with `outgoing`, [`Generation::outgoing`],
    /// to share with another generation.
    pub fn shared(&self, slot: usize, outgoing: bool) -> Option<Arc<Input>> {
        let v = if outgoing {
            &self.outgoing
        } else {
            &self.inputs
        };
        v.get(slot)?.clone()
    }

    pub fn has_inputs(&self) -> bool {
        self.inputs
            .iter()
            .chain(&self.outgoing)
            .any(Option::is_some)
    }

    /// The largest `offsetSamples` of the attached inputs, 0 without any.
    pub fn max_offset(&self) -> i64 {
        self.max_offset
    }

    /// Supervisor side. [`Input::prefault`] on every attached input.
    /// Returns the pages touched.
    pub fn prefault(&self) -> Result<usize> {
        self.inputs
            .iter()
            .chain(&self.outgoing)
            .flatten()
            .map(|i| i.prefault())
            .sum()
    }

    /// RT. How many samples the slowest reader's head, the outgoing ones'
    /// included, is below `index`; 0 once every head reached it, the
    /// condition the group wait waits for. Read from the flows' shared
    /// memory alone. A head that cannot be read counts as reached; the
    /// read reports it.
    pub fn behind(&self, index: u64) -> u64 {
        self.inputs
            .iter()
            .chain(&self.outgoing)
            .flatten()
            .map(|i| i.reader.head().map_or(0, |head| index.saturating_sub(head)))
            .max()
            .unwrap_or(0)
    }

    /// RT. `mxlFlowSynchronizationGroupWaitForDataAt` on the generation's
    /// group: [`GroupWait::Ok`] once every reader's head reached the index
    /// of `timestamp`. Otherwise it blocks on a futex until `timeout_ns`
    /// passed, then fails with `MXL_ERR_OUT_OF_RANGE_TOO_EARLY`, or with
    /// `MXL_ERR_FLOW_INVALID` when the first reader found behind is on a
    /// replaced flow: libmxl checks validity only once the timeout expired.
    /// That check is a `stat()` of the flow's data file, which on a file
    /// system that waits for a missing name blocks for that wait once the
    /// flow is deleted.
    #[deprecated(
        note = "the engine waits on `Generation::reached`; the group wait's validity check can block"
    )]
    pub fn wait(&self, timestamp: u64, timeout_ns: u64) -> GroupWait {
        match self.group.as_ref().map(|g| g.wait(timestamp, timeout_ns)) {
            Some(MXL_STATUS_OK) => GroupWait::Ok,
            Some(status) => GroupWait::Failed(status),
            None => GroupWait::Skipped,
        }
    }
}

/// The flow channel each leg of `cfg` reads: `flowChannels`, or every
/// channel in order. Refuses a selection that is not `channels` long or
/// names a channel at or above [`MAX_FLOW_CHANNELS`].
pub fn legs(cfg: &InputConfig) -> Result<Vec<usize>> {
    let Some(sel) = cfg.flow_channels else {
        return Ok((0..cfg.channels as usize).collect());
    };
    if sel.len() != cfg.channels as usize {
        return Err(Error::Format(format!(
            "flow {}: flowChannels names {} channels, the input has {}",
            cfg.flow_id,
            sel.len(),
            cfg.channels
        )));
    }
    if let Some(c) = sel.iter().find(|&&c| c >= MAX_FLOW_CHANNELS) {
        return Err(Error::Format(format!(
            "flow {}: flowChannels index {c} is not below {MAX_FLOW_CHANNELS}",
            cfg.flow_id
        )));
    }
    Ok(sel.iter().map(|&c| c as usize).collect())
}

/// Whether a flow of `count` channels has the channels `cfg` reads:
/// exactly `channels` without a selection, else more than the highest
/// selected index.
fn fits(cfg: &InputConfig, count: u32) -> bool {
    match cfg.flow_channels {
        None => count == cfg.channels,
        Some(sel) => sel.iter().all(|&c| c < count),
    }
}

/// The channel count `cfg` wants, for an error.
fn wanted(cfg: &InputConfig) -> String {
    match cfg.flow_channels.and_then(|s| s.iter().max()) {
        None => format!("{} channels", cfg.channels),
        Some(&max) => format!("at least {} channels", max + 1),
    }
}

fn check_flow_def(def: &str, cfg: &InputConfig) -> Result<()> {
    let v: serde_json::Value = serde_json::from_str(def)
        .map_err(|e| Error::Format(format!("flow {}: definition: {e}", cfg.flow_id)))?;
    // The rate is checked on the normalised grainRate of the config info.
    // libmxl's flow parser reads numbers as doubles and defaults an absent
    // channel_count to 1.
    let count = v.get("channel_count").map_or(Some(1.0), |c| c.as_f64());
    let ok = v["format"] == "urn:x-nmos:format:audio"
        && v["media_type"] == "audio/float32"
        && v["bit_depth"].as_f64() == Some(32.0)
        && count.is_some_and(|n| {
            n.fract() == 0.0 && (0.0..=f64::from(u32::MAX)).contains(&n) && fits(cfg, n as u32)
        });
    if ok {
        Ok(())
    } else {
        Err(Error::Format(format!(
            "flow {}: definition is not 48 kHz float32 audio with {}: {def}",
            cfg.flow_id,
            wanted(cfg)
        )))
    }
}

/// The outcome of one [`Input::read`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The range was copied out.
    Copy,
    /// The range starts below the end of the last range read; not read.
    BelowLastRead,
    /// The head is below the range end; not read.
    Late,
    /// `MXL_ERR_OUT_OF_RANGE_TOO_LATE`: the range left the readable
    /// history, the producer is ahead of the clock or on another epoch.
    TooLate,
    /// `MXL_ERR_OUT_OF_RANGE_TOO_EARLY`: the head moved backwards between
    /// the check and the read.
    TooEarly,
    /// `MXL_ERR_FLOW_INVALID`: the flow was replaced or removed.
    Invalid,
    /// Any other status, such as `MXL_ERR_UNKNOWN` for an exception caught
    /// inside libmxl.
    Other(Status),
}

/// Outcome counts of one input slot.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub copied: u64,
    pub below_last_read: u64,
    pub late: u64,
    pub too_late: u64,
    pub too_early: u64,
    pub invalid: u64,
    pub other: u64,
    /// Validity probes made by [`Input::identify`].
    pub probes: u64,
}

/// The state of one input slot. It belongs to the slot, not to a reader,
/// and is carried across generation swaps.
#[derive(Clone, Debug, Default)]
pub struct SlotState {
    /// The exclusive end of the last range read from this slot.
    pub last_end: Option<u64>,
    /// The gain of the fade the engine applies to this input.
    pub fade: f32,
    /// The head seen by the last head check.
    pub head: u64,
    /// Consecutive late blocks with `head` unchanged.
    pub frozen_late_blocks: u32,
    /// Consecutive late blocks, `head` moving or not.
    pub late_blocks: u32,
    /// Set by an [`Outcome::Invalid`] read or by [`Input::identify`];
    /// cleared by the supervisor.
    pub invalid: bool,
    pub counters: Counters,
}

impl Input {
    pub fn reader(&self) -> &FlowReader {
        &self.reader
    }

    pub fn offset(&self) -> i64 {
        self.offset
    }

    /// Supervisor side. Reads one byte of every page of the flow's channel
    /// rings in this reader's mapping, so that the RT thread's reads take
    /// no first-touch page faults when memory is not locked with
    /// `MCL_FUTURE`. Gets the sample below the head with
    /// `mxlFlowReaderGetSamplesNonBlocking` and derives the start of
    /// channel 0's ring from the first fragment. Returns the pages touched.
    pub fn prefault(&self) -> Result<usize> {
        let info = self.reader.config()?;
        // SAFETY: both union members are structs of integers, so any bytes
        // are a valid value; attach checked the flow is audio, for which
        // libmxl fills `continuous`.
        let buffer_length = u64::from(unsafe { info.__bindgen_anon_1.continuous }.bufferLength);
        let head = self.reader.head().map_err(|status| Error::Mxl {
            call: "mxlFlowReaderGetRuntimeInfo",
            status,
        })?;
        let mut slice = WrappedMultiBufferSlice::default();
        check(
            "mxlFlowReaderGetSamplesNonBlocking",
            self.reader.samples(head, 1, &mut slice),
        )?;
        if buffer_length == 0 || slice.stride as u64 != buffer_length * size_of::<f32>() as u64 {
            return Err(Error::Format(format!(
                "ring of {buffer_length} samples with a channel stride of {} bytes",
                slice.stride
            )));
        }
        // libmxl places fragment 0 at sample (index - count) mod
        // bufferLength of channel 0's ring; the read above succeeded, so
        // head >= 1.
        let offset = ((head - 1) % buffer_length) as usize * size_of::<f32>();
        let len = slice.count * slice.stride;
        let mut pages = 0;
        // SAFETY: libmxl returned fragment 0 `offset` bytes into channel
        // 0's ring of the reader's mapping, and channel `c < slice.count`
        // starts `c * stride` bytes after channel 0, `stride` being the
        // whole ring of one channel (checked above). So `[base, base +
        // len)` is mapped while the reader lives, which it does for the
        // call. The reads are volatile byte reads, so no alignment is
        // assumed and none is elided.
        unsafe {
            let base = slice.base.fragments[0].pointer.cast::<u8>().sub(offset);
            for at in (0..len).step_by(PAGE).chain((len > 0).then(|| len - 1)) {
                std::ptr::read_volatile(base.add(at));
                pages += 1;
            }
        }
        Ok(pages)
    }

    /// The flow channel each leg reads.
    pub fn legs(&self) -> &[usize] {
        &self.legs
    }

    /// RT. Reads block `k`, the range `[kB + offset, (k + 1)B + offset)`,
    /// into `dst`, planar with B samples per leg, each leg copied from the
    /// flow channel it selects:
    /// 1. a range starting below `state.last_end` is not read;
    /// 2. unless `group` is [`GroupWait::Ok`], the head from
    ///    `mxlFlowReaderGetRuntimeInfo` is checked and a range ending above
    ///    it is late and not read. `group` is the group wait that preceded
    ///    this block, [`GroupWait::Skipped`] for a catch-up block;
    /// 3. `mxlFlowReaderGetSamplesNonBlocking((k + 1)B + offset, B)`, both
    ///    fragments copied out at once.
    ///
    /// `dst` must hold legs x B samples, else the outcome is
    /// `Other(MXL_ERR_INVALID_ARG)`, as it is for a flow that no longer
    /// has the selected channels. It is zeroed for every outcome but
    /// [`Outcome::Copy`].
    pub fn read(
        &self,
        state: &mut SlotState,
        k: u64,
        group: &GroupWait,
        dst: &mut [f32],
    ) -> Outcome {
        let outcome = self.read_range(state, k, group, dst);
        let c = &mut state.counters;
        match outcome {
            Outcome::Copy => c.copied += 1,
            Outcome::BelowLastRead => c.below_last_read += 1,
            Outcome::Late => c.late += 1,
            Outcome::TooLate => c.too_late += 1,
            Outcome::TooEarly => c.too_early += 1,
            Outcome::Invalid => {
                c.invalid += 1;
                state.invalid = true;
            }
            Outcome::Other(_) => c.other += 1,
        }
        if outcome != Outcome::Copy {
            dst.fill(0.0);
        }
        outcome
    }

    fn read_range(
        &self,
        state: &mut SlotState,
        k: u64,
        group: &GroupWait,
        dst: &mut [f32],
    ) -> Outcome {
        let b = self.block;
        let n = b as usize;
        if self.legs.len().checked_mul(n) != Some(dst.len()) {
            return Outcome::Other(MXL_ERR_INVALID_ARG);
        }
        let Some(end) = k
            .checked_add(1)
            .and_then(|k1| k1.checked_mul(b))
            .and_then(|e| e.checked_add_signed(self.offset))
        else {
            return Outcome::Other(MXL_ERR_INVALID_ARG);
        };
        let Some(start) = end.checked_sub(b) else {
            return Outcome::TooLate;
        };
        if state.last_end.is_some_and(|last| start < last) {
            return Outcome::BelowLastRead;
        }
        if *group == GroupWait::Ok {
            state.frozen_late_blocks = 0;
            state.late_blocks = 0;
        } else {
            let head = match self.reader.head() {
                Ok(head) => head,
                Err(status) => return Outcome::Other(status),
            };
            if end > head {
                if head == state.head {
                    state.frozen_late_blocks = state.frozen_late_blocks.saturating_add(1);
                } else {
                    state.frozen_late_blocks = 1;
                }
                state.late_blocks = state.late_blocks.saturating_add(1);
                state.head = head;
                return Outcome::Late;
            }
            state.head = head;
            state.frozen_late_blocks = 0;
            state.late_blocks = 0;
        }

        let mut slice = WrappedMultiBufferSlice::default();
        match self.reader.samples(end, n, &mut slice) {
            MXL_STATUS_OK => {}
            MXL_ERR_OUT_OF_RANGE_TOO_LATE => return Outcome::TooLate,
            MXL_ERR_OUT_OF_RANGE_TOO_EARLY => return Outcome::TooEarly,
            MXL_ERR_FLOW_INVALID => return Outcome::Invalid,
            status => return Outcome::Other(status),
        }
        if self.width > slice.count {
            return Outcome::Other(MXL_ERR_INVALID_ARG);
        }
        let [f0, f1] = slice.base.fragments;
        let width = size_of::<f32>();
        for (out, &ch) in dst.chunks_exact_mut(n).zip(&self.legs) {
            let (a, rest) = out.split_at_mut((f0.size / width).min(n));
            let second = (f1.size / width).min(rest.len());
            // SAFETY: libmxl returned two fragments of `f0.size` and
            // `f1.size` bytes inside channel 0 of the mapped ring, and
            // channel `ch < width <= slice.count` (checked above) lies
            // `ch * stride` bytes on. At most
            // those sizes are copied, into `out`, which holds `n` samples.
            // Bytes are copied, so no alignment is assumed.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    f0.pointer.cast::<u8>().add(ch * slice.stride),
                    a.as_mut_ptr().cast::<u8>(),
                    size_of_val(a),
                );
                std::ptr::copy_nonoverlapping(
                    f1.pointer.cast::<u8>().add(ch * slice.stride),
                    rest.as_mut_ptr().cast::<u8>(),
                    second * width,
                );
            }
        }
        state.last_end = Some(end);
        Outcome::Copy
    }

    /// RT, failure path. Identifies a failed input after a late block: on
    /// a group wait failed with `MXL_ERR_FLOW_INVALID`, which names no
    /// reader, or
    /// when the head has been frozen for [`FROZEN_PROBE_BLOCKS`] late blocks,
    /// probes the flow once with `mxlFlowReaderGetSamplesNonBlocking(head +
    /// 1, 1)`, which runs libmxl's validity check, and marks the slot
    /// invalid on `MXL_ERR_FLOW_INVALID`. A slot already marked is not
    /// probed. Returns true when it marked the slot. The validity check is
    /// a `stat()` of the flow's data file, which on a file system that
    /// waits for a missing name blocks for that wait once the flow is
    /// deleted.
    #[deprecated(
        note = "the engine prunes a late input and checks its flow on the supervisor side"
    )]
    #[allow(deprecated, reason = "FROZEN_PROBE_BLOCKS goes with this probe")]
    pub fn identify(&self, state: &mut SlotState, group: &GroupWait, outcome: Outcome) -> bool {
        if state.invalid
            || outcome != Outcome::Late
            || (*group != GroupWait::Failed(MXL_ERR_FLOW_INVALID)
                && state.frozen_late_blocks != FROZEN_PROBE_BLOCKS)
        {
            return false;
        }
        state.counters.probes += 1;
        state.invalid = self.reader.probe(state.head) == MXL_ERR_FLOW_INVALID;
        state.invalid
    }
}

/// The result of one [`PolledFlow::poll`].
#[derive(Debug)]
pub enum Poll {
    /// No reader could be opened.
    Absent(Error),
    /// The lookup found the flow and its references are being opened, on
    /// the poll reader's [`PollOpener`].
    Opening,
    /// libmxl reported the flow invalid: every reader reference was
    /// released and opening new ones was attempted.
    Invalid,
    /// The flow's head index.
    Head(u64),
}

/// Supervisor side. The poll reader of one flow that one or more pruned
/// slots read, on the supervisor's poll instance: one libmxl reader
/// reference per holding slot, all to the reader libmxl shares per flow
/// id. libmxl is called for the flow only once a [`Lookup`] found its
/// directory, and no call waits for a lookup or an open: a missing flow
/// is looked up on a thread of its own, at the pace of its [`Backoff`],
/// a flow whose head stopped is checked the same way before libmxl's
/// validity check, which looks up the flow's data file, and the
/// references are opened on a [`PollOpener`]'s thread, a libmxl open
/// having been measured at up to 23 ms, and taken by the first poll
/// after.
pub struct PolledFlow {
    instance: Arc<Instance>,
    flow_id: String,
    holders: usize,
    /// Empty, or one reference per holder; while an open runs, fewer.
    readers: Vec<FlowReader>,
    /// The open of the references the holders are short of, at most one.
    opening: Option<Opening>,
    /// Where the references are opened.
    opener: Option<PollOpener>,
    /// References let go and not yet taken ([`PolledFlow::take_released`]).
    released: Vec<Released>,
    /// References to the flow let go and not yet released, and its opens
    /// running.
    pending: Releases,
    lookup: Lookup,
    backoff: Backoff,
    /// The head the last poll read.
    head: Option<u64>,
    /// When a stopped head is checked: at once, then at the backoff's gaps.
    check: Backoff,
}

/// The references to one flow its poll readers let go and are not yet
/// released ([`Released`]), and the opens of the flow they run, shared by
/// the poll readers of that flow on an instance one after another
/// ([`PolledFlow::with_releases`]): none opens the flow while one is held
/// or runs, since libmxl would hand out the stale reader object for the
/// id. A reference has no deadline: one held, as by a reaper thread that
/// does not get to it, holds back this flow's poll reader alone.
#[derive(Clone, Debug, Default)]
pub struct Releases(Arc<AtomicUsize>);

impl Releases {
    /// The references let go and not yet released, and the opens running.
    pub fn pending(&self) -> usize {
        self.0.load(Ordering::Acquire)
    }
}

/// A reference a [`PolledFlow`] let go: dropping it releases the
/// reference, which can wait in the kernel.
pub struct Released {
    reader: Option<FlowReader>,
    pending: Arc<AtomicUsize>,
}

impl Drop for Released {
    fn drop(&mut self) {
        drop(self.reader.take());
        self.pending.fetch_sub(1, Ordering::Release);
    }
}

/// An open of a [`PolledFlow`]'s references running on a thread of its
/// own.
struct Opening {
    opened: Receiver<Opened>,
    /// Every reference was let go since it started: what it opens is let
    /// go too.
    stale: bool,
}

/// What an open thread opened, and the error that stopped it.
struct Opened {
    readers: Vec<FlowReader>,
    error: Option<Error>,
    /// Counted in the flow's [`Releases`] while the open runs and until
    /// what it opened is taken, or released where this is dropped: no
    /// other poll reader of the flow opens it meanwhile. Dropped after the
    /// readers.
    _running: Running,
}

/// The thread that opens poll readers' references, one open after
/// another in the order they are asked for, so that a poll asks for an
/// open without starting a thread: its creation is the only spawn, and
/// its thread ends once every clone is dropped. A poll reader given none
/// ([`PolledFlow::with_opener`]) starts its own at its first open.
#[derive(Clone)]
pub struct PollOpener(Sender<OpenJob>);

impl PollOpener {
    /// Starts the thread, named `samplez-poll-open`. Nothing joins it: it
    /// ends after the open it runs once every clone is dropped, at
    /// shutdown too.
    pub fn start() -> std::io::Result<PollOpener> {
        let (tx, jobs) = channel::<OpenJob>();
        std::thread::Builder::new()
            .name("samplez-poll-open".into())
            .spawn(move || jobs.into_iter().for_each(OpenJob::run))?;
        Ok(PollOpener(tx))
    }

    /// Test-only: an opener whose thread is gone.
    #[cfg(feature = "test-hooks")]
    pub fn gone() -> PollOpener {
        let (tx, jobs) = channel::<OpenJob>();
        drop(jobs);
        PollOpener(tx)
    }
}

/// An open a [`PolledFlow`] asks its [`PollOpener`] for.
struct OpenJob {
    instance: Arc<Instance>,
    flow_id: String,
    n: usize,
    reply: SyncSender<Opened>,
    running: Running,
}

impl OpenJob {
    /// Opens the references and replies; with the poll reader gone, what
    /// it opened is released here.
    fn run(self) {
        let OpenJob {
            instance,
            flow_id,
            n,
            reply,
            running,
        } = self;
        let mut readers = Vec::with_capacity(n);
        let mut error = None;
        while readers.len() < n {
            match FlowReader::open(&instance, &flow_id) {
                Ok(r) => readers.push(r),
                Err(e) => {
                    error = Some(e);
                    break;
                }
            }
        }
        let _ = reply.send(Opened {
            readers,
            error,
            _running: running,
        });
    }
}

/// One open counted in a flow's [`Releases`].
struct Running(Arc<AtomicUsize>);

impl Drop for Running {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Release);
    }
}

impl PolledFlow {
    /// A poll reader with one holder, wanted from now: its first lookup is
    /// started, and a later [`poll`](Self::poll) opens the flow once found.
    pub fn new(instance: &Arc<Instance>, flow_id: &str) -> Result<PolledFlow> {
        PolledFlow::looking_up_with(instance, flow_id, exists)
    }

    /// This poll reader, counting the references it lets go in `releases`,
    /// which an earlier poll reader of the flow on the instance may still
    /// hold references in.
    pub fn with_releases(mut self, releases: &Releases) -> PolledFlow {
        self.pending = releases.clone();
        self
    }

    /// This poll reader, opening its references on `opener`, which other
    /// poll readers may share.
    pub fn with_opener(mut self, opener: &PollOpener) -> PolledFlow {
        self.opener = Some(opener.clone());
        self
    }

    /// [`PolledFlow::new`] with the directory lookup `look`.
    #[doc(hidden)]
    pub fn looking_up_with(
        instance: &Arc<Instance>,
        flow_id: &str,
        look: Look,
    ) -> Result<PolledFlow> {
        crate::validate_id(flow_id)?;
        let now = Instant::now();
        let mut lookup = Lookup::new(flow_dir(instance.domain(), flow_id), look);
        lookup.start();
        Ok(PolledFlow {
            instance: Arc::clone(instance),
            flow_id: flow_id.to_owned(),
            holders: 1,
            readers: Vec::new(),
            opening: None,
            opener: None,
            released: Vec::new(),
            pending: Releases::default(),
            lookup,
            backoff: Backoff::new(now),
            head: None,
            check: Backoff::paced(now),
        })
    }

    pub fn flow_id(&self) -> &str {
        &self.flow_id
    }

    /// Adds a holder. A flow open gets the holder's reference opened on
    /// the [`PollOpener`]; a later [`poll`](Self::poll) takes it, or, when
    /// the open fails, lets every reference go and opens them all again.
    pub fn hold(&mut self) {
        self.holders += 1;
        self.top_up();
    }

    /// The flow is wanted anew, as by a re-route or raise of a slot that
    /// reads it: a missing flow is looked up without a gap again.
    pub fn want(&mut self) {
        self.want_at(Instant::now());
    }

    /// [`PolledFlow::want`] at `now`.
    #[doc(hidden)]
    pub fn want_at(&mut self, now: Instant) {
        self.backoff = Backoff::new(now);
        if self.readers.is_empty() {
            self.lookup.start();
        }
    }

    /// Removes a holder and lets its reference go. Returns the holders
    /// left; at 0 the poll reader is to be dropped.
    pub fn release(&mut self) -> usize {
        self.holders = self.holders.saturating_sub(1);
        self.let_go(self.readers.len().saturating_sub(self.holders));
        self.holders
    }

    /// The references let go since the last call, by a release, a flow
    /// turned invalid or an open that failed, for the caller to release
    /// where releasing can wait in the kernel.
    pub fn take_released(&mut self) -> Vec<Released> {
        std::mem::take(&mut self.released)
    }

    /// Lets the last `n` references go.
    fn let_go(&mut self, n: usize) {
        let from = self.readers.len() - n;
        for reader in self.readers.drain(from..) {
            self.pending.0.fetch_add(1, Ordering::Relaxed);
            self.released.push(Released {
                reader: Some(reader),
                pending: Arc::clone(&self.pending.0),
            });
        }
    }

    /// `maxCommitBatchSizeHint` of the flow, when a reader is open.
    pub fn commit_batch_hint(&self) -> Option<u64> {
        let info = self.readers.first()?.config().ok()?;
        Some(u64::from(info.common.maxCommitBatchSizeHint))
    }

    /// Has `n` references opened on the poll reader's [`PollOpener`],
    /// started here when it has none. When it could not be started, or its
    /// thread is gone, a later poll tries again, on one started then: a
    /// gone thread's opener is dropped.
    fn open(&mut self, n: usize) {
        if self.opener.is_none() {
            self.opener = PollOpener::start().ok();
        }
        let Some(opener) = &self.opener else {
            return;
        };
        let (reply, opened) = sync_channel(1);
        let running = Running(Arc::clone(&self.pending.0));
        running.0.fetch_add(1, Ordering::Relaxed);
        let job = OpenJob {
            instance: Arc::clone(&self.instance),
            flow_id: self.flow_id.clone(),
            n,
            reply,
            running,
        };
        if opener.0.send(job).is_ok() {
            self.opening = Some(Opening {
                opened,
                stale: false,
            });
        } else {
            self.opener = None;
        }
    }

    /// Opens the references the holders are short of while the flow is
    /// open, unless an open runs.
    fn top_up(&mut self) {
        if self.opening.is_none() && !self.readers.is_empty() && self.readers.len() < self.holders {
            self.open(self.holders - self.readers.len());
        }
    }

    /// Takes what the open running opened once it is done. One that
    /// failed lets every reference go, the opened ones included, and the
    /// flow is looked up anew: the error. One started before every
    /// reference was let go lets what it opened go.
    fn opened(&mut self, now: Instant) -> Option<Error> {
        let opened = match self.opening.as_ref()?.opened.try_recv() {
            Ok(opened) => opened,
            Err(TryRecvError::Empty) => return None,
            // The open thread ended without an answer.
            Err(TryRecvError::Disconnected) => {
                self.opening = None;
                return None;
            }
        };
        let stale = self.opening.take().is_some_and(|o| o.stale);
        let Opened {
            readers,
            error,
            _running,
        } = opened;
        if self.readers.is_empty() {
            self.head = None;
        }
        self.readers.extend(readers);
        if stale || error.is_some() {
            self.let_go(self.readers.len());
        } else {
            // Holders released while it ran.
            self.let_go(self.readers.len().saturating_sub(self.holders));
        }
        let error = error.filter(|_| !stale)?;
        self.backoff.missing(now);
        self.restart(now);
        Some(error)
    }

    /// The flow's head, opening the flow once a lookup found it.
    /// `Absent` until then, with the status libmxl gives a missing flow,
    /// and `Opening` while the open runs, on the [`PollOpener`]; the
    /// first poll after it ended reads the head, or `Absent` with the
    /// error of an open that failed.
    ///
    /// While the head moves, nothing else is checked. A head unchanged
    /// since the last poll is checked, at once and then with the gaps of a
    /// [`Backoff`] past its grace while it stays, with
    /// `mxlFlowReaderGetSamplesNonBlocking(head + 1, 1)`, which runs
    /// libmxl's validity check, once a lookup found the flow's directory
    /// still there; until it answers the head is reported unchecked. On
    /// `MXL_ERR_FLOW_INVALID`, or a directory gone, every reference is
    /// let go ([`PolledFlow::take_released`]) and the flow is looked up
    /// anew, so that a flow recreated under the same id is found (upstream
    /// mxl-gst sink, `handleInvalidFlow`). It is opened again only once
    /// every reference let go is released ([`Releases`]): until then libmxl
    /// hands out its stale reader object for the id.
    pub fn poll(&mut self) -> Poll {
        self.poll_at(Instant::now())
    }

    /// [`PolledFlow::poll`] at `now`, the time the backoff reads.
    #[doc(hidden)]
    pub fn poll_at(&mut self, now: Instant) -> Poll {
        if let Some(e) = self.opened(now) {
            return Poll::Absent(e);
        }
        if self.readers.is_empty() {
            if self.opening.as_ref().is_some_and(|o| !o.stale) {
                return Poll::Opening;
            }
            // A stale open running counts too.
            if self.pending.pending() != 0 {
                return Poll::Absent(Error::Mxl {
                    call: "mxlCreateFlowReader",
                    status: MXL_ERR_FLOW_INVALID,
                });
            }
            match self.lookup.answer() {
                Some(true) => {
                    self.open(self.holders);
                    if self.opening.is_some() {
                        return Poll::Opening;
                    }
                    return self.missing(now);
                }
                Some(false) => {
                    self.backoff.missing(now);
                    return self.missing(now);
                }
                None => return self.missing(now),
            }
        }
        // A holder added while an open ran, or whose open did not start.
        self.top_up();
        let head = match self.readers[0].head() {
            Ok(head) => head,
            Err(status) => {
                return Poll::Absent(Error::Mxl {
                    call: "mxlFlowReaderGetRuntimeInfo",
                    status,
                });
            }
        };
        let moved = self.head != Some(head);
        self.head = Some(head);
        if moved {
            // Not a check pending from an earlier stop.
            let _ = self.lookup.answer();
            self.check = Backoff::paced(now);
            return Poll::Head(head);
        }
        let valid = match self.lookup.answer() {
            Some(true) => self.readers[0].probe(head) != MXL_ERR_FLOW_INVALID,
            Some(false) => false,
            None => {
                if self.check.due(now) {
                    self.lookup.start();
                }
                return Poll::Head(head);
            }
        };
        if valid {
            // Checked again after a gap, while the head stays.
            self.check.checked(now);
        } else {
            self.let_go(self.readers.len());
            if let Some(o) = &mut self.opening {
                o.stale = true;
            }
            self.head = None;
            self.backoff = Backoff::new(now);
            self.lookup.start();
            return Poll::Invalid;
        }
        Poll::Head(head)
    }

    /// No reader open: starts a lookup when one is due.
    fn missing(&mut self, now: Instant) -> Poll {
        self.restart(now);
        Poll::Absent(Error::Mxl {
            call: "mxlCreateFlowReader",
            status: MXL_ERR_FLOW_NOT_FOUND,
        })
    }

    fn restart(&mut self, now: Instant) {
        if !self.lookup.running() && self.backoff.due(now) {
            self.lookup.start();
        }
    }
}

/// Supervisor side. Whether a pruned slot's polled head advances again.
#[derive(Clone, Copy, Debug, Default)]
pub struct Recovery {
    /// The head of the last poll.
    head: Option<u64>,
    /// `mxlTimestampToIndex` of the poll that last saw the head move.
    moved_at: Option<u64>,
    /// libmxl's clock at the first move of the current run.
    since: Option<u64>,
}

impl Recovery {
    /// True once the head has advanced for [`RECOVERY_NS`]: it moved, and
    /// it never stayed unchanged between two polls for more than `limit`
    /// samples, the prune limit an attached slot of the flow gets. How far
    /// the head is from the clock is the caller's to check: an attached
    /// slot is pruned for late blocks whether its head moves or not. No
    /// head starts the period again.
    pub fn update(&mut self, api: &MxlApi, head: Option<u64>, now: u64, limit: u64) -> bool {
        let Some(head) = head else {
            *self = Recovery::default();
            return false;
        };
        let at = timestamp_to_index(api, now);
        if self.head.is_some_and(|last| last != head) {
            self.moved_at = Some(at);
            self.since.get_or_insert(now);
        }
        self.head = Some(head);
        if self.moved_at.is_none_or(|m| at.saturating_sub(m) > limit) {
            self.moved_at = None;
            self.since = None;
            return false;
        }
        self.since
            .is_some_and(|since| now.saturating_sub(since) >= RECOVERY_NS)
    }
}
