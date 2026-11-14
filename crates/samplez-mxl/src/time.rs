//! libmxl's clock and index conversions at [`RATE`], and the block
//! timeline built on them. Every conversion is libmxl's own; nothing here
//! converts between time and index itself.
//!
//! Block `k` covers the indices `[kB, (k + 1)B)`. Index arithmetic
//! saturates at `u64::MAX` instead of overflowing.

use mxl_sys::Rational;

use crate::{MxlApi, SAMPLE_RATE};

pub const RATE: Rational = Rational {
    numerator: SAMPLE_RATE,
    denominator: 1,
};

/// RT. `mxlGetTime`: the current time of libmxl's clock (CLOCK_TAI on
/// Linux) in nanoseconds since the epoch.
pub fn now(api: &MxlApi) -> u64 {
    // SAFETY: mxlGetTime has no preconditions.
    unsafe { api.get_time() }
}

/// RT. `mxlTimestampToIndex` at [`RATE`].
pub fn timestamp_to_index(api: &MxlApi, timestamp: u64) -> u64 {
    // SAFETY: `RATE` is a valid rational for the duration of the call.
    unsafe { api.timestamp_to_index(&RATE, timestamp) }
}

/// RT. `mxlIndexToTimestamp` at [`RATE`].
pub fn index_to_timestamp(api: &MxlApi, index: u64) -> u64 {
    // SAFETY: `RATE` is a valid rational for the duration of the call.
    unsafe { api.index_to_timestamp(&RATE, index) }
}

/// RT. `mxlSleepUntil`: sleeps until libmxl's clock reaches `timestamp`.
pub fn sleep_until(api: &MxlApi, timestamp: u64) {
    // SAFETY: mxlSleepUntil has no preconditions.
    unsafe { api.sleep_until(timestamp) }
}

/// RT. `T_k`, the time block `k` is due:
/// `mxlIndexToTimestamp((k + 1)B + margin)`.
pub fn deadline(api: &MxlApi, k: u64, block: u64, margin: u64) -> u64 {
    index_to_timestamp(api, end_index(k, block).saturating_add(margin))
}

/// RT. The newest block due at `now`,
/// `floor((mxlTimestampToIndex(now) - margin) / B) - 1`; `None` before the
/// first block is due. mxlTimestampToIndex rounds to the nearest index, so
/// the block's `T_k` can lie up to half a sample period after `now`.
pub fn newest_due_block(api: &MxlApi, now: u64, block: u64, margin: u64) -> Option<u64> {
    timestamp_to_index(api, now)
        .checked_sub(margin)?
        .checked_div(block)?
        .checked_sub(1)
}

/// RT. The timestamp the input group waits for before block `k`:
/// `mxlIndexToTimestamp((k + 1)B + max(0, max_offset))`, `max_offset` being
/// the largest `offsetSamples` of the generation's inputs.
pub fn group_timestamp(api: &MxlApi, k: u64, block: u64, max_offset: i64) -> u64 {
    index_to_timestamp(
        api,
        end_index(k, block).saturating_add(max_offset.max(0).unsigned_abs()),
    )
}

/// `(k + 1)B`, saturating.
fn end_index(k: u64, block: u64) -> u64 {
    k.saturating_add(1).saturating_mul(block)
}
