//! The block timeline on libmxl's conversions.

mod common;

use common::{B, TIMING, api};
use samplez_mxl::time;

#[test]
fn deadlines_and_the_newest_due_block_agree() {
    let api = api();
    let m = TIMING.margin;
    let idx = |t| time::timestamp_to_index(&api, t);
    let now = time::now(&api);
    let k = time::newest_due_block(&api, now, B, m).unwrap();
    assert!(idx(time::deadline(&api, k, B, m)) <= idx(now));
    assert!(idx(time::deadline(&api, k + 1, B, m)) > idx(now));
    for k in [0, 1, 1_000_000, k] {
        let t = time::deadline(&api, k, B, m);
        assert_eq!(t, time::index_to_timestamp(&api, (k + 1) * B + m));
        assert_eq!(time::newest_due_block(&api, t, B, m), Some(k));
        let before = time::index_to_timestamp(&api, (k + 1) * B + m - 1);
        assert_eq!(time::newest_due_block(&api, before, B, m), k.checked_sub(1));
    }
    assert_eq!(time::newest_due_block(&api, 0, B, m), None);
}

#[test]
fn the_group_timestamp_adds_only_a_positive_offset() {
    let api = api();
    let at = |i| time::index_to_timestamp(&api, i);
    assert_eq!(time::group_timestamp(&api, 9, B, 0), at(10 * B));
    assert_eq!(time::group_timestamp(&api, 9, B, 17), at(10 * B + 17));
    assert_eq!(time::group_timestamp(&api, 9, B, -17), at(10 * B));
}

#[test]
fn sleep_until_returns_at_the_timestamp() {
    let api = api();
    let target = time::now(&api) + 20_000_000;
    time::sleep_until(&api, target);
    assert!(time::now(&api) >= target);
    assert_eq!(
        time::timestamp_to_index(&api, time::index_to_timestamp(&api, 123_456_789)),
        123_456_789
    );
}
