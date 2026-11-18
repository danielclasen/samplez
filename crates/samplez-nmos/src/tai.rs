//! TAI times as NMOS writes them: `<seconds>:<nanoseconds>` since the
//! SMPTE ST 2059 epoch, for resource versions and IS-05 activation times.

use std::fmt;
use std::str::FromStr;
use std::time::Duration;

const NANOS: u64 = 1_000_000_000;

/// A TAI time: seconds and nanoseconds since 1970-01-01T00:00:00 TAI.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Tai {
    pub sec: u64,
    /// Below 1e9.
    pub nsec: u32,
}

impl Tai {
    pub const fn new(sec: u64, nsec: u32) -> Tai {
        Tai { sec, nsec }
    }

    pub fn from_nanos(nanos: u128) -> Tai {
        Tai {
            sec: (nanos / NANOS as u128) as u64,
            nsec: (nanos % NANOS as u128) as u32,
        }
    }

    pub fn as_nanos(self) -> u128 {
        self.sec as u128 * NANOS as u128 + self.nsec as u128
    }

    /// The next representable time, one nanosecond later.
    pub fn next(self) -> Tai {
        Tai::from_nanos(self.as_nanos() + 1)
    }

    pub fn after(self, d: Duration) -> Tai {
        Tai::from_nanos(self.as_nanos() + d.as_nanos())
    }

    /// `self - earlier`, zero when `earlier` is not earlier.
    pub fn since(self, earlier: Tai) -> Duration {
        let n = self.as_nanos().saturating_sub(earlier.as_nanos());
        Duration::new((n / NANOS as u128) as u64, (n % NANOS as u128) as u32)
    }
}

impl fmt::Display for Tai {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.sec, self.nsec)
    }
}

/// Parses `^[0-9]+:[0-9]+$` with the nanoseconds below 1e9.
impl FromStr for Tai {
    type Err = ();

    fn from_str(s: &str) -> Result<Tai, ()> {
        let (sec, nsec) = s.split_once(':').ok_or(())?;
        let digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
        if !digits(sec) || !digits(nsec) {
            return Err(());
        }
        let sec = sec.parse::<u64>().map_err(|_| ())?;
        let nsec = nsec.parse::<u32>().map_err(|_| ())?;
        if nsec as u64 >= NANOS {
            return Err(());
        }
        Ok(Tai { sec, nsec })
    }
}

/// TAI - UTC since 2017-01-01, and until the next leap second.
pub(crate) const TAI_UTC: i64 = 37;

/// The node's TAI clock, read for resource versions and activations.
pub trait TaiClock: Send + Sync + 'static {
    fn now(&self) -> Tai;

    /// How many seconds the clock reads ahead of the system's UTC clock,
    /// where it can tell; checked once at start.
    fn utc_offset(&self) -> Option<i64> {
        None
    }
}

/// The warning for a clock `offset` seconds ahead of UTC, `None` when that
/// is TAI - UTC give or take the second a reading may cross.
pub(crate) fn offset_warning(offset: i64) -> Option<String> {
    let off = TAI_UTC - offset;
    (off.abs() > 1).then(|| {
        format!(
            "the TAI clock reads {offset} s ahead of UTC, not {TAI_UTC} s: the kernel's TAI \
             offset is unset or wrong, so resource versions and IS-05 activation times are \
             {off} s off TAI and an absolute activation fires that far from its time"
        )
    })
}

/// `CLOCK_TAI`, the clock libmxl's `mxlGetTime` reads. It is TAI only
/// while the kernel's TAI offset is set.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemTai;

impl TaiClock for SystemTai {
    fn now(&self) -> Tai {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        // SAFETY: `ts` is writable; CLOCK_TAI exists on Linux.
        unsafe { libc::clock_gettime(libc::CLOCK_TAI, &mut ts) };
        Tai::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }

    fn utc_offset(&self) -> Option<i64> {
        let read = |clock| {
            let mut ts = libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            };
            // SAFETY: `ts` is writable; both clocks exist on Linux.
            unsafe { libc::clock_gettime(clock, &mut ts) };
            ts.tv_sec as i128 * NANOS as i128 + ts.tv_nsec as i128
        };
        let ns = read(libc::CLOCK_TAI) - read(libc::CLOCK_REALTIME);
        Some(((ns + NANOS as i128 / 2).div_euclid(NANOS as i128)) as i64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_through_the_nmos_string() {
        for t in [Tai::new(0, 0), Tai::new(1_700_000_037, 999_999_999)] {
            assert_eq!(t.to_string().parse::<Tai>(), Ok(t));
        }
        assert_eq!(Tai::new(5, 7).to_string(), "5:7");
    }

    #[test]
    fn refuses_what_the_pattern_or_the_range_refuses() {
        for s in [
            "",
            ":",
            "1:",
            ":1",
            "1:2:3",
            "-1:0",
            "1:-0",
            "a:0",
            "1:1000000000",
            " 1:0",
        ] {
            assert!(s.parse::<Tai>().is_err(), "{s}");
        }
        assert_eq!("0001:000000002".parse::<Tai>(), Ok(Tai::new(1, 2)));
    }

    #[test]
    fn arithmetic_carries_the_nanoseconds() {
        assert_eq!(Tai::new(1, 999_999_999).next(), Tai::new(2, 0));
        assert_eq!(
            Tai::new(1, 900_000_000).after(Duration::from_millis(200)),
            Tai::new(2, 100_000_000)
        );
        assert_eq!(
            Tai::new(2, 100_000_000).since(Tai::new(1, 900_000_000)),
            Duration::from_millis(200)
        );
        assert_eq!(Tai::new(1, 0).since(Tai::new(2, 0)), Duration::ZERO);
    }

    #[test]
    fn the_system_clock_reads_clock_tai() {
        let mut real = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let tai = SystemTai.now();
        // SAFETY: `real` is writable.
        unsafe { libc::clock_gettime(libc::CLOCK_REALTIME, &mut real) };
        // The offset is the kernel's, 0 when unset; the second read may
        // have crossed into the next second.
        let offset = tai.sec as i64 - real.tv_sec;
        assert!((-1..=60).contains(&offset), "{offset}");
        let measured = SystemTai.utc_offset().unwrap();
        assert!((offset - measured).abs() <= 1, "{offset} {measured}");
    }

    #[test]
    fn an_offset_other_than_tai_utc_warns_with_its_consequence() {
        for ok in [36, 37, 38] {
            assert_eq!(offset_warning(ok), None, "{ok}");
        }
        let w = offset_warning(0).unwrap();
        assert!(w.contains("reads 0 s ahead of UTC, not 37 s"), "{w}");
        assert!(w.contains("activation times are 37 s off TAI"), "{w}");
        assert!(offset_warning(-37).is_some());
    }
}
