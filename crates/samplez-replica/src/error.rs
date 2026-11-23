//! Why the replica's API refused or failed.

use std::fmt;
use std::io;
use std::time::Duration;

#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// A raft heartbeat of zero.
    HeartbeatZero,
    /// A raft heartbeat not below the election timeout minimum.
    HeartbeatNotBelowElection {
        heartbeat: Duration,
        election_min: Duration,
    },
    /// An election timeout minimum not below its maximum.
    ElectionRange { min: Duration, max: Duration },
    /// The engine's ramp length and block size are not the replica's
    /// limits', at which it follows gain ramps.
    RampConfig {
        replica_ramp: u32,
        replica_block: u64,
        engine_ramp: u32,
        engine_block: u64,
    },
    /// A `Route` or `MasterEnable` appended to a product that drives an
    /// engine, which owns every input's flow.
    ProductOwnsInputs,
    /// Contact was started before.
    ContactRunning,
    /// A count of voters that is not odd, or not one the node ids hold.
    VoterCount { replicas: u16, witness: bool },
    /// The store, or the random source, could not be read.
    Io(io::Error),
    /// openraft refused the configuration, did not start, or did not
    /// initialize the set.
    Raft(Box<dyn std::error::Error + Send + Sync>),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::HeartbeatZero => f.write_str("raft heartbeat must not be zero"),
            Error::HeartbeatNotBelowElection {
                heartbeat,
                election_min,
            } => write!(
                f,
                "raft heartbeat ({heartbeat:?}) must be below the election timeout minimum \
                 ({election_min:?})"
            ),
            Error::ElectionRange { min, max } => write!(
                f,
                "raft election timeout minimum ({min:?}) must be below its maximum ({max:?})"
            ),
            Error::RampConfig {
                replica_ramp,
                replica_block,
                engine_ramp,
                engine_block,
            } => write!(
                f,
                "the replica's ramp_samples {replica_ramp} and block {replica_block} are not the \
                 engine's rampSamples {engine_ramp} and blockSize {engine_block}: they must be \
                 equal"
            ),
            Error::ProductOwnsInputs => f.write_str("the product owns the inputs' flows"),
            Error::ContactRunning => f.write_str("contact runs already"),
            Error::VoterCount { replicas, witness } => write!(
                f,
                "{replicas} replicas {} the witness are {} voters; the count must be odd",
                if *witness { "with" } else { "without" },
                u32::from(*replicas) + u32::from(*witness)
            ),
            Error::Io(e) => e.fmt(f),
            Error::Raft(e) => e.fmt(f),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(e) => Some(e),
            Error::Raft(e) => Some(e.as_ref()),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

impl Error {
    pub(crate) fn raft(e: impl std::error::Error + Send + Sync + 'static) -> Error {
        Error::Raft(Box::new(e))
    }
}
