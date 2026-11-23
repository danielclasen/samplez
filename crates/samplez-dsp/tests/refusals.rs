//! What a refused parameter says, word for word: the messages reach the
//! products' clients, so a change to them is a change to the API.

use samplez_dsp::{DuckParam, GATE_OFF, GateParams, LimiterParams, StripParam, ToneParams};

fn refused(r: Result<(), impl std::fmt::Display>) -> String {
    match r {
        Ok(()) => panic!("taken"),
        Err(e) => e.to_string(),
    }
}

#[test]
fn a_refused_limiter_names_the_value_and_its_range() {
    let limiter = |ceiling_db, release_ms| LimiterParams {
        enabled: true,
        ceiling_db,
        release_ms,
    };
    assert_eq!(
        refused(limiter(1.5, 50.0).check()),
        "limiter ceiling 1.5 is outside -20..=0"
    );
    assert_eq!(
        refused(limiter(-1.0, 0.5).check()),
        "limiter release 0.5 is outside 1..=1000"
    );
    assert_eq!(
        refused(limiter(f32::NAN, 50.0).check()),
        "limiter ceiling NaN is outside -20..=0"
    );
}

#[test]
fn a_refused_tone_names_the_value_and_its_range() {
    let tone = |frequency_hz, level_db| ToneParams {
        frequency_hz,
        level_db,
        ident: false,
    };
    assert_eq!(
        refused(tone(10, -20.0).check()),
        "tone frequency 10 Hz is outside 20..=20000"
    );
    assert_eq!(
        refused(tone(1000, 3.0).check()),
        "tone level 3 dBFS is outside -60..=0"
    );
}

#[test]
fn a_refused_strip_parameter_names_what_is_missing_or_out_of_range() {
    assert_eq!(
        refused(StripParam::LegTrim { leg: 9, db: 0.0 }.check(2)),
        "no leg 9 on a strip of 2"
    );
    assert_eq!(
        refused(StripParam::Trim { db: f32::INFINITY }.check(2)),
        format!(
            "trim inf is outside {}..={}",
            samplez_dsp::strip::TRIM_DB.start(),
            samplez_dsp::strip::TRIM_DB.end()
        )
    );
    let gate = GateParams {
        threshold_db: 6.0,
        ..GATE_OFF
    };
    assert_eq!(
        refused(StripParam::Gate(gate).check(2)),
        "gate threshold 6 is outside -80..=0"
    );
    let band = samplez_dsp::strip::DEFAULT_BANDS[0];
    assert_eq!(
        refused(
            StripParam::EqBand {
                band: 99,
                value: band
            }
            .check(2)
        ),
        "no eq band 99"
    );
}

#[test]
fn a_refused_duck_parameter_names_the_key() {
    assert_eq!(refused(DuckParam::Unkey { key: 99 }.check()), "no key 99");
    assert_eq!(
        refused(
            DuckParam::Key {
                key: 0,
                legs: 9,
                gain: 1.0
            }
            .check()
        ),
        "a key of 9 legs, not 1 to 8"
    );
}
