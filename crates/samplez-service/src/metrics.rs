//! A metrics registry rendered in the Prometheus text format.
//!
//! [`Counter::add`] and [`Histogram::observe`] make no allocation and take
//! no lock, so the RT thread may call them. Registration and rendering lock
//! the registry and allocate. Registering a series that exists returns it.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct Metrics(Arc<Mutex<Vec<Family>>>);

struct Family {
    name: String,
    help: String,
    series: Vec<(Vec<(String, String)>, Series)>,
}

#[derive(Clone)]
enum Series {
    Counter(Counter),
    Histogram(Histogram),
}

impl Series {
    fn kind(&self) -> &'static str {
        match self {
            Series::Counter(_) => "counter",
            Series::Histogram(_) => "histogram",
        }
    }
}

#[derive(Clone)]
pub struct Counter(Arc<AtomicU64>);

impl Counter {
    pub fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    pub fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct Histogram(Arc<HistogramInner>);

struct HistogramInner {
    bounds: Box<[f64]>,
    /// One per bound plus `+Inf`; not cumulative.
    buckets: Box<[AtomicU64]>,
    /// The bits of an `f64`.
    sum: AtomicU64,
}

impl Histogram {
    /// NaN is not counted. With one thread observing, as the RT thread does
    /// with its own histograms, the update of the sum succeeds at the first
    /// attempt; concurrent observers retry it.
    pub fn observe(&self, value: f64) {
        if value.is_nan() {
            return;
        }
        let h = &self.0;
        let i = h.bounds.iter().take_while(|b| value > **b).count();
        h.buckets[i].fetch_add(1, Ordering::Relaxed);
        let mut sum = h.sum.load(Ordering::Relaxed);
        loop {
            let next = (f64::from_bits(sum) + value).to_bits();
            match h
                .sum
                .compare_exchange_weak(sum, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(current) => sum = current,
            }
        }
    }
}

fn valid_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == ':')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

impl Metrics {
    /// The series of `name` with `labels`, created when absent. Panics when
    /// the name or a label name is invalid, or when `name` is registered
    /// with another type.
    pub fn counter(&self, name: &str, help: &str, labels: &[(&str, &str)]) -> Counter {
        let counter = Counter(Arc::default());
        match self.register(name, help, labels, Series::Counter(counter.clone())) {
            Series::Counter(c) => c,
            Series::Histogram(_) => unreachable!("register checks the type"),
        }
    }

    /// `bounds` are the bucket upper bounds, finite and ascending; `+Inf` is
    /// added. Panics like [`Metrics::counter`], when `bounds` are not
    /// finite and ascending, and when the series exists with other bounds.
    pub fn histogram(
        &self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        bounds: &[f64],
    ) -> Histogram {
        assert!(
            bounds.iter().all(|b| b.is_finite()) && bounds.windows(2).all(|w| w[0] < w[1]),
            "histogram bounds of {name} are not finite and ascending"
        );
        let histogram = Histogram(Arc::new(HistogramInner {
            bounds: bounds.into(),
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum: AtomicU64::new(0f64.to_bits()),
        }));
        match self.register(name, help, labels, Series::Histogram(histogram)) {
            Series::Histogram(h) => {
                assert!(
                    *h.0.bounds == *bounds,
                    "{name} registered with other bounds"
                );
                h
            }
            Series::Counter(_) => unreachable!("register checks the type"),
        }
    }

    /// Removes the series of `name` with `labels`; the handles still count
    /// but are no longer rendered.
    pub fn remove(&self, name: &str, labels: &[(&str, &str)]) {
        let mut families = self.0.lock().expect("metrics lock poisoned");
        for family in families.iter_mut().filter(|f| f.name == name) {
            family.series.retain(|(l, _)| !same_labels(l, labels));
        }
        families.retain(|f| !f.series.is_empty());
    }

    /// The registered series, or `series` once registered.
    fn register(&self, name: &str, help: &str, labels: &[(&str, &str)], series: Series) -> Series {
        assert!(valid_name(name), "invalid metric name {name:?}");
        for (label, _) in labels {
            assert!(
                valid_name(label) && !label.contains(':') && *label != "le",
                "invalid label name {label:?} on {name}"
            );
        }
        let labels: Vec<(String, String)> = labels
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut families = self.0.lock().expect("metrics lock poisoned");
        let family = match families.iter_mut().position(|f| f.name == name) {
            Some(i) => &mut families[i],
            None => {
                families.push(Family {
                    name: name.into(),
                    help: help.into(),
                    series: Vec::new(),
                });
                families.last_mut().expect("just pushed")
            }
        };
        if let Some((_, first)) = family.series.first() {
            assert_eq!(
                first.kind(),
                series.kind(),
                "{name} registered as another type"
            );
        }
        if let Some((_, existing)) = family.series.iter().find(|(l, _)| *l == labels) {
            return existing.clone();
        }
        family.series.push((labels, series.clone()));
        series
    }

    pub fn render(&self) -> String {
        let families = self.0.lock().expect("metrics lock poisoned");
        let mut out = String::new();
        for family in families.iter() {
            let kind = family.series.first().map_or("untyped", |(_, s)| s.kind());
            let help = family.help.replace('\\', "\\\\").replace('\n', "\\n");
            writeln!(out, "# HELP {} {help}", family.name).unwrap();
            writeln!(out, "# TYPE {} {kind}", family.name).unwrap();
            for (labels, series) in &family.series {
                let name = &family.name;
                match series {
                    Series::Counter(c) => {
                        writeln!(out, "{name}{} {}", label_set(pairs(labels)), c.get()).unwrap();
                    }
                    Series::Histogram(Histogram(h)) => {
                        let mut cumulative = 0;
                        for (i, bucket) in h.buckets.iter().enumerate() {
                            cumulative += bucket.load(Ordering::Relaxed);
                            let le = h.bounds.get(i).map_or("+Inf".into(), |b| float(*b));
                            let set = label_set(pairs(labels).chain([("le", le.as_str())]));
                            writeln!(out, "{name}_bucket{set} {cumulative}").unwrap();
                        }
                        let sum = float(f64::from_bits(h.sum.load(Ordering::Relaxed)));
                        let set = label_set(pairs(labels));
                        writeln!(out, "{name}_sum{set} {sum}").unwrap();
                        writeln!(out, "{name}_count{set} {cumulative}").unwrap();
                    }
                }
            }
        }
        out
    }
}

/// A float as the Prometheus text format writes it.
fn float(v: f64) -> String {
    match v {
        f64::INFINITY => "+Inf".into(),
        f64::NEG_INFINITY => "-Inf".into(),
        v if v.is_nan() => "NaN".into(),
        v => v.to_string(),
    }
}

fn same_labels(registered: &[(String, String)], labels: &[(&str, &str)]) -> bool {
    registered.len() == labels.len()
        && registered
            .iter()
            .zip(labels)
            .all(|((k, v), (k2, v2))| k == k2 && v == v2)
}

fn pairs(labels: &[(String, String)]) -> impl Iterator<Item = (&str, &str)> {
    labels.iter().map(|(k, v)| (k.as_str(), v.as_str()))
}

/// `{k="v",...}` with the values escaped; empty for no labels.
pub(crate) fn label_set<'a>(labels: impl IntoIterator<Item = (&'a str, &'a str)>) -> String {
    let pairs: Vec<String> = labels
        .into_iter()
        .map(|(k, v)| {
            let v = v
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            format!("{k}=\"{v}\"")
        })
        .collect();
    if pairs.is_empty() {
        String::new()
    } else {
        format!("{{{}}}", pairs.join(","))
    }
}
