# Changelog

## [0.26.0](https://github.com/danielclasen/samplez/compare/v0.25.0...v0.26.0) (2026-11-23)


### ⚠ BREAKING CHANGES

* **replica:** peer::LinkCounts has new public fields reports_sent, report_bytes, reports_answered, report_round_trip and report_encode, so code that builds it with a struct literal must set them.

### Features

* **engine:** keep the other threads off the RT CPU's SMT siblings ([#191](https://github.com/danielclasen/samplez/issues/191)) ([6303700](https://github.com/danielclasen/samplez/commit/630370078fd5c65b5f38a9a0986cf2b9eb944d92))
* **replica:** count report sizes, round trips and contact loop CPU ([e68139c](https://github.com/danielclasen/samplez/commit/e68139c428a3dc91d4a2ede2ba740952741fc456))

## [0.25.0](https://github.com/danielclasen/samplez/compare/v0.24.0...v0.25.0) (2026-11-22)


### ⚠ BREAKING CHANGES

* **dsp:** CompParams has a public mix field, COMP_OFF carries a full mix, and samplez_replica::PROTOCOL is 7.

### Features

* **dsp:** add a parallel mix to the compressor ([498f993](https://github.com/danielclasen/samplez/commit/498f99332616d96a1a1257aa79d2171239a74b2a))

## [0.24.0](https://github.com/danielclasen/samplez/compare/v0.23.1...v0.24.0) (2026-11-22)


### ⚠ BREAKING CHANGES

* **replica:** contact::PeerMetrics has a new public field, last_report_age, so code that builds it with a struct literal must set it.

### Features

* **replica:** report the age of each peer's last direct report ([e9961c0](https://github.com/danielclasen/samplez/commit/e9961c0c9b781c63b496ff588d574a08000aa5df))

## [0.23.1](https://github.com/danielclasen/samplez/compare/v0.23.0...v0.23.1) (2026-11-22)


### Features

* **engine:** report an input whose head is off the MXL clock ([#184](https://github.com/danielclasen/samplez/issues/184)) ([572675c](https://github.com/danielclasen/samplez/commit/572675ce65a7ceffa440bbd010a35a03bb3d6d26))

## [0.23.0](https://github.com/danielclasen/samplez/compare/v0.22.0...v0.23.0) (2026-11-22)


### ⚠ BREAKING CHANGES

* **engine:** Entry::PreStripMeter, Engine::set_pre_strip_meter, Targets::pre_strip_meters and ProductState::pre_strip_meters are new, a configured pre-strip meter starts off until a PreStripMeter entry switches it on, and samplez_replica::PROTOCOL is 6.
* **engine:** samplez::COMPUTE_VERSION 3: an input's delay line runs before its strip and pre-strip taps, so replicas of 0.22 and this release do not run as peers.

### Features

* **engine:** switch an input's pre-strip meter on at run time ([12e2492](https://github.com/danielclasen/samplez/commit/12e24924c748f5aeda50d4fbeba7ed79ccb9824d))


### Bug Fixes

* **engine:** run an input's delay before its strip and pre-strip taps ([#181](https://github.com/danielclasen/samplez/issues/181)) ([65e9179](https://github.com/danielclasen/samplez/commit/65e9179f522b3467a8a5242a92dfc8c5ee669971))

## [0.22.0](https://github.com/danielclasen/samplez/compare/v0.21.0...v0.22.0) (2026-11-21)


### ⚠ BREAKING CHANGES

* **replica:** resync::Metrics has a new public field, rebuilt_writing_nothing, so code that builds it with a struct literal must set it.

### Bug Fixes

* **replica:** rebuild a diverged replica with every output disabled ([#179](https://github.com/danielclasen/samplez/issues/179)) ([b1c6af0](https://github.com/danielclasen/samplez/commit/b1c6af0d6ed67938202e74642a8cabd34080b122))

## [0.21.0](https://github.com/danielclasen/samplez/compare/v0.20.0...v0.21.0) (2026-11-21)


### ⚠ BREAKING CHANGES

* **engine:** config::Input has a new public field, pre_strip_rows, so code that builds it with a struct literal must set it.
* **engine:** config::Input has a new public field, pre_strip_meter, and InputStatus the public field pre_strip_meters, so code that builds either with a struct literal must set them.
* **engine:** `Entry::OutputDelay`, `Engine::set_output_delay` and `config::Output::max_delay_samples` are new, `Targets` and `ProductState` gain the field `output_delays`, and `delayBudgetSamples` now counts the outputs' delay lines too.
* **engine:** `Entry::OutputStrip`, `Engine::set_output_strip` and `config::Output::strip` are new, and `Targets` and `ProductState` gain the field `output_strips`. `ProductState` now implements `Default`. `samplez_replica::PROTOCOL` is 5.

### Features

* **engine:** add an opt-in channel strip on outputs ([#173](https://github.com/danielclasen/samplez/issues/173)) ([98a82bc](https://github.com/danielclasen/samplez/commit/98a82bc9f777e5e6434c7c232cf1b08c12ed2654))
* **engine:** add an opt-in delay line on outputs ([#174](https://github.com/danielclasen/samplez/issues/174)) ([9be6d3d](https://github.com/danielclasen/samplez/commit/9be6d3d268c05c322bc9dc9abe6ab29fd60ccc83))
* **engine:** feed an input to the matrix before its strip where asked ([6944b5d](https://github.com/danielclasen/samplez/commit/6944b5d151513cd03331ac95a4f0cb53c03934f8))
* **engine:** meter an input before its strip where its config asks ([0e8ffef](https://github.com/danielclasen/samplez/commit/0e8ffef77c5ed6fccd9c83662da000e0ab05789d))

## [0.20.0](https://github.com/danielclasen/samplez/compare/v0.19.0...v0.20.0) (2026-11-21)


### ⚠ BREAKING CHANGES

* **engine:** config::Output and config::Meter gain the pub field `correlation`, and OutputMeters gains the pub field `correlation`, so struct literals and exhaustive patterns of them need it.
* **engine:** samplez_mxl::input::Poll has a new variant, Opening, and PolledFlow::hold no longer opens the holder's reference before it returns; samplez_mxl::input::PollOpener and PolledFlow::with_opener are new. samplez::Counters has a new field, generation_past_margin.
* **engine:** ProductState has a new field, in_flight, and Matrix::settle_ducks follows the depth ramps running.
* **engine:** PROTOCOL moves from 3 to 4. A block a withdrawn replica withholds at once as unfillable now reports the digest it was computed with, and peers judge its pages by that digest, so replicas of both protocols cannot judge each other's pages alike. The compatibility key changes and a rollout rolls by baseline hand-over, not as peers.
* **engine:** config::Input has a new public field, true_peak, and InputMeters the public fields true_peak and max_true_peak, so code that builds either with a struct literal must set them.
* **replica:** Apply has a new public field, at_index, so a product that builds Apply with a struct literal must set it.

### Features

* **engine:** meter an input's true peak where its config asks for it ([#169](https://github.com/danielclasen/samplez/issues/169)) ([baf7ad6](https://github.com/danielclasen/samplez/commit/baf7ad611bf31c44a5585037d2639c447af96a00))
* **engine:** meter the correlation of an element's front legs where its config asks ([#171](https://github.com/danielclasen/samplez/issues/171)) ([4a28e46](https://github.com/danielclasen/samplez/commit/4a28e46d30e407447990f51fd69a2eeb0a49599f))
* **nmos:** take resource versions from a replicated product ([#168](https://github.com/danielclasen/samplez/issues/168)) ([4642bbc](https://github.com/danielclasen/samplez/commit/4642bbcb37b087a63374635887f53aa80359e86b))
* **replica:** give Product::apply the index its entry takes effect at ([#166](https://github.com/danielclasen/samplez/issues/166)) ([fe63fcb](https://github.com/danielclasen/samplez/commit/fe63fcb0bf841962d68017077611f6ae2ba84e54))


### Bug Fixes

* **engine:** join late fades and resume input and duck ramps ([#170](https://github.com/danielclasen/samplez/issues/170)) ([78d9100](https://github.com/danielclasen/samplez/commit/78d91000e332871fbc9cf39414429bebcb5ee4b5))
* **engine:** open a generation's readers off the hand-over path ([#165](https://github.com/danielclasen/samplez/issues/165)) ([98c4225](https://github.com/danielclasen/samplez/commit/98c4225cefe97054825bf86a2d1fb96c603b4e48))
* **engine:** open poll readers off the supervisor ([#172](https://github.com/danielclasen/samplez/issues/172)) ([3e96b83](https://github.com/danielclasen/samplez/commit/3e96b834bbc906fa9e05f039f2526b00d08d5688))
* **replica:** name a baseline once while every output is disabled ([#175](https://github.com/danielclasen/samplez/issues/175)) ([b00cffb](https://github.com/danielclasen/samplez/commit/b00cffb3c9139eacd0faeed34431091b6bb5e746))

## [0.19.0](https://github.com/danielclasen/samplez/compare/v0.18.0...v0.19.0) (2026-11-20)


### ⚠ BREAKING CHANGES

* **replica:** the `/readyz` term of an audio replica in contact passes while it commits; once its process has committed since it started, also while another audio replica commits, in any state (`starting`, `withdrawn`, `diverged`, `resync`, `version_mismatch`); before that only in `version_mismatch` below the baseline's versions while another commits. It fails while none commits. A withdrawn replica with no output enabled passes. Out of contact it keeps its last value for `rollout::DEPARTED` (3 s), then passes only while the replica commits. `ReplicaInfo::commitable` holds for an audio replica whose `/readyz` term passes, counted by a leader or not. `resync::Failed` gains `Refused`, and `resync::Metrics` gains `rebuilt_for_key` and `refused`.

### Bug Fixes

* **replica:** hand the baseline over under a Baseline without a key ([d6615b5](https://github.com/danielclasen/samplez/commit/d6615b5250f4bfe980a126e171a97b31aa34d930))

## [0.18.0](https://github.com/danielclasen/samplez/compare/v0.17.0...v0.18.0) (2026-11-20)


### ⚠ BREAKING CHANGES

* **engine:** `Entry::Tone`, `Engine::set_tone` and `InputState::Tone` are new. `config::Input::domain` is an `Option<String>`, and `config::Input` has the field `tone`. `Targets` and `ProductState` have the field `tones`, `ProductState` the field `tone_ramps`, the snapshot's `Replicated` the field `tone_ramps`. `samplez_replica::PROTOCOL` is 3.
* **replica:** contact::Timing has a new field, prune_after_blocks, the engine's pruneAfterBlocks, which the product sets. report::Own gains `readable` and `unreadable` and samplez::InputStatus gains `readable`. A product deciding by Replica::input_liveness still drains Replication.reports.
* **replica:** `Own` and `Baseline` gain a `versions` field on the wire, read as none from a peer without it. `Rollout` gains `versions()`, defaulting to none, and `Fixed::new` takes the versions. `rollout::may_take_over` takes whether the baseline's versions have departed. `state::Metrics` gains `mismatch` and `contact::Metrics` gains `versions`. A replica in `version_mismatch` reads Ready while another audio replica commits.

### Features

* **engine:** add a line-up tone generator input ([#161](https://github.com/danielclasen/samplez/issues/161)) ([60a54de](https://github.com/danielclasen/samplez/commit/60a54de3b26f52920213874646019e3eb92f7e1e))
* **replica:** decide input liveness by a majority of audio voters ([9538d09](https://github.com/danielclasen/samplez/commit/9538d098ec54b99b76bae02a5646b463780e3a23))
* **replica:** hand the baseline over only to versions not below it ([d0f639d](https://github.com/danielclasen/samplez/commit/d0f639db2cc8f43daedd93ec60552b6b7805def8))


### Bug Fixes

* **replica:** drop apply_lead_raised_by when that voter leaves ([9538d09](https://github.com/danielclasen/samplez/commit/9538d098ec54b99b76bae02a5646b463780e3a23))
* **replica:** keep a peer connection across a missed report ([#157](https://github.com/danielclasen/samplez/issues/157)) ([cc58feb](https://github.com/danielclasen/samplez/commit/cc58febf6ed8b3036d5ba049d50144db2b329117))

## [0.17.0](https://github.com/danielclasen/samplez/compare/v0.16.0...v0.17.0) (2026-11-20)


### ⚠ BREAKING CHANGES

* **replica:** `samplez::ENGINE_VERSION` is replaced by `samplez::COMPUTE_VERSION: u32`, and a core copy's encoding carries it as a number. `rollout::key` is `rollout::key::<P: Product>(output)`. `Product` has the required associated const `DERIVATION_VERSION`. `samplez-dsp` requires `libm` at exactly 0.2.16.
* **engine:** `Entry` has the variants `LoudnessRun` and `ResetLoudness`. `Engine::reset_loudness` appends a `ResetLoudness` entry to an engine's own log and returns `GainError`; an engine that follows a replicated log refuses it as invalid. `ResetError` is removed. `Engine::set_loudness_running` is new. `Targets` and `ProductState` have the field `loudness_running`. `OutputMeters` has the fields `paused` and `running`. `samplez_dsp::Slice` has the variant `Paused`. Core copies carry the paused meters. `Targets` has the field `loudness_resets`, one reset epoch per meter. `StateMachine` and the snapshot's `Replicated` have the field `resets`; a snapshot without it loads with none.

### Features

* **engine:** reset and pause loudness through log entries ([2650fc5](https://github.com/danielclasen/samplez/commit/2650fc543bc3155c2d4a48c8e73f4ae148bb7b06))
* **replica:** key compatibility on explicit versions ([#151](https://github.com/danielclasen/samplez/issues/151)) ([d29e070](https://github.com/danielclasen/samplez/commit/d29e0702ce2fa3e1f89cc073797c3a85696075ac))


### Bug Fixes

* **replica:** replay a resync from before a snapshot with no record ([#155](https://github.com/danielclasen/samplez/issues/155)) ([bf48522](https://github.com/danielclasen/samplez/commit/bf48522ad30b87cdfdb655ab706c5b39fef6942c))

## [0.16.0](https://github.com/danielclasen/samplez/compare/v0.15.0...v0.16.0) (2026-11-20)


### ⚠ BREAKING CHANGES

* **engine:** keep other threads off the RT thread's CPU ([#148](https://github.com/danielclasen/samplez/issues/148))
* **engine:** rt::setup_thread takes the ProcessStatus from setup_process. ProcessStatus gains rt_cpu and isolation, and config Rt gains isolate_rt_cpu (rt.isolateRtCpu, default true), which changes the affinity of every thread created after setup_process when the RT thread is pinned.

### Features

* **engine:** keep other threads off the RT thread's CPU ([55b4d5e](https://github.com/danielclasen/samplez/commit/55b4d5e9c5e0963b6a5eae2297f62d8519ac2085))
* **engine:** keep other threads off the RT thread's CPU ([#148](https://github.com/danielclasen/samplez/issues/148)) ([55b4d5e](https://github.com/danielclasen/samplez/commit/55b4d5e9c5e0963b6a5eae2297f62d8519ac2085))
* **replica:** count per-peer contact silences ([#146](https://github.com/danielclasen/samplez/issues/146)) ([019e6b2](https://github.com/danielclasen/samplez/commit/019e6b2cc3532182de71ed34414b033800e6e624))

## [0.15.0](https://github.com/danielclasen/samplez/compare/v0.14.0...v0.15.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **engine:** `OutputMeters` loses `valid` and gains `gaps` and `lost`. `samplez_dsp::Loudness::push` takes a `Slice`, `reset` takes the slice index the measurement restarts at, and `valid` and `gap` are removed in favour of `gaps`, `lost`, `start` and `skip`.

### Features

* **engine:** keep measuring loudness across gaps and count them ([#140](https://github.com/danielclasen/samplez/issues/140)) ([558186e](https://github.com/danielclasen/samplez/commit/558186e7218240b927c19a9bf9480c4131d008f6))

## [0.14.0](https://github.com/danielclasen/samplez/compare/v0.13.1...v0.14.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **engine:** Engine::install_product no longer ramps gains from silence or fades inputs in; the first blocks after an install carry the targets at full level.

### Bug Fixes

* **engine:** install a product state with no fade-in ([1120aae](https://github.com/danielclasen/samplez/commit/1120aaef03ac3a5fdc4339bb89b151ccccd1fd26))
* **engine:** start a rebuilt masked term's duck at the live gain ([#142](https://github.com/danielclasen/samplez/issues/142)) ([ff1a1ff](https://github.com/danielclasen/samplez/commit/ff1a1ffcfb2a9d56b850af234a516044e3375534))

## [0.13.1](https://github.com/danielclasen/samplez/compare/v0.13.0...v0.13.1) (2026-11-19)


### Bug Fixes

* **replica:** fail fast on an apply inside with_state ([#138](https://github.com/danielclasen/samplez/issues/138)) ([e52cd57](https://github.com/danielclasen/samplez/commit/e52cd5791c097dead5b8b2b802008e6b58a5058c))

## [0.13.0](https://github.com/danielclasen/samplez/compare/v0.12.3...v0.13.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **replica:** Product requires Sync.

### Bug Fixes

* **replica:** stop with_state closures deadlocking the contact loop ([#135](https://github.com/danielclasen/samplez/issues/135)) ([c90052f](https://github.com/danielclasen/samplez/commit/c90052fbd69406cd8998327cd0a886968473a683))

## [0.12.3](https://github.com/danielclasen/samplez/compare/v0.12.2...v0.12.3) (2026-11-19)


### Bug Fixes

* **replica:** ask the engine in order and skip resolves it never held ([#132](https://github.com/danielclasen/samplez/issues/132)) ([873c350](https://github.com/danielclasen/samplez/commit/873c35030824d080d182132e8f793adc945b2a90))

## [0.12.2](https://github.com/danielclasen/samplez/compare/v0.12.1...v0.12.2) (2026-11-19)


### Bug Fixes

* **replica:** bound the contact lock hold of a catch-up ([#131](https://github.com/danielclasen/samplez/issues/131)) ([7613fb3](https://github.com/danielclasen/samplez/commit/7613fb3a382264033791ce5bab55bd6c2888c5f9))
* **replica:** release the metrics guard before locking contact ([#129](https://github.com/danielclasen/samplez/issues/129)) ([2e953ff](https://github.com/danielclasen/samplez/commit/2e953ffb12d82ccb046f55c014ab017211f0209f))
* **service:** keep /healthz answering when a replica lock is stuck ([#128](https://github.com/danielclasen/samplez/issues/128)) ([13dee4e](https://github.com/danielclasen/samplez/commit/13dee4ed77ef8d74ddc489a8a3576dd497ed2ba7))

## [0.12.1](https://github.com/danielclasen/samplez/compare/v0.12.0...v0.12.1) (2026-11-19)


### Bug Fixes

* **replica:** rebuild past late entries applied while committing ([#126](https://github.com/danielclasen/samplez/issues/126)) ([d3efa49](https://github.com/danielclasen/samplez/commit/d3efa49c4d3dd4ac58c68a446c838231415cf54e))

## [0.12.0](https://github.com/danielclasen/samplez/compare/v0.11.0...v0.12.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **replica:** Timing has a new field, lead_minute.

### Features

* **replica:** lengthen raft election timeouts and make them configurable ([#124](https://github.com/danielclasen/samplez/issues/124)) ([6795f22](https://github.com/danielclasen/samplez/commit/6795f2224ac8fe207e781c5d4c252c50327f112c))


### Bug Fixes

* **engine:** hand a delay line's history over in steps ([#122](https://github.com/danielclasen/samplez/issues/122)) ([cd67219](https://github.com/danielclasen/samplez/commit/cd67219419e8a40048513b321889dd59a473e35b))
* **replica:** keep an election from pinning the apply lead at its cap ([#121](https://github.com/danielclasen/samplez/issues/121)) ([281760a](https://github.com/danielclasen/samplez/commit/281760a86d2340db5e0c38aa97d80cae1a7a13fc))

## [0.11.0](https://github.com/danielclasen/samplez/compare/v0.10.0...v0.11.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **engine:** add a delay line per input ([#113](https://github.com/danielclasen/samplez/issues/113))
* **service:** /healthz no longer carries the "replica" object. Read it from /readyz.

### Features

* **engine:** add a delay line per input ([#113](https://github.com/danielclasen/samplez/issues/113)) ([669e1ae](https://github.com/danielclasen/samplez/commit/669e1ae97c644ba2d69dc364b4794e2140f93b04))


### Bug Fixes

* **replica:** rebuild past entries applied late while committing ([#116](https://github.com/danielclasen/samplez/issues/116)) ([c5fe396](https://github.com/danielclasen/samplez/commit/c5fe396321d9bd9235cbfdb84c2e2d6488de6226))
* **service:** answer /healthz without the replica's locks or the shared task ([#115](https://github.com/danielclasen/samplez/issues/115)) ([0bf29a7](https://github.com/danielclasen/samplez/commit/0bf29a751cfe4fedbb2040e51c8d00ff39ed79d3))


### Reverts

* rebuild past entries applied late while committing ([#118](https://github.com/danielclasen/samplez/issues/118)) ([c2307a1](https://github.com/danielclasen/samplez/commit/c2307a1ab787f158aac6480c4faed5685aabf1bb))

## [0.10.0](https://github.com/danielclasen/samplez/compare/v0.9.1...v0.10.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **engine:** `Entry` gains `Limiter`; `Targets` and `ProductState` gain `limiters`; `config::Output` gains `limiter`.

### Features

* **engine:** add a true-peak limiter per output ([#112](https://github.com/danielclasen/samplez/issues/112)) ([4e7b791](https://github.com/danielclasen/samplez/commit/4e7b79180d6d5f5f744c73ad6453980a6008cdb1))

## [0.9.1](https://github.com/danielclasen/samplez/compare/v0.9.0...v0.9.1) (2026-11-19)


### Bug Fixes

* **engine:** prune an input whose blocks stay late while its head moves ([#110](https://github.com/danielclasen/samplez/issues/110)) ([78714cb](https://github.com/danielclasen/samplez/commit/78714cb5d5f51d12178a2ea8d0f7d112b460b38a))
* **replica:** count a flow mismatch on across a resync hand-over ([#109](https://github.com/danielclasen/samplez/issues/109)) ([9acb7a1](https://github.com/danielclasen/samplez/commit/9acb7a11cc8a46e90b81c5487a6358e33328ce8f))

## [0.9.0](https://github.com/danielclasen/samplez/compare/v0.8.3...v0.9.0) (2026-11-19)


### ⚠ BREAKING CHANGES

* **nmos:** `NmosConfig` has a new `continue_held` field.
* **engine:** `config::Input::flow_id` is an `Option<String>`, and

### Features

* **engine:** start an output disabled and an input detached ([#93](https://github.com/danielclasen/samplez/issues/93)) ([efa0523](https://github.com/danielclasen/samplez/commit/efa0523fc989332b727ff2712b140e6ccb4eda4a))
* **nmos:** continue a registration a registry still holds ([#99](https://github.com/danielclasen/samplez/issues/99)) ([7cf8781](https://github.com/danielclasen/samplez/commit/7cf87812d50a900099f234f2be6c3d04573c2f9b))


### Bug Fixes

* **replica:** rebuild for flows only once the mismatch outlasts the lead ([#106](https://github.com/danielclasen/samplez/issues/106)) ([c4d965a](https://github.com/danielclasen/samplez/commit/c4d965ae456c1d861dd92fceed7a89e5501f147c))

## [0.8.3](https://github.com/danielclasen/samplez/compare/v0.8.2...v0.8.3) (2026-11-19)


### Bug Fixes

* **engine:** wake a head wait at least twice per block period ([#102](https://github.com/danielclasen/samplez/issues/102)) ([d657096](https://github.com/danielclasen/samplez/commit/d657096939ccfd8ed28f6f905896a501470f47e3))
* **replica:** release entries a restarted replica held unresolved ([#105](https://github.com/danielclasen/samplez/issues/105)) ([414511a](https://github.com/danielclasen/samplez/commit/414511a2db1d508d9fa5d90108cf73ecd42064b2))

## [0.8.2](https://github.com/danielclasen/samplez/compare/v0.8.1...v0.8.2) (2026-11-19)


### Bug Fixes

* **engine:** wait for input on heads, no flow check on the RT thread ([#100](https://github.com/danielclasen/samplez/issues/100)) ([23a09c2](https://github.com/danielclasen/samplez/commit/23a09c27d9710f8e68e6846a0f9021f233d0b031))

## [0.8.1](https://github.com/danielclasen/samplez/compare/v0.8.0...v0.8.1) (2026-11-18)


### Bug Fixes

* **engine:** look up input flows off the supervisor ([#92](https://github.com/danielclasen/samplez/issues/92)) ([41960aa](https://github.com/danielclasen/samplez/commit/41960aa2e4d93008366a6a53d79b47e56c43e715))
* **replica:** count only enabled outputs when a replica writes nothing ([#98](https://github.com/danielclasen/samplez/issues/98)) ([6b42843](https://github.com/danielclasen/samplez/commit/6b428439b9edcbe5ce74b87a995b12fff9ed0a8b))
* **replica:** replace a refused strip or duck setting alike everywhere ([b55f3d4](https://github.com/danielclasen/samplez/commit/b55f3d492753bc553dd17a1bc6fc021213b75d69))

## [0.8.0](https://github.com/danielclasen/samplez/compare/v0.7.0...v0.8.0) (2026-11-18)


### ⚠ BREAKING CHANGES

* **engine:** Targets.flows and ProductState.flows are Vec<Option<String>>, None for a detached slot. Entry gains Detach, InputState gains Detached, and the core's encoding gains a slot's detached state and a batch's detached slots.
* **engine:** Entry, Targets, ProductState and Status gain duck fields, Config gains duckColumns, and the core encoding carries the detectors.

### Features

* **engine:** apply duck groups from log entries ([#79](https://github.com/danielclasen/samplez/issues/79)) ([ea5b164](https://github.com/danielclasen/samplez/commit/ea5b1646cd56ac6f2090116ac722558e4eeab4e3))
* **engine:** detach inputs and release outputs ([#80](https://github.com/danielclasen/samplez/issues/80)) ([1daf676](https://github.com/danielclasen/samplez/commit/1daf676dff7305c0039c314288ad03db29063e47))
* **nmos:** add samplez-nmos with the IS-04 v1.3 Node API ([#73](https://github.com/danielclasen/samplez/issues/73)) ([d4566c9](https://github.com/danielclasen/samplez/commit/d4566c9b0f63877320e1b590f8ed408ef6f99c83))
* **nmos:** register the node with an IS-04 v1.3 registry ([#75](https://github.com/danielclasen/samplez/issues/75)) ([ae1e7dd](https://github.com/danielclasen/samplez/commit/ae1e7dd2d6a5c7a16f0ceef0477d20b7ce04de3e))
* **nmos:** serve the IS-05 v1.2 Connection API for MXL ports ([#74](https://github.com/danielclasen/samplez/issues/74)) ([6cd273b](https://github.com/danielclasen/samplez/commit/6cd273bbfcf9c1d2630e735e67c000283e06ae58))
* **replica:** log the index a catch-up waits for and its voter ([#84](https://github.com/danielclasen/samplez/issues/84)) ([25a2246](https://github.com/danielclasen/samplez/commit/25a224668c29e4320b584b224663d28fb72ab295))


### Bug Fixes

* **engine:** keep no outgoing flow when detaching a pruned slot ([#82](https://github.com/danielclasen/samplez/issues/82)) ([f9bc735](https://github.com/danielclasen/samplez/commit/f9bc73552f4527f20de630080d61ef087ef742ba))
* **replica:** align the fallback index with the block, harden two tests ([#78](https://github.com/danielclasen/samplez/issues/78)) ([4245201](https://github.com/danielclasen/samplez/commit/424520157636043053a77f215fceb3f4d9606891))
* **replica:** count a follower only on an accepted append from its leader ([e67a724](https://github.com/danielclasen/samplez/commit/e67a724f3fe442eacd06084267161b4d0525d818))
* **replica:** derive a gain an engine refuses as silence everywhere ([#85](https://github.com/danielclasen/samplez/issues/85)) ([0ccedac](https://github.com/danielclasen/samplez/commit/0ccedacec7654391d4c81523b4bff6ccfd9e4056))
* **replica:** end the membership watch when the replica shuts down ([#86](https://github.com/danielclasen/samplez/issues/86)) ([cbe1bc6](https://github.com/danielclasen/samplez/commit/cbe1bc6b96c21d7c5c9f601f3d449fa8d7620a58))
* **replica:** hold the lead's fall only at the cap ([03d6745](https://github.com/danielclasen/samplez/commit/03d67458ed3f624f51ffd56911ff3c39c2d4565c))
* **replica:** hold the lead's fall while entries need more than it ([7423ff1](https://github.com/danielclasen/samplez/commit/7423ff12bedebd8a6b5ef6b5c78f067a3ff821ee))
* **replica:** keep a generation's earliest index within AHEAD_MAX ([7423ff1](https://github.com/danielclasen/samplez/commit/7423ff12bedebd8a6b5ef6b5c78f067a3ff821ee))
* **replica:** never count a learner as serving ([7423ff1](https://github.com/danielclasen/samplez/commit/7423ff12bedebd8a6b5ef6b5c78f067a3ff821ee))
* **replica:** promote learners, settle late entries, rebuild off the tick ([#81](https://github.com/danielclasen/samplez/issues/81)) ([40307f3](https://github.com/danielclasen/samplez/commit/40307f33b027c33ee10a118aa09ca07d5055ed1e))
* **replica:** resync onto a donor that took entries late ([#83](https://github.com/danielclasen/samplez/issues/83)) ([5b89fee](https://github.com/danielclasen/samplez/commit/5b89fee393a00e7eee753594fbff4e3083c77358))
* **replica:** wait for the extra lead in an engine rebuild ([7423ff1](https://github.com/danielclasen/samplez/commit/7423ff12bedebd8a6b5ef6b5c78f067a3ff821ee))
* **replica:** warn when the membership watch ends on a raft fault ([e67a724](https://github.com/danielclasen/samplez/commit/e67a724f3fe442eacd06084267161b4d0525d818))

## [0.7.0](https://github.com/danielclasen/samplez/compare/v0.6.0...v0.7.0) (2026-11-18)


### ⚠ BREAKING CHANGES

* **replica:** Audio::appended takes the log index, at_index and derived entries; Audio::truncated, Audio::skip and Audio::admits are gone; Audio::rebuild_engine takes the product state and the index to install at or after. EngineAudio::new takes no Translate, and Translate, Translated, Rebuild, input_entry and with_rebuild are gone. Product::paths and LogView::paths are gone. resync::Source::entries returns records or Missed. LeaderLog::remaining is replaced by superseded. Sample gains entries_filtered.
* **replica:** `Body` gains the `Protocol` variant; `Body::Restamp` gains `generation`, `Body::Void` gains `at_index` and `generation`, `Body::LeaseExpiry` gains `at_index`. A binary of an earlier protocol cannot read a log that holds `Body::Protocol`.
* **replica:** `Product::apply` takes an `Apply` with how the command was ordered; `Command` has a `generation` field; `Replica::append` returns a `Result`.
* **replica:** `Product` loses its `Default` bound and requires `config_hash`. `Store::open` and `Replica::start` take a `Factory<P>` in place of the product.
* **engine:** `Engine::install_product` takes the index to install at, before the timeout; 0 keeps installing at the next block.

### Features

* **dsp:** duck matrix terms by detector envelopes ([#70](https://github.com/danielclasen/samplez/issues/70)) ([042a597](https://github.com/danielclasen/samplez/commit/042a597700e5892b838db7f6efe7ed2e2008192a))
* **engine:** add engine targets and install a product core at an index ([#63](https://github.com/danielclasen/samplez/issues/63)) ([fc12d4a](https://github.com/danielclasen/samplez/commit/fc12d4ad61707eb8c6489725904e061a2ba04777))
* **replica:** build the product from a factory, check its config ([#64](https://github.com/danielclasen/samplez/issues/64)) ([23de5f4](https://github.com/danielclasen/samplez/commit/23de5f4a65b7a0b5fba77cd9ae7a8d57d5f98a4f))
* **replica:** derive engine records from product state at apply ([#66](https://github.com/danielclasen/samplez/issues/66)) ([f36f752](https://github.com/danielclasen/samplez/commit/f36f752e71dec766ef1e4bff03762e3f9c2ba42e))
* **replica:** gate generation flags on protocol 2, re-assert at a void ([#68](https://github.com/danielclasen/samplez/issues/68)) ([6b3148a](https://github.com/danielclasen/samplez/commit/6b3148ad606879c0e2f7c0c601fd0c90000b158a))
* **replica:** hand derived records to the engine at apply ([#71](https://github.com/danielclasen/samplez/issues/71)) ([7bd86dc](https://github.com/danielclasen/samplez/commit/7bd86dc61c6dbb6022411f62c497c11528d14f87))


### Performance Improvements

* **engine:** compare strip parameters without formatting in diff ([#69](https://github.com/danielclasen/samplez/issues/69)) ([277228c](https://github.com/danielclasen/samplez/commit/277228c69b3d25dbe4b8b049e3de2b7eeb2fa95f))

## [0.6.0](https://github.com/danielclasen/samplez/compare/v0.5.0...v0.6.0) (2026-11-18)


### ⚠ BREAKING CHANGES

* **engine:** a core copy's encoding carries the pending batches, and a Core's pending list holds one entry per log entry.

### Features

* **dsp:** add a duck detector ([#62](https://github.com/danielclasen/samplez/issues/62)) ([b416541](https://github.com/danielclasen/samplez/commit/b4165411f04030b7679d24475fc775f8af72dc5a))
* **engine:** carry strip state in a core copy ([#55](https://github.com/danielclasen/samplez/issues/55)) ([39edace](https://github.com/danielclasen/samplez/commit/39edace0a078e7b34c772432c5a16c9f372081fc))
* **engine:** fold a log entry's engine entries into one batch ([#56](https://github.com/danielclasen/samplez/issues/56)) ([9dcda4d](https://github.com/danielclasen/samplez/commit/9dcda4d22a1eac97eadf834a33b618e7e23fb421))
* **replica:** own the cluster marker by the audio StatefulSet ([70e70c0](https://github.com/danielclasen/samplez/commit/70e70c0088282966442d17719a9187bdb4dd719e))


### Bug Fixes

* **engine:** drop a held core before installing a product core ([70e70c0](https://github.com/danielclasen/samplez/commit/70e70c0088282966442d17719a9187bdb4dd719e))
* **engine:** refill a copy left behind across a hand-over install ([70e70c0](https://github.com/danielclasen/samplez/commit/70e70c0088282966442d17719a9187bdb4dd719e))
* **engine:** refill a core copy that predates a later control ([70e70c0](https://github.com/danielclasen/samplez/commit/70e70c0088282966442d17719a9187bdb4dd719e))
* **engine:** withdraw every output before F passes a withheld block ([#61](https://github.com/danielclasen/samplez/issues/61)) ([91720f5](https://github.com/danielclasen/samplez/commit/91720f5560c9f5175de3d292c4d766514fba1efb))
* **replica:** recreate a missing marker from the running set ([70e70c0](https://github.com/danielclasen/samplez/commit/70e70c0088282966442d17719a9187bdb4dd719e))

## [0.5.0](https://github.com/danielclasen/samplez/compare/v0.4.0...v0.5.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **replica:** ReplicaState gains Resync, PeerRequest and PeerResponse gain Core, Timing gains resync_budget, and contact Metrics gains resync. A replica of this version reports a state older peers cannot decode.
* **replica:** Baseline has a key field, ReplicaState a VersionMismatch variant, Own key and revision fields, and the Audio trait a required stop_at method.
* **replica:** Body::Restamp and Body::Void carry the term of the entry they name, Body gains PurgePoint, Contact::decisions takes the leader's term, and contact::Metrics gains fields.

### Features

* **replica:** decide unresolved entries in the log ([#48](https://github.com/danielclasen/samplez/issues/48)) ([da07b05](https://github.com/danielclasen/samplez/commit/da07b05a946bd4b862b3ccc0176f7433c88a400c))
* **replica:** end session leases and refuse writes E does not count ([#51](https://github.com/danielclasen/samplez/issues/51)) ([f892c58](https://github.com/danielclasen/samplez/commit/f892c5863d6cde3f20d1785305762f76a3ccb64b))
* **replica:** hand over the compatibility key in a rollout ([#54](https://github.com/danielclasen/samplez/issues/54)) ([ff3c030](https://github.com/danielclasen/samplez/commit/ff3c03077d15acdbdad7eb9f744c89cc3ac5a12d))
* **replica:** judge clocks against the verified replicas ([#47](https://github.com/danielclasen/samplez/issues/47)) ([6ffe037](https://github.com/danielclasen/samplez/commit/6ffe037a9e5499e2750e7f8a0d62be74d38d12dc))
* **replica:** resync a diverged replica from a donor ([#52](https://github.com/danielclasen/samplez/issues/52)) ([f26d011](https://github.com/danielclasen/samplez/commit/f26d011af14813e1a7ec3cd29b56542c18e2d653))
* **replica:** verify TAI offsets and hold unverified replicas ([#53](https://github.com/danielclasen/samplez/issues/53)) ([96fbb0a](https://github.com/danielclasen/samplez/commit/96fbb0abca70f31a497d2e33f1984dad6c0a6ba0))


### Bug Fixes

* **replica:** stop a running replica before the harness restarts it ([#49](https://github.com/danielclasen/samplez/issues/49)) ([d2c8612](https://github.com/danielclasen/samplez/commit/d2c8612a1bdac4154170d6923b91ef93ecbe4cb4))

## [0.4.0](https://github.com/danielclasen/samplez/compare/v0.3.0...v0.4.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **service:** Identity has a new public field, name.
* **engine:** config::Input gains flow_channels and samplez_mxl::input::InputConfig gains flow_channels; code building these structs field by field has to set them.

### Features

* **engine:** select an input's legs from the channels of a wider flow ([#45](https://github.com/danielclasen/samplez/issues/45)) ([ad13e0b](https://github.com/danielclasen/samplez/commit/ad13e0ba025f8a3834b71846fa59288bc042aef6))
* **service:** carry the user's display name in Identity ([#44](https://github.com/danielclasen/samplez/issues/44)) ([361f704](https://github.com/danielclasen/samplez/commit/361f704fa2a227ddf405678ea5e254c9e7e781df))

## [0.3.0](https://github.com/danielclasen/samplez/compare/v0.2.0...v0.3.0) (2026-11-17)


### ⚠ BREAKING CHANGES

* **engine:** Config gains meters, config::Input gains loudness_channels, Status gains meters and InputStatus gains meters; code building these structs field by field has to set them.
* **engine:** samplez_mxl::input::Recovery::update takes the prune limit in samples instead of the timing and the input offset, and ON_TIME_NS is RECOVERY_NS.

### Features

* **engine:** meter every input after its strip and meter-only columns ([#42](https://github.com/danielclasen/samplez/issues/42)) ([96c0f28](https://github.com/danielclasen/samplez/commit/96c0f287b6d4a4e57ccb5f741b2872ea6e9bf5d4))


### Bug Fixes

* **engine:** re-add a pruned input once its head advances ([#41](https://github.com/danielclasen/samplez/issues/41)) ([f0f31fa](https://github.com/danielclasen/samplez/commit/f0f31fabbe904698600aeec950b53a6f6fab098f))

## [0.2.0](https://github.com/danielclasen/samplez/compare/v0.1.0...v0.2.0) (2026-11-16)


### ⚠ BREAKING CHANGES

* **service:** AuthMode gains Oidc and AuthConfig gains oidc. The API listener routes /api/v1/session in every mode and /api/v1/logout in token and oidc mode, paths a product's router can no longer define.
* **engine:** `Ctx` carries `Ranks` in place of `rank`, `Engine::set_rank` is replaced by `Engine::set_ranks`, and `commit_through` takes an optional end of the committed range.
* **engine:** Cause gains NoCore, StatusChange gains NoCore, and Counters gains blocks_without_core and entries_without_core.
* **engine:** Status gains replay_speed, and Counters gains handovers_installed, handovers_passed, handovers_generation, handovers_missed and handover_entries_lost.
* **engine:** copy the engine core after a block
* **mxl:** refuse inputs too short for a resync replay
* **dsp:** samplez_mxl::input::Timing gains resync_budget and samplez_mxl::Error gains History.
* **engine:** Entry gains Strip.
* **service:** Hub::new takes the Metrics to count text overflows in and Hub::subscribe the connection's Owner; Hub::publish returns Sent; Frame, Pushed, Popped and Ended gain variants. A connection that sends no catch-up request within 1 s of being accepted is now closed as a pong timeout.
* **engine:** `Replication` has a new field, `hashes`; code building it with a struct literal must set it. `None` hashes nothing, as before.
* **engine:** Engine::start_replicated takes a Replication, with the voter count and an optional status-change channel, in place of the report channel.
* **mxl:** OutputConfig has a new field, batch.
* **service:** Connect has no since and the upgrade no longer reads ?since=; serve_client takes an Encode and calls on_message with an Admission; Reply, Ended and Resolution have new variants; Delta::command is Delta::commands; ReplicatedState requires read_index, answering a ReadIndex, and applied.
* **engine:** Counters::gains_consumed is renamed entries_consumed and counts every log entry the RT thread takes off its list, not only gain changes.
* **service:** say in Ack how a committed command resolved
* **service:** answer a resent command as its first copy resolved
* **service:** make Ack non_exhaustive
* **service:** refuse replica ids that are not DNS labels
* **service:** ReplicaInfo has the fields `ready` and `commitable`; NotApplied is a struct with `reason` and `detail`; serve_client's on_message returns ws::Reply; Owner does not implement Serialize; Sessions::new and Service::new refuse a replica id that is empty or longer than 63 bytes.

### Features

* **dsp:** add history rings and allocation-free matrix copies ([0efe1bc](https://github.com/danielclasen/samplez/commit/0efe1bc9ae3af79ad744f88b96b9fa320b490342))
* **dsp:** add the channel strip's gain stage, high-pass and EQ ([9609181](https://github.com/danielclasen/samplez/commit/960918112e45c02482ec58bc36d47e3e5d547eef))
* **dsp:** add the strip's gate and compressor ([f400f8e](https://github.com/danielclasen/samplez/commit/f400f8e25732aa701c39aa472411227d492a0511))
* **dsp:** export the true-peak history length as TruePeak::HISTORY ([3bb777d](https://github.com/danielclasen/samplez/commit/3bb777d02b387e63e9db71b7cc3b76ef222f413a))
* **engine:** add block statuses and the output hold ring ([#20](https://github.com/danielclasen/samplez/issues/20)) ([c1a6dd4](https://github.com/danielclasen/samplez/commit/c1a6dd40bd4232951141acb17db996040b962d3c))
* **engine:** apply transitions from index-anchored entries ([3bb777d](https://github.com/danielclasen/samplez/commit/3bb777d02b387e63e9db71b7cc3b76ef222f413a))
* **engine:** cancel a requested commit start before it takes ([7390e57](https://github.com/danielclasen/samplez/commit/7390e57a66009e1f85b4b8bed529b58f7a7ee799))
* **engine:** copy the engine core after a block ([0efe1bc](https://github.com/danielclasen/samplez/commit/0efe1bc9ae3af79ad744f88b96b9fa320b490342))
* **engine:** drop an appended entry the log has yet to decide ([e783b57](https://github.com/danielclasen/samplez/commit/e783b57e7bca8255cc45db446d875533fa13f444))
* **engine:** drop the core and rebuild it from product state ([b9c1c20](https://github.com/danielclasen/samplez/commit/b9c1c20d302b04355141d11b7374618baf033b99))
* **engine:** fence on detected clock steps and sample the TAI offset ([#22](https://github.com/danielclasen/samplez/issues/22)) ([cce5bd7](https://github.com/danielclasen/samplez/commit/cce5bd76f2a6bb84e70f6d187e22020528258a4d))
* **engine:** hash pages and fingerprint applied entries ([33400cc](https://github.com/danielclasen/samplez/commit/33400cc2afc7e5e273e6cff1232a4e997cc3a6fb))
* **engine:** hold and withhold blocks by status ([104089f](https://github.com/danielclasen/samplez/commit/104089f3d7a89cca65cf5940b3743a86814ba93c))
* **engine:** rank blocks per index and commit through up to P ([9291f7a](https://github.com/danielclasen/samplez/commit/9291f7a7d9fda35bfd5c36c663a99c2c5d9ff28a))
* **engine:** read a replica's progress and clock flags together ([e783b57](https://github.com/danielclasen/samplez/commit/e783b57e7bca8255cc45db446d875533fa13f444))
* **engine:** replay a donor's core and hand it over at a block ([27f0440](https://github.com/danielclasen/samplez/commit/27f0440e4ead5bd3d32fe03ca82859a790268fb7))
* **engine:** run the channel strip on every input ([f347089](https://github.com/danielclasen/samplez/commit/f3470892998fea4236deaeb79103dca3f08e7af2))
* meter outputs with true peak and loudness ([#11](https://github.com/danielclasen/samplez/issues/11)) ([44fd9bd](https://github.com/danielclasen/samplez/commit/44fd9bdbff78af088ebea2e5cf6c3400311d886e))
* **mxl:** refuse inputs too short for a resync replay ([0efe1bc](https://github.com/danielclasen/samplez/commit/0efe1bc9ae3af79ad744f88b96b9fa320b490342))
* **mxl:** write sample ranges and size the commit batch ([#19](https://github.com/danielclasen/samplez/issues/19)) ([af85095](https://github.com/danielclasen/samplez/commit/af85095fe2b6d7c645ef87dc51898d7d6749262e))
* **replica:** add samplez-replica with log store and dedup ([c34fdd3](https://github.com/danielclasen/samplez/commit/c34fdd390b4861718bb07d2c44f7e940f8a232e5))
* **replica:** confirm a follower's read index through its leader ([9c347d5](https://github.com/danielclasen/samplez/commit/9c347d59469a21c2ec11887285704bcb435a954f))
* **replica:** connect voters over the peer listener ([9c347d5](https://github.com/danielclasen/samplez/commit/9c347d59469a21c2ec11887285704bcb435a954f))
* **replica:** count one revision per log entry that changes the product ([94d2827](https://github.com/danielclasen/samplez/commit/94d2827361dd6a8a3db3aaec5e6bfcc2026bea9b))
* **replica:** exchange side reports and fence on contact ([e783b57](https://github.com/danielclasen/samplez/commit/e783b57e7bca8255cc45db446d875533fa13f444))
* **replica:** name a baseline and resume on page hashes ([7390e57](https://github.com/danielclasen/samplez/commit/7390e57a66009e1f85b4b8bed529b58f7a7ee799))
* **replica:** stamp entries from E and bound command age ([ad9632a](https://github.com/danielclasen/samplez/commit/ad9632a267fbf1ec54fe25ac24b92f84d4275a03))
* **replica:** start a set from its marker and reconcile the voters ([#17](https://github.com/danielclasen/samplez/issues/17)) ([5b856ab](https://github.com/danielclasen/samplez/commit/5b856abc474231d05a3409a81613753d5ad05277))
* **replica:** withhold and commit through on evidence ([9291f7a](https://github.com/danielclasen/samplez/commit/9291f7a7d9fda35bfd5c36c663a99c2c5d9ff28a))
* **service:** add a too-large reason for refused commands ([9c347d5](https://github.com/danielclasen/samplez/commit/9c347d59469a21c2ec11887285704bcb435a954f))
* **service:** add mutual TLS for the replica peer listener ([9c347d5](https://github.com/danielclasen/samplez/commit/9c347d59469a21c2ec11887285704bcb435a954f))
* **service:** add OpenID Connect sign-in as the oidc auth mode ([#39](https://github.com/danielclasen/samplez/issues/39)) ([24501ec](https://github.com/danielclasen/samplez/commit/24501ecdbb935a54d7a37cdccd3b0e3bb4635b52))
* **service:** add replicated-state hooks to samplez-service ([#10](https://github.com/danielclasen/samplez/issues/10)) ([7c8f1d3](https://github.com/danielclasen/samplez/commit/7c8f1d31965cc902d99c5560f1858b71fe704bcb))
* **service:** answer a resent command as its first copy resolved ([c34fdd3](https://github.com/danielclasen/samplez/commit/c34fdd390b4861718bb07d2c44f7e940f8a232e5))
* **service:** carry position and heartbeats in the product protocol ([94d2827](https://github.com/danielclasen/samplez/commit/94d2827361dd6a8a3db3aaec5e6bfcc2026bea9b))
* **service:** let the product render auth and browser-check refusals ([cbaec08](https://github.com/danielclasen/samplez/commit/cbaec08e0ff4ade249d93de6db11cc6bf24ac312))
* **service:** make Ack non_exhaustive ([c34fdd3](https://github.com/danielclasen/samplez/commit/c34fdd390b4861718bb07d2c44f7e940f8a232e5))
* **service:** say in Ack how a committed command resolved ([c34fdd3](https://github.com/danielclasen/samplez/commit/c34fdd390b4861718bb07d2c44f7e940f8a232e5))
* **service:** send product status and text frames to chosen clients ([cbaec08](https://github.com/danielclasen/samplez/commit/cbaec08e0ff4ade249d93de6db11cc6bf24ac312))
* **service:** start pings at the client's first catch-up request ([cbaec08](https://github.com/danielclasen/samplez/commit/cbaec08e0ff4ade249d93de6db11cc6bf24ac312))


### Bug Fixes

* **dsp:** accept every slice index in Loudness::push ([3bb777d](https://github.com/danielclasen/samplez/commit/3bb777d02b387e63e9db71b7cc3b76ef222f413a))
* **engine:** restart the true peak after unmetered samples ([3bb777d](https://github.com/danielclasen/samplez/commit/3bb777d02b387e63e9db71b7cc3b76ef222f413a))
* **replica:** count the only voter of a set while it leads ([#29](https://github.com/danielclasen/samplez/issues/29)) ([91e414d](https://github.com/danielclasen/samplez/commit/91e414d15a28c49d20eec6eb81c4c4343fa23335))
* **replica:** take openraft 0.9.26 so a lone voter's election ends ([36763c4](https://github.com/danielclasen/samplez/commit/36763c4bfa7faf4ab06be9df84d4ee190cf28dee))
* **service:** refuse replica ids that are not DNS labels ([c34fdd3](https://github.com/danielclasen/samplez/commit/c34fdd390b4861718bb07d2c44f7e940f8a232e5))


### Performance Improvements

* **dsp:** cut the channel strip's CPU cost ([#37](https://github.com/danielclasen/samplez/issues/37)) ([9db62c7](https://github.com/danielclasen/samplez/commit/9db62c748b00c5f90ad0e05d97e23c3460429e32))

## 0.1.0 (2026-11-14)


### ⚠ BREAKING CHANGES

* **service:** submit and command_revision of ReplicatedState take the Owner (principal, session, role) and dedup by principal, session and id; Pending::submit is a plain function; Begin::InFlight is removed; a client-chosen ?session= must be 22 to 128 base64url characters; the not_after field of TlsStatus is optional; Sessions::new returns a Result.

### Features

* **dsp:** add gain ramps, mixing matrix and meters ([#2](https://github.com/danielclasen/samplez/issues/2)) ([ecf176c](https://github.com/danielclasen/samplez/commit/ecf176c50bebd78a6dac43175ff9fa1959c7e38a))
* **dsp:** advance matrix ramps without mixing ([a1eea3e](https://github.com/danielclasen/samplez/commit/a1eea3eb6e9224621c4ff45dafdd7548d9e1e4ea))
* **engine:** add the MXL engine core and samplez-hop ([a1eea3e](https://github.com/danielclasen/samplez/commit/a1eea3eb6e9224621c4ff45dafdd7548d9e1e4ea))
* **mxl:** add MXL flow input and output on libmxl's usage model ([#4](https://github.com/danielclasen/samplez/issues/4)) ([65872e9](https://github.com/danielclasen/samplez/commit/65872e9a0a1d20b61ddfc50ad38c42f75df3c190))
* **service:** add the shared listener, auth and WebSocket layer ([#5](https://github.com/danielclasen/samplez/issues/5)) ([4cd869c](https://github.com/danielclasen/samplez/commit/4cd869c69c77a546b94a34a1644afccb903b8565))


### Bug Fixes

* **dsp:** name buffer overflow, keep SavedMxcsr on its thread ([a1eea3e](https://github.com/danielclasen/samplez/commit/a1eea3eb6e9224621c4ff45dafdd7548d9e1e4ea))
* **engine:** drop the RT set-up channel only after the RT thread joins ([26a19d5](https://github.com/danielclasen/samplez/commit/26a19d544b3b204e18644949e60fd3ff3633694b))
* **service:** count a pong from when its ping was sent ([26a19d5](https://github.com/danielclasen/samplez/commit/26a19d544b3b204e18644949e60fd3ff3633694b))
* **service:** end a connection whose pause ran out before it resumed ([26a19d5](https://github.com/danielclasen/samplez/commit/26a19d544b3b204e18644949e60fd3ff3633694b))
* **service:** keep command order, dedup per session, renew certificates ([3f2518c](https://github.com/danielclasen/samplez/commit/3f2518c185d34b512645ae9863bf44806d82de4e))
