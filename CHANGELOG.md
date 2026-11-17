# Changelog

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
