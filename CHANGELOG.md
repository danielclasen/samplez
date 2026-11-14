# Changelog

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
