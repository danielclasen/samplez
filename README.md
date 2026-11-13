# samplez

Real-time multichannel audio engine with MXL I/O, written in Rust.

## Building

`docker/builder` holds the build environment: Rust, clang and libclang for
bindgen, and a Release build of libmxl (MXL v1.1.0) installed under
`/opt/mxl`. CI builds and tests inside it.

```sh
docker build -t samplez-builder docker/builder
docker run --rm --user "$(id -u):$(id -g)" -v "$PWD:/w" -w /w \
  samplez-builder cargo test --workspace
```

## Licence

Apache-2.0. The builder image carries the licence files of MXL and of the
libraries statically linked into libmxl under `/opt/mxl/share/licenses`.
