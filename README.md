# samplez

Real-time multichannel audio engine with MXL I/O, written in Rust. It is the
shared engine of [foldz](https://github.com/danielclasen/foldz) (downmix) and
[faderz](https://github.com/danielclasen/faderz) (mixer).

## Building

`docker/builder` holds the build environment: Rust, clang and libclang for
bindgen, and a Release build of libmxl (MXL v1.1.0) installed under
`/opt/mxl`. CI builds and tests inside it.

```sh
docker build -t samplez-builder docker/builder
docker run --rm -v "$PWD:/w" -w /w samplez-builder cargo test --workspace
```

## License

Apache-2.0. libmxl is distributed under its own Apache-2.0 licence; its
licence files are installed in `/opt/mxl` in the builder image.
