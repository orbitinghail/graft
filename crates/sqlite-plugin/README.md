# sqlite-plugin

`sqlite-plugin` provides a streamlined and flexible way to implement SQLite virtual file systems (VFS) in Rust. Inspired by [sqlite-vfs], it offers a distinct design with key enhancements:

- **Centralized Control**: The `Vfs` trait intercepts all file operations at the VFS level, rather than delegating them directly to file handles. This simplifies shared state management and enables more advanced behaviors.
- **Custom Pragmas**: Easily define and handle custom SQLite pragmas to extend database functionality.
- **Integrated Logging**: Seamlessly forward logs to SQLite’s built-in logging system for unified diagnostics.

[sqlite-vfs]: https://github.com/rklaehn/sqlite-vfs

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE] or https://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT] or https://opensource.org/licenses/MIT)

at your option.

[LICENSE-APACHE]: https://github.com/orbitinghail/graft/blob/main/LICENSE-APACHE
[LICENSE-MIT]: https://github.com/orbitinghail/graft/blob/main/LICENSE-MIT

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you shall be dual licensed as above, without any
additional terms or conditions.
