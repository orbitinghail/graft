use zerocopy::{BE, FromBytes, Immutable, KnownLayout, U16, U32};

/// The header of a SQLite database file.
/// Used for easy debugging. See `pragma graft_dbg_hdr` for an example.
#[derive(Clone, Debug, FromBytes, Immutable, KnownLayout)]
#[repr(C)]
pub struct SqliteHeader {
    /// The header string: "SQLite format 3\000"
    magic: [u8; 16],
    /// The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
    page_size: U16<BE>,
    /// File format write version. 1 for legacy; 2 for WAL.
    file_format_write_version: u8,
    /// File format read version. 1 for legacy; 2 for WAL.
    file_format_read_version: u8,
    /// Bytes of unused "reserved" space at the end of each page. Usually 0.
    reserved_space: u8,
    /// Maximum embedded payload fraction. Must be 64.
    max_embedded_payload_fraction: u8,
    /// Minimum embedded payload fraction. Must be 32.
    min_embedded_payload_fraction: u8,
    /// Leaf payload fraction. Must be 32.
    leaf_payload_fraction: u8,
    /// File change counter.
    file_change_counter: U32<BE>,
    /// Size of the database file in pages. The "in-header database size".
    database_size_in_pages: U32<BE>,
    /// Page number of the first freelist trunk page.
    first_freelist_trunk_page: U32<BE>,
    /// Total number of freelist pages.
    total_freelist_pages: U32<BE>,
    /// The schema cookie.
    schema_cookie: U32<BE>,
    /// The schema format number. Supported schema formats are 1, 2, 3, and 4.
    schema_format_number: U32<BE>,
    /// Default page cache size.
    default_page_cache_size: U32<BE>,
    /// The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
    largest_root_btree_page: U32<BE>,
    /// The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
    database_text_encoding: U32<BE>,
    /// The "user version" as read and set by the user_version pragma.
    user_version: U32<BE>,
    /// True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
    incremental_vacuum_mode: U32<BE>,
    /// The "Application ID" set by PRAGMA application_id.
    application_id: U32<BE>,
    /// Reserved for expansion. Must be zero.
    reserved: [u8; 20],
    /// The version-valid-for number.
    version_valid_for_number: U32<BE>,
    /// SQLITE_VERSION_NUMBER
    sqlite_version_number: U32<BE>,
}
