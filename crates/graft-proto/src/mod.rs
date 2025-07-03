#[allow(clippy::doc_markdown)]
pub mod graft {
    pub mod common {
        pub mod v1 {
            include!("graft.common.v1.rs");
        }
    }
    pub mod metastore {
        pub mod v1 {
            include!("graft.metastore.v1.rs");
        }
    }
    pub mod pagestore {
        pub mod v1 {
            include!("graft.pagestore.v1.rs");
        }
    }
    pub mod core {
        pub mod v1 {
            include!("graft.core.v1.rs");
        }
    }
}
