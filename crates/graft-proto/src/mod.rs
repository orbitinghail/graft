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
    pub mod remote {
        pub mod v1 {
            include!("graft.remote.v1.rs");
        }
    }
    pub mod local {
        pub mod v1 {
            include!("graft.local.v1.rs");
        }
    }
}
