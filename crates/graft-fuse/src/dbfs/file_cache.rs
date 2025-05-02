use graft_core::byte_unit::ByteUnit;
use quick_cache::{Weighter, unsync::Cache};

struct SizeWeighter;

impl Weighter<u64, Vec<u8>> for SizeWeighter {
    fn weight(&self, _key: &u64, value: &Vec<u8>) -> u64 {
        value.len() as u64
    }
}

#[derive(Debug)]
pub struct FileCache {
    cache: Cache<u64, Vec<u8>, SizeWeighter>,
}

impl FileCache {
    /// estimated_max_items should be roughly equivalent to max_size / average item weight
    pub fn new(estimated_max_items: usize, max_size: ByteUnit) -> Self {
        FileCache {
            cache: Cache::with_weighter(estimated_max_items, max_size.as_u64(), SizeWeighter),
        }
    }

    pub fn get_or_insert_with<E>(
        &mut self,
        key: u64,
        with: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<&Vec<u8>, E> {
        let item = self.cache.get_or_insert_with(&key, with)?;
        Ok(item.expect("items should always be admitted to the cache"))
    }
}
