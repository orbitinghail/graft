use std::marker::PhantomData;

use bytes::Bytes;
use culprit::ResultExt;
use fjall::{Keyspace, PartitionCreateOptions};
use graft_core::codec::Codec;

use crate::local::{fjall_storage::keys::FjallKey, storage::StorageErr};

pub struct TypedPartition<K, C: Codec> {
    partition: fjall::Partition,
    _phantom: PhantomData<(K, C)>,
}

impl<K, C> TypedPartition<K, C>
where
    K: FjallKey,
    C: Codec,
{
    pub fn new(
        keyspace: Keyspace,
        name: &str,
        opts: PartitionCreateOptions,
    ) -> culprit::Result<Self, StorageErr> {
        Ok(Self {
            partition: keyspace.open_partition(name, opts)?,
            _phantom: PhantomData,
        })
    }

    pub fn get(&self, key: K) -> culprit::Result<Option<C::Message>, StorageErr> {
        if let Some(slice) = self.partition.get(key.as_slice().as_ref())? {
            let bytes = Bytes::from(slice);
            return Ok(Some(C::decode(bytes).or_into_ctx()?));
        }
        return Ok(None);
    }

    pub fn insert(&self, key: K, val: C::Message) -> culprit::Result<(), StorageErr> {
        self.partition
            .insert(key.into_slice(), C::encode_to_bytes(val))?;
        Ok(())
    }
}
