use std::marker::PhantomData;

use fjall::{Keyspace, PartitionCreateOptions};

use crate::local::fjall_storage::fjall_repr::FjallRepr;

pub struct TypedPartition<K, V> {
    partition: fjall::Partition,
    _phantom: PhantomData<(K, V)>,
}

impl<K, V> TypedPartition<K, V>
where
    K: FjallRepr,
    V: FjallRepr,
{
    pub fn new(
        keyspace: Keyspace,
        name: &str,
        opts: PartitionCreateOptions,
    ) -> Result<Self, fjall::Error> {
        Ok(Self {
            partition: keyspace.open_partition(name, opts)?,
            _phantom: PhantomData,
        })
    }
}
