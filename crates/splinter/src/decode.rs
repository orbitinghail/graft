use std::{
    any::type_name,
    fmt::{Debug, Display},
    marker::PhantomData,
    ops::Deref,
};

use bytes::Bytes;
use zerocopy::{FromBytes, Immutable, KnownLayout, Unaligned};

use crate::DecodeErr;

pub struct Ref<T>(Bytes, PhantomData<T>);

impl<T> Ref<T> {
    const SIZE: usize = size_of::<T>();

    pub fn from_prefix(data: &mut Bytes) -> Result<Ref<T>, DecodeErr> {
        if data.len() < Self::SIZE {
            return Err(DecodeErr::InvalidLength { ty: type_name::<T>(), size: Self::SIZE });
        }

        let prefix = data.split_to(Self::SIZE);
        Ok(Ref(prefix, PhantomData))
    }

    pub fn from_suffix(data: &mut Bytes) -> Result<Ref<T>, DecodeErr> {
        if data.len() < Self::SIZE {
            return Err(DecodeErr::InvalidLength { ty: type_name::<T>(), size: Self::SIZE });
        }

        let suffix = data.split_off(data.len() - Self::SIZE);
        Ok(Ref(suffix, PhantomData))
    }
}

impl<T> Deref for Ref<T>
where
    T: KnownLayout + Immutable + FromBytes + Unaligned,
{
    type Target = T;

    fn deref(&self) -> &Self::Target {
        T::ref_from_bytes(&self.0).expect("internal error: Ref::deref should be infallible")
    }
}

impl<T> Debug for Ref<T>
where
    T: Debug + KnownLayout + Immutable + FromBytes + Unaligned,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner: &T = self;
        f.debug_tuple("Ref").field(&inner).finish()
    }
}

impl<T> Display for Ref<T>
where
    T: Display + KnownLayout + Immutable + FromBytes + Unaligned,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner: &T = self;
        inner.fmt(f)
    }
}
