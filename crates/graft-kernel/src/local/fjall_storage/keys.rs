use bytes::{BufMut, BytesMut};
use culprit::{Culprit, ResultExt};
use fjall::Slice;
use graft_core::{
    PageIdx, SegmentId, VolumeId,
    cbe::{CBE32, CBE64},
    handle_id::{HandleId, HandleIdErr},
    lsn::{InvalidLSN, LSN},
    page_idx::ConvertToPageIdxErr,
    zerocopy_ext::{TryFromBytesExt, ZerocopyErr},
};
use zerocopy::{ConvertError, Immutable, IntoBytes, KnownLayout, TryFromBytes, Unaligned};

#[derive(Debug, thiserror::Error)]
pub enum KeyDecodeErr {
    #[error("Corrupt key: {0}")]
    CorruptKey(#[from] ZerocopyErr),

    #[error("Invalid LSN: {0}")]
    InvalidLSN(#[from] InvalidLSN),

    #[error("Invalid page index: {0}")]
    InvalidPageIdx(#[from] ConvertToPageIdxErr),

    #[error("Invalid handle ID: {0}")]
    InvalidHandleId(#[from] HandleIdErr),
}

impl<A, S, V> From<ConvertError<A, S, V>> for KeyDecodeErr {
    #[inline]
    fn from(value: ConvertError<A, S, V>) -> Self {
        Self::CorruptKey(value.into())
    }
}

struct KeyBuilder {
    builder: BytesMut,
}

impl KeyBuilder {
    /// Creates a new `KeyBuilder` with the specified length.
    /// SAFETY:
    /// len must be exactly equal to the length of the resulting key
    fn new(len: usize) -> Self {
        // Use `with_capacity` & `set_len`` to avoid zeroing the buffer
        let mut builder = BytesMut::with_capacity(len);

        // SAFETY:
        // 1. we just allocated `len` bytes
        // 2. we will panic if the caller doesn't write exactly `len` bytes to the builder
        #[allow(unsafe_code)]
        unsafe {
            builder.set_len(len);
        }

        Self { builder }
    }

    fn put_slice(mut self, src: &[u8]) -> Self {
        assert!(
            self.builder.spare_capacity_mut().len() >= src.len(),
            "KeyBuilder: not enough capacity"
        );
        self.builder.put_slice(src);
        self
    }

    fn build(self) -> Slice {
        assert!(
            self.builder.capacity() == self.builder.len(),
            "KeyBuilder: declared capacity does not match written byte count"
        );
        self.builder.freeze().into()
    }
}

struct KeyReader<'a> {
    slice: &'a [u8],
}

impl<'a> KeyReader<'a> {
    /// Creates a new `KeyReader` from the given byte slice.
    #[inline]
    fn new(key: &'a [u8]) -> Self {
        Self { slice: key }
    }

    #[inline]
    fn read_convert<ZK, T, F>(&mut self, mut convert: F) -> Result<T, Culprit<KeyDecodeErr>>
    where
        ZK: TryFromBytes + KnownLayout + Immutable + Unaligned + 'a,
        F: FnMut(&ZK) -> Result<T, Culprit<KeyDecodeErr>>,
    {
        let (zk, rest) = ZK::try_ref_from_prefix(&self.slice)?;
        self.slice = rest;
        convert(zk)
    }

    #[inline]
    fn close(&self) -> Result<(), Culprit<KeyDecodeErr>> {
        if !self.slice.is_empty() {
            Err(KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidSize).into())
        } else {
            Ok(())
        }
    }
}

pub struct HandleKey(HandleId);

impl HandleKey {
    #[inline]
    pub fn new(hid: HandleId) -> Self {
        Self(hid)
    }

    #[inline]
    pub fn handle(&self) -> &HandleId {
        &self.0
    }
}

impl From<HandleKey> for Slice {
    fn from(key: HandleKey) -> Slice {
        key.0.as_bytes().into()
    }
}

impl TryFrom<Slice> for HandleKey {
    type Error = Culprit<KeyDecodeErr>;

    fn try_from(slice: Slice) -> Result<Self, Self::Error> {
        Ok(Self(HandleId::try_from(slice.as_ref()).or_into_ctx()?))
    }
}

#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, Copy, PartialEq, Eq,
)]
#[repr(u8)]
pub enum VolumeProp {
    Control = 1,
    Checkpoints = 2,
}

/// Key for the `volumes` partition
#[derive(
    Debug, KnownLayout, Immutable, TryFromBytes, IntoBytes, Unaligned, Clone, PartialEq, Eq,
)]
#[repr(C)]
pub struct VolumeKey {
    vid: VolumeId,
    prop: VolumeProp,
}

impl VolumeKey {
    #[inline]
    pub fn new(vid: VolumeId, prop: VolumeProp) -> Self {
        Self { vid, prop }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn property(&self) -> VolumeProp {
        self.prop
    }

    /// Attempts to directly transmute a byte slice into a &VolumeKey.
    #[inline]
    pub fn try_ref_from_bytes(bytes: &[u8]) -> Result<&Self, Culprit<KeyDecodeErr>> {
        Self::try_ref_from_unaligned_bytes(bytes).or_ctx(KeyDecodeErr::CorruptKey)
    }
}

impl From<VolumeKey> for Slice {
    fn from(key: VolumeKey) -> Slice {
        key.as_bytes().into()
    }
}

impl TryFrom<Slice> for VolumeKey {
    type Error = Culprit<KeyDecodeErr>;

    fn try_from(slice: Slice) -> Result<Self, Self::Error> {
        Ok(Self::try_read_from_bytes(slice.as_ref())?)
    }
}

/// Key for the `log` partition
pub struct CommitKey {
    vid: VolumeId,
    lsn: LSN,
}

impl CommitKey {
    #[inline]
    pub fn new(vid: VolumeId, lsn: LSN) -> Self {
        Self { vid, lsn }
    }

    #[inline]
    pub fn vid(&self) -> &VolumeId {
        &self.vid
    }

    #[inline]
    pub fn lsn(&self) -> &LSN {
        &self.lsn
    }
}

impl From<CommitKey> for Slice {
    fn from(key: CommitKey) -> Slice {
        KeyBuilder::new(/* vid = */ 16 + /* lsn = */ 8)
            .put_slice(key.vid.as_bytes())
            .put_slice(CBE64::from(key.lsn).as_bytes())
            .build()
    }
}

impl TryFrom<Slice> for CommitKey {
    type Error = Culprit<KeyDecodeErr>;

    fn try_from(slice: Slice) -> Result<Self, Self::Error> {
        let mut reader = KeyReader::new(slice.as_ref());
        let key = Self {
            vid: reader.read_convert::<VolumeId, _, _>(|vid| Ok(vid.clone()))?,
            lsn: reader.read_convert::<CBE64, _, _>(|cbe| Ok(LSN::try_from(cbe)?))?,
        };
        reader.close()?;
        Ok(key)
    }
}

/// Key for the `pages` partition
pub struct PageKey {
    sid: SegmentId,
    pageidx: PageIdx,
}

impl PageKey {
    #[inline]
    pub fn new(sid: SegmentId, pageidx: PageIdx) -> Self {
        Self { sid, pageidx }
    }

    #[inline]
    pub fn sid(&self) -> &SegmentId {
        &self.sid
    }

    #[inline]
    pub fn pageidx(&self) -> &PageIdx {
        &self.pageidx
    }
}

impl From<PageKey> for Slice {
    fn from(key: PageKey) -> Slice {
        KeyBuilder::new(/* sid = */ 16 + /* pageidx = */ 4)
            .put_slice(key.sid.as_bytes())
            .put_slice(CBE32::from(key.pageidx).as_bytes())
            .build()
    }
}

impl TryFrom<Slice> for PageKey {
    type Error = Culprit<KeyDecodeErr>;

    fn try_from(slice: Slice) -> Result<Self, Self::Error> {
        let mut reader = KeyReader::new(slice.as_ref());
        let key = Self {
            sid: reader.read_convert::<SegmentId, _, _>(|sid| Ok(sid.clone()))?,
            pageidx: reader.read_convert::<CBE32, _, _>(|cbe| Ok(PageIdx::try_from(cbe)?))?,
        };
        reader.close()?;
        Ok(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_matches::assert_matches;
    use bytes::BytesMut;

    #[graft_test::test]
    fn handle_key_roundtrip() {
        let slice: Slice = HandleKey::new(HandleId::new("test-handle").unwrap()).into();
        let parsed: HandleKey = slice.try_into().unwrap();
        assert_eq!(parsed.handle().as_str(), "test-handle");
    }

    #[graft_test::test]
    fn handle_key_invalid() {
        // invalid characters
        let slice: Slice = Slice::from("bad id");
        let err: Culprit<KeyDecodeErr> = HandleKey::try_from(slice).err().unwrap();
        assert_matches!(*err.ctx(), KeyDecodeErr::InvalidHandleId(_));

        // empty
        let slice: Slice = Slice::from("");
        let err: Culprit<KeyDecodeErr> = HandleKey::try_from(slice).err().unwrap();
        assert_matches!(*err.ctx(), KeyDecodeErr::InvalidHandleId(_));

        // too long
        let long = "a".repeat(graft_core::handle_id::MAX_HANDLE_ID_LEN + 1);
        let slice: Slice = long.as_bytes().into();
        let err: Culprit<KeyDecodeErr> = HandleKey::try_from(slice).err().unwrap();
        assert_matches!(*err.ctx(), KeyDecodeErr::InvalidHandleId(_));
    }

    #[graft_test::test]
    fn volume_key_roundtrip() {
        let vid = VolumeId::random();
        for prop in [VolumeProp::Control, VolumeProp::Checkpoints] {
            let key = VolumeKey::new(vid.clone(), prop);
            let slice: Slice = key.clone().into();
            let parsed: VolumeKey = slice.try_into().unwrap();
            assert_eq!(parsed, key);
        }
    }

    #[graft_test::test]
    fn volume_key_try_ref_from_bytes() {
        let vid = VolumeId::random();
        for prop in [VolumeProp::Control, VolumeProp::Checkpoints] {
            let key = VolumeKey::new(vid.clone(), prop);
            let slice: Slice = key.clone().into();
            let parsed: &VolumeKey = VolumeKey::try_ref_from_bytes(slice.as_ref()).unwrap();
            assert_eq!(parsed, &key);
        }
    }

    #[graft_test::test]
    fn volume_key_invalid() {
        let vid = VolumeId::random();

        // wrong size (missing property byte)
        let mut bytes = vid.as_bytes().to_vec();
        let slice: Slice = bytes.clone().into();
        let err: Culprit<KeyDecodeErr> = VolumeKey::try_from(slice).err().unwrap();
        assert_matches!(
            *err.ctx(),
            KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidSize)
        );

        // empty
        let slice: Slice = Slice::from("");
        let err: Culprit<KeyDecodeErr> = VolumeKey::try_from(slice).err().unwrap();
        assert_matches!(
            *err.ctx(),
            KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidSize)
        );

        // invalid enum tag
        bytes.push(0xff);
        let slice: Slice = bytes.into();
        let err: Culprit<KeyDecodeErr> = VolumeKey::try_from(slice).err().unwrap();
        assert_matches!(
            *err.ctx(),
            KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidData)
        );
    }

    #[graft_test::test]
    fn commit_key_roundtrip() {
        let vid = VolumeId::random();
        let lsn = LSN::new(123);

        let mut buf = BytesMut::with_capacity(16 + 8);
        buf.extend_from_slice(vid.as_bytes());
        buf.extend_from_slice(CBE64::from(lsn).as_bytes());
        let slice: Slice = buf.freeze().into();

        let parsed: CommitKey = slice.clone().try_into().unwrap();
        assert_eq!(parsed.vid(), &vid);
        assert_eq!(parsed.lsn(), &lsn);

        let mut buf = BytesMut::with_capacity(16 + 8);
        buf.extend_from_slice(parsed.vid().as_bytes());
        buf.extend_from_slice(CBE64::from(*parsed.lsn()).as_bytes());
        let encoded: Slice = buf.freeze().into();
        assert_eq!(slice.as_ref(), encoded.as_ref());
    }

    #[graft_test::test]
    fn commit_key_invalid() {
        let vid = VolumeId::random();

        // zero LSN is invalid
        let mut builder = BytesMut::new();
        builder.extend_from_slice(vid.as_bytes());
        builder.extend_from_slice(CBE64::new(0).as_bytes());
        let slice: Slice = builder.freeze().into();
        let err: Culprit<KeyDecodeErr> = CommitKey::try_from(slice).err().unwrap();
        assert_matches!(*err.ctx(), KeyDecodeErr::InvalidLSN(_));

        // wrong size
        let slice: Slice = Slice::from("short");
        let err: Culprit<KeyDecodeErr> = CommitKey::try_from(slice).err().unwrap();
        assert_matches!(
            *err.ctx(),
            KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidSize)
        );

        // empty
        let slice: Slice = Slice::from("");
        let err: Culprit<KeyDecodeErr> = CommitKey::try_from(slice).err().unwrap();
        assert_matches!(
            *err.ctx(),
            KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidSize)
        );
    }

    #[graft_test::test]
    fn page_key_roundtrip() {
        let sid = SegmentId::random();
        let idx = PageIdx::new(5);

        let mut buf = BytesMut::with_capacity(16 + 4);
        buf.extend_from_slice(sid.as_bytes());
        buf.extend_from_slice(CBE32::from(idx).as_bytes());
        let slice: Slice = buf.freeze().into();

        let parsed: PageKey = slice.clone().try_into().unwrap();
        assert_eq!(parsed.sid(), &sid);
        assert_eq!(parsed.pageidx(), &idx);

        let mut buf = BytesMut::with_capacity(16 + 4);
        buf.extend_from_slice(parsed.sid().as_bytes());
        buf.extend_from_slice(CBE32::from(*parsed.pageidx()).as_bytes());
        let encoded: Slice = buf.freeze().into();
        assert_eq!(slice.as_ref(), encoded.as_ref());
    }

    #[graft_test::test]
    fn page_key_invalid() {
        let sid = SegmentId::random();

        // zero page index
        let mut builder = BytesMut::new();
        builder.extend_from_slice(sid.as_bytes());
        builder.extend_from_slice(CBE32::new(0).as_bytes());
        let slice: Slice = builder.freeze().into();
        let err: Culprit<KeyDecodeErr> = PageKey::try_from(slice).err().unwrap();
        assert_matches!(*err.ctx(), KeyDecodeErr::InvalidPageIdx(_));

        // wrong size
        let slice: Slice = Slice::from("short");
        let err: Culprit<KeyDecodeErr> = PageKey::try_from(slice).err().unwrap();
        assert_matches!(
            *err.ctx(),
            KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidSize)
        );

        // empty
        let slice: Slice = Slice::from("");
        let err: Culprit<KeyDecodeErr> = PageKey::try_from(slice).err().unwrap();
        assert_matches!(
            *err.ctx(),
            KeyDecodeErr::CorruptKey(ZerocopyErr::InvalidSize)
        );
    }
}
