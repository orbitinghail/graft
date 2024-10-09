use std::{collections::BTreeMap, fmt::Debug};

use bytes::{Bytes, BytesMut};
use graft_core::{guid::VolumeId, offset::Offset};
use splinter::{writer::SplinterBuilder, Splinter};

#[derive(Default, Clone)]
pub struct OffsetsMap(BTreeMap<VolumeId, Splinter<Bytes>>);

impl Debug for OffsetsMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("OffsetsMap");
        for (vid, splinter) in &self.0 {
            out.field(&vid.short(), &splinter.cardinality());
        }
        out.finish()
    }
}

#[derive(Default)]
pub struct OffsetsMapBuilder {
    map: OffsetsMap,
    vid: Option<VolumeId>,
    builder: SplinterBuilder<BytesMut>,
}

impl OffsetsMap {
    pub fn builder() -> OffsetsMapBuilder {
        Default::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, vid: &VolumeId) -> Option<&Splinter<Bytes>> {
        self.0.get(vid)
    }

    pub fn contains(&self, vid: &VolumeId, offset: Offset) -> bool {
        self.0
            .get(vid)
            .map(|splinter| splinter.contains(offset))
            .unwrap_or(false)
    }
}

impl OffsetsMapBuilder {
    pub fn insert(&mut self, vid: VolumeId, offset: Offset) {
        if let Some(current) = &self.vid {
            if *current != vid {
                assert!(vid > *current, "Volumes must be inserted in order by ID");

                let builder = std::mem::take(&mut self.builder);
                let splinter = Splinter::from_bytes(builder.build().freeze()).unwrap();
                self.map.0.insert(current.clone(), splinter);
                self.vid = Some(vid);
            }
        } else {
            self.vid = Some(vid);
        }

        self.builder.push(offset)
    }

    pub fn build(self) -> OffsetsMap {
        let Self { mut map, vid, builder } = self;

        if !builder.is_empty() {
            assert!(vid.is_some(), "Non-empty builder must have a volume ID");
            let splinter = Splinter::from_bytes(builder.build().freeze()).unwrap();
            map.0.insert(vid.unwrap(), splinter);
        }

        map
    }
}
