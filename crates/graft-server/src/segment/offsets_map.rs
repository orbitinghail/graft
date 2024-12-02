use std::{collections::BTreeMap, fmt::Debug};

use bytes::Bytes;
use graft_core::{page_offset::PageOffset, VolumeId};
use splinter::{Splinter, SplinterRef};

#[derive(Default)]
pub struct OffsetsMap(BTreeMap<VolumeId, SplinterRef<Bytes>>);

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
    splinter: Splinter,
}

impl OffsetsMap {
    pub fn builder() -> OffsetsMapBuilder {
        Default::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, vid: &VolumeId) -> Option<&SplinterRef<Bytes>> {
        self.0.get(vid)
    }

    pub fn contains(&self, vid: &VolumeId, offset: PageOffset) -> bool {
        self.0
            .get(vid)
            .map(|splinter| splinter.contains(offset))
            .unwrap_or(false)
    }
}

impl OffsetsMapBuilder {
    pub fn insert(&mut self, vid: VolumeId, offset: PageOffset) {
        if let Some(current) = &self.vid {
            if *current != vid {
                assert!(vid > *current, "Volumes must be inserted in order by ID");

                let splinter = std::mem::take(&mut self.splinter);
                self.map
                    .0
                    .insert(current.clone(), splinter.serialize_to_splinter_ref());
                self.vid = Some(vid);
            }
        } else {
            self.vid = Some(vid);
        }

        self.splinter.insert(offset)
    }

    pub fn build(self) -> OffsetsMap {
        let Self { mut map, vid, splinter } = self;

        if !splinter.is_empty() {
            assert!(vid.is_some(), "Non-empty builder must have a volume ID");
            map.0
                .insert(vid.unwrap(), splinter.serialize_to_splinter_ref());
        }

        map
    }
}
