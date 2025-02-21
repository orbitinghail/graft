use std::{collections::BTreeMap, fmt::Debug};

use bytes::Bytes;
use graft_core::{PageIdx, VolumeId};
use splinter::{Splinter, SplinterRef};

#[derive(Default)]
pub struct MultiGraft(BTreeMap<VolumeId, SplinterRef<Bytes>>);

impl Debug for MultiGraft {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("MultiGraft");
        for (vid, splinter) in &self.0 {
            out.field(&vid.short(), &splinter.cardinality());
        }
        out.finish()
    }
}

impl MultiGraft {
    pub fn builder() -> MultiGraftBuilder {
        Default::default()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn get(&self, vid: &VolumeId) -> Option<&SplinterRef<Bytes>> {
        self.0.get(vid)
    }

    pub fn contains(&self, vid: &VolumeId, pageidx: PageIdx) -> bool {
        self.0
            .get(vid)
            .map(|graft| graft.contains(pageidx.into()))
            .unwrap_or(false)
    }
}

#[derive(Default)]
pub struct MultiGraftBuilder {
    map: MultiGraft,
    vid: Option<VolumeId>,
    splinter: Splinter,
}

impl MultiGraftBuilder {
    pub fn insert(&mut self, vid: &VolumeId, pageidx: PageIdx) {
        if let Some(current) = &self.vid {
            if current != vid {
                assert!(vid > current, "Volumes must be inserted in order by ID");

                let splinter = std::mem::take(&mut self.splinter);
                self.map
                    .0
                    .insert(current.clone(), splinter.serialize_to_splinter_ref());
                self.vid = Some(vid.clone());
            }
        } else {
            self.vid = Some(vid.clone());
        }

        self.splinter.insert(pageidx.into())
    }

    pub fn build(self) -> MultiGraft {
        let Self { mut map, vid, splinter } = self;

        if !splinter.is_empty() {
            assert!(vid.is_some(), "Non-empty builder must have a volume ID");
            map.0
                .insert(vid.unwrap(), splinter.serialize_to_splinter_ref());
        }

        map
    }
}
