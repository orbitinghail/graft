use std::collections::{btree_map::IntoIter, BTreeMap};

use graft_core::{page::Page, PageIdx};

#[derive(Default, Debug, Clone)]
pub struct Memtable {
    pages: BTreeMap<PageIdx, Page>,
}

impl Memtable {
    pub fn truncate(&mut self, max_pageidx: Option<PageIdx>) {
        if let Some(max_pageidx) = max_pageidx {
            // remove all pages with pageidx > max_pageidx
            let _ = self.pages.split_off(&max_pageidx.saturating_next());
        } else {
            self.pages.clear();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn insert(&mut self, pageidx: PageIdx, page: Page) {
        self.pages.insert(pageidx, page);
    }

    pub fn get(&self, pageidx: PageIdx) -> Option<&Page> {
        self.pages.get(&pageidx)
    }
}

impl IntoIterator for Memtable {
    type Item = (PageIdx, Page);
    type IntoIter = IntoIter<PageIdx, Page>;

    fn into_iter(self) -> Self::IntoIter {
        self.pages.into_iter()
    }
}
