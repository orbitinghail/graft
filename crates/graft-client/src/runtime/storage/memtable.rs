use std::collections::{btree_map::IntoIter, BTreeMap};

use graft_core::{page::Page, PageIdx};

#[derive(Default, Debug, Clone)]
pub struct Memtable {
    pages: BTreeMap<PageIdx, Page>,
}

impl Memtable {
    pub fn truncate(&mut self, max_offset: Option<PageIdx>) {
        if let Some(max_offset) = max_offset {
            // remove all pages with offset > max_offset
            let _ = self.pages.split_off(&max_offset.saturating_next());
        } else {
            self.pages.clear();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn insert(&mut self, offset: PageIdx, page: Page) {
        self.pages.insert(offset, page);
    }

    pub fn get(&self, offset: PageIdx) -> Option<&Page> {
        self.pages.get(&offset)
    }
}

impl IntoIterator for Memtable {
    type Item = (PageIdx, Page);
    type IntoIter = IntoIter<PageIdx, Page>;

    fn into_iter(self) -> Self::IntoIter {
        self.pages.into_iter()
    }
}
