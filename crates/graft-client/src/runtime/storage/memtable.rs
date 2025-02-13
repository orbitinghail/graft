use std::collections::{btree_map::IntoIter, BTreeMap};

use graft_core::{page::Page, page_offset::PageOffset};

#[derive(Default, Debug, Clone)]
pub struct Memtable {
    pages: BTreeMap<PageOffset, Page>,
}

impl Memtable {
    pub fn truncate(&mut self, max_offset: Option<PageOffset>) {
        if let Some(max_offset) = max_offset {
            self.pages.retain(|k, _| k <= &max_offset);
        } else {
            self.pages.clear();
        }
    }

    pub fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub fn insert(&mut self, offset: PageOffset, page: Page) {
        self.pages.insert(offset, page);
    }

    pub fn get(&self, offset: PageOffset) -> Option<&Page> {
        self.pages.get(&offset)
    }
}

impl IntoIterator for Memtable {
    type Item = (PageOffset, Page);
    type IntoIter = IntoIter<PageOffset, Page>;

    fn into_iter(self) -> Self::IntoIter {
        self.pages.into_iter()
    }
}
