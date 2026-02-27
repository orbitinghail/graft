use foldhash::fast::RandomState;
use hashbrown::hash_table::{AbsentEntry, Entry, IntoIter, Iter, IterMut, OccupiedEntry};
use std::{
    borrow::Borrow,
    hash::{BuildHasher, Hash},
};

pub trait HTEntry {
    type Key: Hash + Clone + PartialEq + Eq;
    fn key(&self) -> &Self::Key;
}

pub trait HTFromKey: HTEntry {
    fn from_key(key: Self::Key) -> Self;
}

#[derive(Debug, Clone)]
pub struct HashTable<T: HTEntry> {
    table: hashbrown::HashTable<T>,
    hash_builder: RandomState,
}

impl<T: HTEntry> Default for HashTable<T> {
    fn default() -> Self {
        Self {
            table: Default::default(),
            hash_builder: RandomState::default(),
        }
    }
}

impl<T: HTFromKey> HashTable<T> {
    /// Returns a mutable reference to the value corresponding to the key.
    /// If the key is not present in the table, it is inserted with a default value.
    pub fn get_mut(&mut self, key: &T::Key) -> &mut T {
        self.entry(key)
            .or_insert_with(|| T::from_key(key.clone()))
            .into_mut()
    }

    /// Ensures a value for the key by inserting the default value if it is not present.
    pub fn ensure(&mut self, key: &T::Key) {
        self.entry(key).or_insert_with(|| T::from_key(key.clone()));
    }
}

impl<T: HTEntry> HashTable<T> {
    fn h<K>(&self, key: &K) -> u64
    where
        T::Key: Borrow<K> + PartialEq<K>,
        K: Hash + Eq + ?Sized,
    {
        self.hash_builder.hash_one(key)
    }

    pub fn len(&self) -> usize {
        self.table.len()
    }

    pub fn is_empty(&self) -> bool {
        self.table.is_empty()
    }

    pub fn has<K>(&self, key: &K) -> bool
    where
        T::Key: Borrow<K> + PartialEq<K>,
        K: Hash + Eq + ?Sized,
    {
        self.find(key).is_some()
    }

    /// Finds a reference to the value corresponding to the key
    /// or returns `None` if the key is not present in the table.
    pub fn find<K>(&self, key: &K) -> Option<&T>
    where
        T::Key: Borrow<K> + PartialEq<K>,
        K: Hash + Eq + ?Sized,
    {
        self.table.find(self.h(key), |entry| entry.key() == key)
    }

    /// Finds a mutable reference to the value corresponding to the key
    /// or returns `None` if the key is not present in the table.
    pub fn find_mut<K>(&mut self, key: &K) -> Option<&mut T>
    where
        T::Key: Borrow<K> + PartialEq<K>,
        K: Hash + Eq + ?Sized,
    {
        self.table.find_mut(self.h(key), |entry| entry.key() == key)
    }

    pub fn insert(&mut self, item: T) {
        match self.entry(item.key()) {
            Entry::Occupied(mut entry) => {
                *entry.get_mut() = item;
            }
            Entry::Vacant(entry) => {
                entry.insert(item);
            }
        }
    }

    pub fn extract_if<'a, F>(&'a mut self, f: F) -> impl Iterator<Item = T> + 'a
    where
        F: FnMut(&mut T) -> bool + 'a,
    {
        self.table.extract_if(f)
    }

    pub fn remove<K>(&mut self, key: &K) -> Option<T>
    where
        T::Key: Borrow<K> + PartialEq<K>,
        K: Hash + Eq + ?Sized,
    {
        if let Ok(entry) = self
            .table
            .find_entry(self.h(key), |entry| entry.key() == key)
        {
            let (v, _) = entry.remove();
            Some(v)
        } else {
            None
        }
    }

    pub fn find_entry<K>(&mut self, key: &K) -> Result<OccupiedEntry<'_, T>, AbsentEntry<'_, T>>
    where
        T::Key: Borrow<K> + PartialEq<K>,
        K: Hash + Eq + ?Sized,
    {
        self.table
            .find_entry(self.hash_builder.hash_one(key), |entry| entry.key() == key)
    }

    pub fn entry<K>(&mut self, key: &K) -> Entry<'_, T>
    where
        T::Key: Borrow<K> + PartialEq<K>,
        K: Hash + Eq + ?Sized,
    {
        let hb = &self.hash_builder;
        self.table.entry(
            hb.hash_one(key),
            |entry| entry.key() == key,
            |entry| hb.hash_one(entry.key()),
        )
    }

    #[inline]
    pub fn iter(&self) -> Iter<'_, T> {
        self.table.iter()
    }

    #[inline]
    pub fn iter_mut(&mut self) -> IterMut<'_, T> {
        self.table.iter_mut()
    }

    pub fn first_key(&self) -> Option<T::Key> {
        self.iter().next().map(|entry| entry.key().clone())
    }
}

impl<T: HTEntry> IntoIterator for HashTable<T> {
    type Item = T;
    type IntoIter = IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.table.into_iter()
    }
}

impl<'a, T: HTEntry> IntoIterator for &'a HashTable<T> {
    type Item = &'a T;
    type IntoIter = Iter<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl<'a, T: HTEntry> IntoIterator for &'a mut HashTable<T> {
    type Item = &'a mut T;
    type IntoIter = IterMut<'a, T>;

    #[inline]
    fn into_iter(self) -> Self::IntoIter {
        self.iter_mut()
    }
}
