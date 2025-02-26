use crate::Segment;

pub trait Relation {
    type ValRef<'a>
    where
        Self: 'a;

    /// Returns the number of values in the relation.
    fn len(&self) -> usize;

    /// Returns true if the relation contains no values.
    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the value associated with the given key.
    fn get(&self, key: Segment) -> Option<Self::ValRef<'_>>;

    /// Returns an iterator over the key-value pairs of the relation sorted by key.
    fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)>;

    /// Returns an iterator over the inner join of two relations.
    fn inner_join<'a, R>(
        &'a self,
        right: &'a R,
    ) -> impl Iterator<Item = (Segment, Self::ValRef<'a>, R::ValRef<'a>)>
    where
        R: Relation,
    {
        self.sorted_iter()
            .filter_map(|(k, l)| right.get(k).map(|r| (k, l, r)))
    }
}

impl<T> Relation for &T
where
    T: Relation,
{
    type ValRef<'a>
        = T::ValRef<'a>
    where
        Self: 'a;

    fn len(&self) -> usize {
        (**self).len()
    }

    fn get(&self, key: Segment) -> Option<Self::ValRef<'_>> {
        (**self).get(key)
    }

    fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
        (**self).sorted_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::Segment;

    use super::Relation;

    struct TestRelation<T> {
        data: BTreeMap<Segment, T>,
    }

    impl<T> Relation for TestRelation<T> {
        type ValRef<'a>
            = &'a T
        where
            Self: 'a;

        fn len(&self) -> usize {
            self.data.len()
        }

        fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
            self.data.iter().map(|(k, v)| (*k, v))
        }

        fn get(&self, key: Segment) -> Option<Self::ValRef<'_>> {
            self.data.get(&key)
        }
    }

    #[test]
    fn test_len() {
        let relation = TestRelation { data: [(1, 1), (2, 2), (3, 3)].into() };
        assert_eq!(relation.len(), 3);
        assert!(!relation.is_empty());
    }

    #[test]
    fn test_values() {
        let relation = TestRelation { data: [(1, 1), (2, 2), (3, 3)].into() };
        let values: Vec<_> = relation.sorted_iter().map(|(_, b)| *b).collect();
        assert_eq!(values, [1, 2, 3]);
    }

    #[test]
    fn test_inner_join() {
        let left = TestRelation { data: [(1, 1), (2, 2), (3, 3)].into() };
        let right = TestRelation { data: [(2, 4), (3, 5), (4, 6)].into() };

        let joined: Vec<_> = left.inner_join(&right).collect();
        assert_eq!(joined, [(2, &2, &4), (3, &3, &5)]);
    }
}
