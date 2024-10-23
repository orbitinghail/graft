use crate::{ops::Intersection, Segment};

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

    /// Returns an iterator over the values of the relation sorted by key.
    fn sorted_values(&self) -> impl Iterator<Item = Self::ValRef<'_>>;

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

    /// Returns an iterator over the outer join of two relations.
    fn outer_join<'a, R: Relation>(
        &'a self,
        right: &'a R,
    ) -> impl Iterator<Item = (Segment, Option<Self::ValRef<'a>>, Option<R::ValRef<'a>>)> {
        let mut left = self.sorted_iter().peekable();
        let mut right = right.sorted_iter().peekable();

        std::iter::from_fn(move || match (left.peek(), right.peek()) {
            // lk == rk
            (Some(&(lk, _)), Some(&(rk, _))) if lk == rk => Some((
                lk,
                left.next().map(|(_, v)| v),
                right.next().map(|(_, v)| v),
            )),

            // lk < rk
            (Some(&(lk, _)), Some(&(rk, _))) if lk < rk => {
                Some((lk, left.next().map(|(_, v)| v), None))
            }

            // lk > rk
            (Some(&(_, _)), Some(&(rk, _))) => Some((rk, None, right.next().map(|(_, v)| v))),

            // right is exhausted
            (Some(&(lk, _)), None) => Some((lk, left.next().map(|(_, v)| v), None)),

            // left is exhausted
            (None, Some(&(rk, _))) => Some((rk, None, right.next().map(|(_, v)| v))),

            // both are exhausted
            (None, None) => None,
        })
    }
}

pub trait RelationMut: Relation {
    type Val;

    /// Returns an iterator over the key-value pairs of the relation sorted by key.
    /// The values are mutable
    fn sorted_iter_mut(&mut self) -> impl Iterator<Item = (Segment, &mut Self::Val)>;

    /// Returns an iterator over the inner join of two relations.
    /// The left values are mutable.
    fn inner_join_mut<'a, R: Relation>(
        &'a mut self,
        right: &'a R,
    ) -> impl Iterator<Item = (Segment, &mut Self::Val, R::ValRef<'a>)> {
        self.sorted_iter_mut()
            .filter_map(|(k, l)| right.get(k).map(|r| (k, l, r)))
    }
}

// impl<R, L> Intersection<R> for L
// where
//     R: Relation,
//     L: Relation,
//     for<'a> L::ValRef<'a>: Intersection<R::ValRef<'a>>,
// {
//     type Output = <<L as Relation>::ValRef<'_> as Intersection<R::ValRef<'_>>>::Output<'a> where Self: 'a;

//     fn intersection(&self, rhs: &R) -> Self::Output<'_> {
//         todo!()
//     }
// }

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::Segment;

    use super::Relation;

    struct TestRelation<T> {
        data: BTreeMap<Segment, T>,
    }

    impl<T> Relation for TestRelation<T> {
        type ValRef<'a> = &'a T
        where
            Self: 'a;

        fn len(&self) -> usize {
            self.data.len()
        }

        fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::ValRef<'_>)> {
            self.data.iter().map(|(k, v)| (*k, v))
        }

        fn sorted_values(&self) -> impl Iterator<Item = Self::ValRef<'_>> {
            self.data.values()
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
        let values: Vec<_> = relation.sorted_values().copied().collect();
        assert_eq!(values, [1, 2, 3]);
    }

    #[test]
    fn test_inner_join() {
        let left = TestRelation { data: [(1, 1), (2, 2), (3, 3)].into() };
        let right = TestRelation { data: [(2, 4), (3, 5), (4, 6)].into() };

        let joined: Vec<_> = left.inner_join(&right).collect();
        assert_eq!(joined, [(2, &2, &4), (3, &3, &5)]);
    }

    #[test]
    fn test_outer_join() {
        let left = TestRelation { data: [(1, 1), (2, 2), (3, 3)].into() };
        let right = TestRelation { data: [(2, 4), (3, 5), (4, 6)].into() };

        let joined: Vec<_> = left.outer_join(&right).collect();
        assert_eq!(
            joined,
            [
                (1, Some(&1), None),
                (2, Some(&2), Some(&4)),
                (3, Some(&3), Some(&5)),
                (4, None, Some(&6))
            ]
        );
    }
}
