type Segment = u8;

pub trait Relation {
    type Val<'a>
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
    fn get(&self, key: Segment) -> Option<Self::Val<'_>>;

    /// Returns an iterator over the key-value pairs of the relation sorted by key.
    fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::Val<'_>)>;

    /// Returns an iterator over the values of the relation sorted by key.
    fn sorted_values(&self) -> impl Iterator<Item = Self::Val<'_>>;

    fn inner_join<'a, R>(
        &'a self,
        right: &'a R,
    ) -> impl Iterator<Item = (Segment, Self::Val<'a>, R::Val<'a>)>
    where
        R: Relation,
    {
        self.sorted_iter()
            .filter_map(|(k, l)| right.get(k).map(|r| (k, l, r)))
    }
}

pub trait Intersection<Rhs = Self> {
    type Output;

    /// Returns the intersection between self and other
    fn intersection(&self, rhs: Rhs) -> Self::Output;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;

    struct Container<T> {
        data: BTreeMap<Segment, T>,
    }

    impl<T> Relation for Container<T> {
        type Val<'a> = &'a T where Self: 'a;

        fn len(&self) -> usize {
            self.data.len()
        }

        fn get(&self, key: Segment) -> Option<Self::Val<'_>> {
            self.data.get(&key)
        }

        fn sorted_iter(&self) -> impl Iterator<Item = (Segment, Self::Val<'_>)> {
            self.data.iter().map(|(k, v)| (*k, v))
        }

        fn sorted_values(&self) -> impl Iterator<Item = Self::Val<'_>> {
            self.data.values()
        }
    }

    impl Intersection for &usize {
        type Output = usize;

        fn intersection(&self, rhs: &usize) -> Self::Output {
            *self & *rhs
        }
    }

    // impl<T> Intersection<&Container<T>> for Container<T>
    // where
    //     T: Intersection,
    // {
    //     type Output = Container<T::Output>;

    //     fn intersection(&self, rhs: Container<T>) -> Self::Output {
    //         let mut out = Container { data: Default::default() };
    //         let iter = self
    //             .inner_join(rhs)
    //             .map(|(key, l, r)| (key, l.intersection(r)));
    //         for (key, inter) in iter {
    //             out.data.insert(key, inter);
    //         }
    //         out
    //     }
    // }

    // #[test]
    // fn test_inner_join() {
    //     let left = Container { data: [(1, 1), (2, 2), (3, 3)].into() };
    //     let right = Container { data: [(2, 2), (3, 3), (4, 4)].into() };

    //     let joined: Vec<_> = left.inner_join(&right).collect();
    //     assert_eq!(joined, [(2, &2, &2), (3, &3, &3)]);

    //     let inter: Vec<_> = left.intersection(&right).collect();
    //     assert_eq!(inter, [(2, 2), (3, 3)]);
    // }
}
