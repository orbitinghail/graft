use super::{Splinter, SplinterRef};

// Splinter == Splinter
impl PartialEq for Splinter {
    fn eq(&self, other: &Self) -> bool {
        self.partitions == other.partitions
    }
}

// SplinterRef == Splinter
impl<T: AsRef<[u8]>> PartialEq<SplinterRef<T>> for Splinter {
    fn eq(&self, other: &SplinterRef<T>) -> bool {
        other.load_partitions() == self.partitions
    }
}

// Splinter == SplinterRef
impl<T: AsRef<[u8]>> PartialEq<Splinter> for SplinterRef<T> {
    fn eq(&self, other: &Splinter) -> bool {
        self.load_partitions() == other.partitions
    }
}

// SplinterRef == SplinterRef
impl<T1: AsRef<[u8]>, T2: AsRef<[u8]>> PartialEq<SplinterRef<T2>> for SplinterRef<T1> {
    fn eq(&self, other: &SplinterRef<T2>) -> bool {
        self.load_partitions() == other.load_partitions()
    }
}
