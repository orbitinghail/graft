pub trait Cut<Rhs = Self> {
    type Output;

    /// Returns the intersection between self and other while removing the
    /// intersection from self
    fn cut(&mut self, rhs: &Rhs) -> Self::Output;
}

pub trait Intersection<Rhs = Self> {
    type Output;

    /// Returns the intersection between self and other
    fn intersection(&self, rhs: &Rhs) -> Self::Output;
}

pub trait Union<Rhs = Self> {
    type Output;

    /// Returns the union between self and other
    fn union(&self, rhs: &Rhs) -> Self::Output;
}

pub trait Merge<Rhs = Self> {
    /// Merges rhs into self
    fn merge(&mut self, rhs: &Rhs);
}
