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
