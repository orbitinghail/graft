use std::{error::Error, fmt::Write, sync::Arc};

use crate::caller_location::CallerLocation;

pub trait LocationStack: Error {
    fn location(&self) -> &CallerLocation;
    fn next(&self) -> Option<&dyn LocationStack>;
}

impl<T: LocationStack> LocationStack for &T {
    fn location(&self) -> &CallerLocation {
        (*self).location()
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        (*self).next()
    }
}

impl<T: LocationStack> LocationStack for Arc<T> {
    fn location(&self) -> &CallerLocation {
        self.as_ref().location()
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        self.as_ref().next()
    }
}

impl std::error::Error for Box<dyn LocationStack + Send + Sync> {}

impl LocationStack for Box<dyn LocationStack + Send + Sync> {
    fn location(&self) -> &CallerLocation {
        self.as_ref().location()
    }

    fn next(&self) -> Option<&dyn LocationStack> {
        self.as_ref().next()
    }
}

/// Walks the location stack, calling the callback for each value. Returns the deepest value in the stack
fn walk<T: LocationStack + ?Sized, E>(
    stack: &T,
    mut cb: impl FnMut(&dyn LocationStack) -> Result<(), E>,
) -> Result<Option<&dyn LocationStack>, E> {
    let Some(mut cursor) = stack.next() else {
        return Ok(None);
    };
    cb(cursor)?;
    while let Some(next) = cursor.next() {
        cursor = next;
        cb(cursor)?;
    }
    Ok(Some(cursor))
}

pub fn format_location_stack<W: Write, T: LocationStack + ?Sized>(
    f: &mut W,
    stack: &T,
) -> std::fmt::Result {
    write!(f, "{} at {}", stack, stack.location())?;
    let mut height = 1;

    let last = walk(stack, |err| {
        write!(f, "\n{}: {} at {}", height, err, err.location())?;
        height += 1;
        Ok(())
    })?;

    if let Some(source) = last.and_then(|last| last.source()) {
        write!(f, "\n{}: {}", height, source)?;
    }

    Ok(())
}

pub fn summarize_location_stack<W: Write, T: LocationStack + ?Sized>(
    f: &mut W,
    stack: &T,
) -> std::fmt::Result {
    write!(f, "{} at {}", stack, stack.location())?;
    let mut height = 1;

    // try to print the last error in the chain followed by it's source
    if let Some(last) = walk(stack, |_| Ok(()))? {
        write!(f, "\n{}: {} at {}", height, last, last.location())?;
        height += 1;
        if let Some(source) = last.source() {
            write!(f, "\n{height}: {}", source)?;
        }
    } else if let Some(source) = stack.source() {
        // we are the last error in the chain, print our source
        write!(f, "\n{height}: {}", source)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{error::Error, fmt::Display};

    use super::*;
    use crate::CallerLocation;

    #[derive(Debug)]
    struct TrivialError;

    impl Error for TrivialError {}

    impl Display for TrivialError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "TrivialError")
        }
    }

    #[derive(Debug)]
    struct Node {
        height: usize,
        loc: CallerLocation,
        next: Option<Box<Node>>,
    }

    impl Error for Node {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            Some(&TrivialError)
        }
    }

    impl Display for Node {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "Node({})", self.height)
        }
    }

    impl LocationStack for Node {
        fn location(&self) -> &crate::CallerLocation {
            &self.loc
        }

        fn next(&self) -> Option<&dyn LocationStack> {
            if let Some(next) = &self.next {
                Some(next.as_ref())
            } else {
                None
            }
        }
    }

    #[test]
    fn test_format_location_stack() {
        let loc = CallerLocation::default();
        let node = Node {
            height: 0,
            loc,
            next: Some(Box::new(Node {
                height: 1,
                loc,
                next: Some(Box::new(Node { height: 2, loc, next: None })),
            })),
        };

        let mut output = String::new();
        format_location_stack(&mut output, &node).unwrap();
        assert_eq!(
            output,
            format!(
                "Node(0) at {loc}\n\
                1: Node(1) at {loc}\n\
                2: Node(2) at {loc}\n\
                3: TrivialError",
            )
        );
    }

    #[test]
    fn test_summarize_location_stack() {
        let loc = CallerLocation::default();
        let node = Node {
            height: 0,
            loc,
            next: Some(Box::new(Node {
                height: 1,
                loc,
                next: Some(Box::new(Node { height: 2, loc, next: None })),
            })),
        };

        let mut output = String::new();
        summarize_location_stack(&mut output, &node).unwrap();
        assert_eq!(
            output,
            format!(
                "Node(0) at {loc}\n\
                1: Node(2) at {loc}\n\
                2: TrivialError",
            )
        );
    }
}
