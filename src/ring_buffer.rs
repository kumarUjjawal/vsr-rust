use std::fmt::Debug;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RingBufferError {
    NoSpaceLeft,
}

#[derive(Clone)]
pub struct RingBuffer<T, const SIZE: usize> {
    buffer: [Option<T>; SIZE],
    index: usize,
    count: usize,
}

impl<T, const SIZE: usize> RingBuffer<T, SIZE> {
    pub fn new() -> Self {
        Self {
            buffer: std::array::from_fn(|_| None),
            index: 0,
            count: 0,
        }
    }
    pub fn is_full(&self) -> bool {
        self.count == SIZE
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn front(&self) -> Option<&T> {
        if self.is_empty() {
            None
        } else {
            self.buffer[self.index].as_ref()
        }
    }

    pub fn front_mut(&mut self) -> Option<&mut T> {
        if self.is_empty() {
            None
        } else {
            self.buffer[self.index].as_mut()
        }
    }

    pub fn push(&mut self, item: T) -> Result<(), RingBufferError> {
        if self.is_full() {
            return Err(RingBufferError::NoSpaceLeft);
        }

        let tail_index = (self.index + self.count) % SIZE;
        self.buffer[tail_index] = Some(item);
        self.count += 1;
        Ok(())
    }

    pub fn push_assume_capacity(&mut self, item: T) {
        self.push(item)
            .expect("push_assume_capacity called on a full RingBuffer");
    }

    pub fn pop(&mut self) -> Option<T> {
        if self.is_empty() {
            return None;
        }

        let mut slot = None;
        std::mem::swap(&mut slot, &mut self.buffer[self.index]);
        self.index = (self.index + 1) % SIZE;
        self.count -= 1;
        slot
    }

    pub fn iter(&self) -> RingBufferIter<'_, T, SIZE> {
        RingBufferIter {
            ring: self,
            offset: 0,
        }
    }

    pub fn find_mut<F>(&mut self, mut predicate: F) -> Option<&mut T>
    where
        F: FnMut(&T) -> bool,
    {
        for i in 0..self.count {
            let idx = (self.index + i) % SIZE;
            if let Some(item_ref) = self.buffer[idx].as_ref()
                && predicate(item_ref)
            {
                return self.buffer[idx].as_mut();
            }
        }
        None
    }

    pub fn clear(&mut self) {
        self.index = 0;
        self.count = 0;
    }
}

impl<T, const SIZE: usize> Default for RingBuffer<T, SIZE> {
    fn default() -> Self {
        Self::new()
    }
}

pub struct RingBufferIter<'a, T, const SIZE: usize> {
    ring: &'a RingBuffer<T, SIZE>,
    offset: usize,
}

impl<'a, T, const SIZE: usize> Iterator for RingBufferIter<'a, T, SIZE> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.ring.count {
            return None;
        }

        let index = (self.ring.index + self.offset) % SIZE;
        self.offset += 1;
        self.ring.buffer[index].as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_push_and_pop() {
        let mut ring: RingBuffer<u32, 3> = RingBuffer::new();
        assert!(ring.is_empty());

        ring.push(10).unwrap();
        ring.push(20).unwrap();

        assert!(!ring.is_empty());
        assert!(!ring.is_full());
        assert_eq!(ring.front(), Some(&10));

        ring.push(30).unwrap();
        assert!(ring.is_full());

        assert_eq!(ring.push(40), Err(RingBufferError::NoSpaceLeft));

        assert_eq!(ring.pop(), Some(10));
        assert_eq!(ring.pop(), Some(20));
        assert_eq!(ring.front(), Some(&30));
        ring.push(40).unwrap();

        assert_eq!(ring.pop(), Some(30));
        assert_eq!(ring.pop(), Some(40));
        assert_eq!(ring.pop(), None);
        assert!(ring.is_empty());
    }

    #[test]
    fn test_iterator() {
        let mut ring: RingBuffer<String, 4> = RingBuffer::new();
        ring.push("a".to_string()).unwrap();
        ring.push("b".to_string()).unwrap();
        ring.push("c".to_string()).unwrap();

        let collected: Vec<&String> = ring.iter().collect();
        assert_eq!(
            collected,
            vec![&"a".to_string(), &"b".to_string(), &"c".to_string()]
        );

        ring.pop(); // removes "a"

        let collected_after_pop: Vec<&String> = ring.iter().collect();
        assert_eq!(
            collected_after_pop,
            vec![&"b".to_string(), &"c".to_string()]
        );
    }

    #[test]
    fn test_with_non_copy_non_default_type() {
        // This struct cannot be copied or defaulted.
        #[derive(Debug, PartialEq)]
        struct NonCopy(String);

        let mut ring: RingBuffer<NonCopy, 2> = RingBuffer::new();

        ring.push(NonCopy("hello".into())).unwrap();
        ring.push(NonCopy("world".into())).unwrap();

        assert!(ring.is_full());
        assert_eq!(ring.front(), Some(&NonCopy("hello".into())));

        assert_eq!(ring.pop(), Some(NonCopy("hello".into())));
        assert_eq!(ring.pop(), Some(NonCopy("world".into())));
        assert!(ring.is_empty());
    }
}
