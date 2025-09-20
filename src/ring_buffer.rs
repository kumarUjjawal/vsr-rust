pub struct RingBuffer<T, const SIZE: usize> {
    buffer: [T; SIZE],
    index: usize,
    count: usize,
}

impl<T: Copy + Default, const SIZE: usize> RingBuffer<T, SIZE> {
    pub fn new() -> Self {
        Self {
            buffer: [T::default(); SIZE],
            index: 0,
            count: 0,
        }
    }
    pub fn full(&self) -> bool {
        return self.count == self.buffer.len();
    }

    pub fn empty(&self) -> bool {
        return self.count == 0;
    }

    pub fn head(&self) -> Option<&T> {
        if self.empty() {
            None
        } else {
            Some(&self.buffer[self.index])
        }
    }

    pub fn head_mut(&mut self) -> Option<&mut T> {
        if self.empty() {
            None
        } else {
            Some(&mut self.buffer[self.index])
        }
    }

    pub fn tail(&self) -> Option<&T> {
        if self.empty() {
            None
        } else {
            Some(&self.buffer[(self.index + self.count - 1) % self.buffer.len()])
        }
    }

    pub fn tail_mut(&mut self) -> Option<&mut T> {
        if self.empty() {
            None
        } else {
            Some(&mut self.buffer[(self.index + self.count - 1) % self.buffer.len()])
        }
    }

    pub fn next_tail(&self) -> Option<&T> {
        if self.full() {
            None
        } else {
            Some(&self.buffer[(&self.index + self.count) % self.buffer.len()])
        }
    }

    pub fn next_tail_mut(&mut self) -> Option<&mut T> {
        if self.full() {
            None
        } else {
            Some(&mut self.buffer[(self.index + self.count) % self.buffer.len()])
        }
    }

    pub fn advance_head(&mut self) {
        self.index += 1;
        self.index %= self.buffer.len();
        self.count -= 1;
    }

    pub fn advance_tail(&mut self) {
        assert!(self.count < self.buffer.len());
        self.count += 1;
    }

    pub fn push(&mut self, item: T) -> Result<(), RingBufferError> {
        let next_tail = self.next_tail_mut().ok_or(RingBufferError::NoSpaceLeft)?;
        *next_tail = item;
        self.advance_tail();
        Ok(())
    }

    pub fn push_assume_capacity(&mut self, item: T) {
        match self.push(item) {
            Ok(()) => {}
            Err(RingBufferError::NoSpaceLeft) => {
                panic!("push_assume_capacity called on full buffer")
            }
        };
    }

    pub fn pop(&mut self) -> Option<T> {
        let result = match self.head() {
            Some(item) => *item,
            None => return None,
        };
        self.advance_head();
        Some(result)
    }

    pub fn iter(&self) -> RingBufferIter<'_, T, SIZE> {
        RingBufferIter {
            ring: self,
            count: 0,
        }
    }

    pub fn iter_mut(&mut self) -> RingBufferIterMut<'_, T, SIZE> {
        RingBufferIterMut {
            buffer: &mut self.buffer,
            ring_index: self.index,
            ring_count: self.count,
            current_count: 0,
        }
    }
}

pub struct RingBufferIter<'a, T, const SIZE: usize> {
    ring: &'a RingBuffer<T, SIZE>,
    count: usize,
}

impl<'a, T, const SIZE: usize> Iterator for RingBufferIter<'a, T, SIZE> {
    type Item = &'a T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.count >= self.ring.count {
            return None;
        }

        let index = (self.ring.index + self.count) % SIZE;
        let item = &self.ring.buffer[index];
        self.count += 1;
        Some(item)
    }
}

pub struct RingBufferIterMut<'a, T, const SIZE: usize> {
    buffer: &'a mut [T; SIZE],
    ring_index: usize,
    ring_count: usize,
    current_count: usize,
}

impl<'a, T, const SIZE: usize> Iterator for RingBufferIterMut<'a, T, SIZE> {
    type Item = &'a mut T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_count >= self.ring_count {
            return None;
        }

        let index = (self.ring_index + self.current_count) % SIZE;
        self.current_count += 1;

        // Safe because we know the bounds and have exclusive access
        unsafe {
            let ptr = self.buffer.as_mut_ptr().add(index);
            Some(&mut *ptr)
        }
    }
}

#[derive(Debug)]
pub enum RingBufferError {
    NoSpaceLeft,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_iterator<const SIZE: usize>(ring: &RingBuffer<u32, SIZE>, expected_values: &[u32]) {
        let ring_index = ring.index;

        for _ in 0..2 {
            let collected: Vec<u32> = ring.iter().copied().collect();
            assert_eq!(collected, expected_values);

            let mut iterator = ring.iter();
            let mut index = 0;
            while let Some(item) = iterator.next() {
                assert_eq!(expected_values[index], *item);
                index += 1;
            }
            assert_eq!(expected_values.len(), index);
        }

        assert_eq!(ring_index, ring.index);
    }

    #[test]
    fn test_low_level_interface() {
        let mut ring: RingBuffer<u32, 2> = RingBuffer::new();
        test_iterator(&ring, &[]);

        // Test empty ring
        assert_eq!(None, ring.head());
        assert_eq!(None, ring.head_mut().map(|r| r as *mut _));
        assert_eq!(None, ring.tail());
        assert_eq!(None, ring.tail_mut().map(|r| r as *mut _));

        // Add first element using low-level interface
        *ring.next_tail_mut().unwrap() = 0;
        ring.advance_tail();
        assert_eq!(Some(&0), ring.tail());
        assert_eq!(0, *ring.tail_mut().unwrap());
        test_iterator(&ring, &[0]);

        // Add second element
        *ring.next_tail_mut().unwrap() = 1;
        ring.advance_tail();
        assert_eq!(Some(&1), ring.tail());
        assert_eq!(1, *ring.tail_mut().unwrap());
        test_iterator(&ring, &[0, 1]);

        // Ring should be full now
        assert_eq!(None, ring.next_tail());
        assert_eq!(None, ring.next_tail_mut());

        // Remove first element
        assert_eq!(Some(&0), ring.head());
        assert_eq!(0, *ring.head_mut().unwrap());
        ring.advance_head();
        test_iterator(&ring, &[1]);

        // Add third element (wraps around)
        *ring.next_tail_mut().unwrap() = 2;
        ring.advance_tail();
        assert_eq!(Some(&2), ring.tail());
        assert_eq!(2, *ring.tail_mut().unwrap());
        test_iterator(&ring, &[1, 2]);

        // Test mutable iteration (equivalent to next_ptr in Zig)
        for item in ring.iter_mut() {
            *item += 1000;
        }

        assert_eq!(Some(&1001), ring.head());
        assert_eq!(1001, *ring.head_mut().unwrap());
        ring.advance_head();
        test_iterator(&ring, &[1002]);

        // Add fourth element
        *ring.next_tail_mut().unwrap() = 3;
        ring.advance_tail();
        assert_eq!(Some(&3), ring.tail());
        assert_eq!(3, *ring.tail_mut().unwrap());
        test_iterator(&ring, &[1002, 3]);

        // Remove remaining elements one by one
        assert_eq!(Some(&1002), ring.head());
        assert_eq!(1002, *ring.head_mut().unwrap());
        ring.advance_head();
        test_iterator(&ring, &[3]);

        assert_eq!(Some(&3), ring.head());
        assert_eq!(3, *ring.head_mut().unwrap());
        ring.advance_head();
        test_iterator(&ring, &[]);

        // Ring should be empty again
        assert_eq!(None, ring.head());
        assert_eq!(None, ring.head_mut());
        assert_eq!(None, ring.tail());
        assert_eq!(None, ring.tail_mut());
    }

    #[test]
    fn test_push_pop_high_level_interface() {
        let mut fifo: RingBuffer<u32, 3> = RingBuffer::new();

        assert!(!fifo.full());
        assert!(fifo.empty());

        fifo.push(1).unwrap();
        assert_eq!(Some(&1), fifo.head());

        assert!(!fifo.full());
        assert!(!fifo.empty());

        fifo.push(2).unwrap();
        assert_eq!(Some(&1), fifo.head());

        fifo.push(3).unwrap();

        // Should fail to push when full
        assert!(matches!(fifo.push(4), Err(RingBufferError::NoSpaceLeft)));

        assert!(fifo.full());
        assert!(!fifo.empty());

        assert_eq!(Some(&1), fifo.head());
        assert_eq!(Some(1), fifo.pop());

        assert!(!fifo.full());
        assert!(!fifo.empty());

        fifo.push(4).unwrap();

        assert_eq!(Some(2), fifo.pop());
        assert_eq!(Some(3), fifo.pop());
        assert_eq!(Some(4), fifo.pop());
        assert_eq!(None, fifo.pop());

        assert!(!fifo.full());
        assert!(fifo.empty());
    }

    #[test]
    fn test_mutable_iterator() {
        let mut ring: RingBuffer<u32, 3> = RingBuffer::new();

        ring.push(10).unwrap();
        ring.push(20).unwrap();
        ring.push(30).unwrap();

        // Modify all elements through mutable iterator
        for item in ring.iter_mut() {
            *item *= 2;
        }

        // Check that values were modified
        assert_eq!(Some(20), ring.pop());
        assert_eq!(Some(40), ring.pop());
        assert_eq!(Some(60), ring.pop());
    }

    #[test]
    fn test_wraparound_behavior() {
        let mut ring: RingBuffer<i32, 3> = RingBuffer::new();

        // Fill the ring
        ring.push(1).unwrap();
        ring.push(2).unwrap();
        ring.push(3).unwrap();

        // Remove one element
        assert_eq!(Some(1), ring.pop());

        // Add another (should wrap around)
        ring.push(4).unwrap();

        // Verify order is maintained
        let values: Vec<i32> = ring.iter().copied().collect();
        assert_eq!(vec![2, 3, 4], values);
    }

    #[test]
    fn test_empty_operations() {
        let mut ring: RingBuffer<u32, 3> = RingBuffer::new();

        assert!(ring.empty());
        assert!(!ring.full());
        assert_eq!(None, ring.head());
        assert_eq!(None, ring.tail());
        assert_eq!(None, ring.pop());

        let values: Vec<&u32> = ring.iter().collect();
        let expanded: Vec<&u32> = vec![];
        assert_eq!(values, expanded);
    }

    #[test]
    fn test_full_operations() {
        let mut ring: RingBuffer<u32, 2> = RingBuffer::new();

        ring.push(1).unwrap();
        ring.push(2).unwrap();

        assert!(!ring.empty());
        assert!(ring.full());
        assert_eq!(None, ring.next_tail());
        assert_eq!(None, ring.next_tail_mut());

        assert!(matches!(ring.push(3), Err(RingBufferError::NoSpaceLeft)));
    }

    #[test]
    fn test_push_assume_capacity() {
        let mut ring: RingBuffer<u32, 3> = RingBuffer::new();

        ring.push_assume_capacity(1);
        ring.push_assume_capacity(2);
        ring.push_assume_capacity(3);

        assert_eq!(Some(1), ring.pop());
        assert_eq!(Some(2), ring.pop());
        assert_eq!(Some(3), ring.pop());
    }

    #[test]
    #[should_panic(expected = "push_assume_capacity called on full buffer")]
    fn test_push_assume_capacity_panic() {
        let mut ring: RingBuffer<u32, 2> = RingBuffer::new();

        ring.push_assume_capacity(1);
        ring.push_assume_capacity(2);
        ring.push_assume_capacity(3); // Should panic here
    }

    #[test]
    fn test_edge_cases() {
        let mut ring: RingBuffer<u32, 1> = RingBuffer::new();

        // Single element ring buffer
        ring.push(42).unwrap();
        assert!(ring.full());
        assert_eq!(Some(&42), ring.head());
        assert_eq!(Some(&42), ring.tail());

        assert_eq!(Some(42), ring.pop());
        assert!(ring.empty());
    }
}
