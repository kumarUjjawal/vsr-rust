use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
pub struct Node<T> {
    data: T,
    next: Option<Rc<RefCell<Node<T>>>>,
}

#[derive(Debug)]
pub struct FIFO<T> {
    head: Option<Rc<RefCell<Node<T>>>>,
    tail: Option<Rc<RefCell<Node<T>>>>,
}

impl<T> FIFO<T> {
    pub fn new() -> Self {
        FIFO {
            head: None,
            tail: None,
        }
    }

    pub fn push(&mut self, element: T) {
        let new_node = Rc::new(RefCell::new(Node {
            data: element,
            next: None,
        }));

        if let Some(tail) = &self.tail {
            tail.borrow_mut().next = Some(new_node.clone());
            self.tail = Some(new_node);
        } else {
            self.head = Some(new_node.clone());
            self.tail = Some(new_node);
        }
    }

    pub fn peek(&self) -> Option<T>
    where
        T: Clone,
    {
        self.head.as_ref().map(|node| node.borrow().data.clone())
    }

    pub fn pop(&mut self) -> Option<T> {
        let head_node = self.head.take()?;

        self.head = head_node.borrow_mut().next.take();

        if self.head.is_none() {
            self.tail = None;
        }

        let node = Rc::try_unwrap(head_node)
            .map_err(|_| "multiple references exists")
            .unwrap()
            .into_inner();

        Some(node.data)
    }

    pub fn remove(&mut self, predicate: impl Fn(&T) -> bool) -> Option<T>
    where
        T: Clone, // Add this constraint
    {
        // Check if head matches
        if let Some(head) = &self.head {
            if predicate(&head.borrow().data) {
                return self.pop();
            }
        }

        let mut current = self.head.clone();

        while let Some(current_node) = current {
            let next_node = current_node.borrow().next.clone();

            if let Some(next) = &next_node {
                if predicate(&next.borrow().data) {
                    // Clone the data FIRST, before any reference manipulation
                    let data_to_return = next.borrow().data.clone();

                    // Update tail if needed
                    if let Some(tail) = &self.tail {
                        if Rc::ptr_eq(tail, next) {
                            self.tail = Some(current_node.clone());
                        }
                    }

                    // Break the chain
                    let next_next = next.borrow().next.clone();
                    current_node.borrow_mut().next = next_next;

                    // Clear the removed node's next pointer
                    next.borrow_mut().next = None;

                    // Return the data (no need to unwrap Rc)
                    return Some(data_to_return);
                }
            }

            current = next_node;
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Clone)]
    struct Foo {
        id: u32,
    }

    #[test]
    fn test_push_pop_peek_remove() {
        let one = Foo { id: 1 };
        let two = Foo { id: 2 };
        let three = Foo { id: 3 };
        let mut fifo: FIFO<Foo> = FIFO::new();

        // Test basic push/peek
        fifo.push(one.clone());
        assert_eq!(fifo.peek(), Some(one.clone()));

        fifo.push(two.clone());
        fifo.push(three.clone());
        assert_eq!(fifo.peek(), Some(one.clone())); // Still first element

        // Test remove first element, then pop
        assert_eq!(fifo.remove(|f| f.id == 1), Some(one.clone()));
        assert_eq!(fifo.pop(), Some(two.clone()));
        assert_eq!(fifo.pop(), Some(three.clone()));
        assert_eq!(fifo.pop(), None);

        // Test remove middle element
        fifo.push(one.clone());
        fifo.push(two.clone());
        fifo.push(three.clone());
        assert_eq!(fifo.remove(|f| f.id == 2), Some(two.clone()));
        assert_eq!(fifo.pop(), Some(one.clone()));
        assert_eq!(fifo.pop(), Some(three.clone()));
        assert_eq!(fifo.pop(), None);

        // Test remove last element
        fifo.push(one.clone());
        fifo.push(two.clone());
        fifo.push(three.clone());
        assert_eq!(fifo.remove(|f| f.id == 3), Some(three.clone()));
        assert_eq!(fifo.pop(), Some(one.clone()));
        assert_eq!(fifo.pop(), Some(two.clone()));
        assert_eq!(fifo.pop(), None);

        // Test remove then push
        fifo.push(one.clone());
        fifo.push(two.clone());
        assert_eq!(fifo.remove(|f| f.id == 2), Some(two.clone()));
        fifo.push(three.clone());
        assert_eq!(fifo.pop(), Some(one.clone()));
        assert_eq!(fifo.pop(), Some(three.clone()));
        assert_eq!(fifo.pop(), None);
    }

    #[test]
    fn test_empty_operations() {
        let mut fifo: FIFO<i32> = FIFO::new();

        assert_eq!(fifo.pop(), None);
        assert_eq!(fifo.peek(), None);
        assert_eq!(fifo.remove(|x| *x == 5), None);
    }

    #[test]
    fn test_single_element() {
        let mut fifo: FIFO<i32> = FIFO::new();

        fifo.push(42);
        assert_eq!(fifo.peek(), Some(42));
        assert_eq!(fifo.pop(), Some(42));
        assert_eq!(fifo.pop(), None);
    }
}
