use std::{marker::PhantomData, sync::{
    atomic::{fence, AtomicI64, Ordering},
    Arc,
}};

use crossbeam_utils::CachePadded;

use crate::{barrier::Barrier, cursor::Cursor, ringbuffer::RingBuffer, Sequence};

/// An error returned from the [`try_recv`] method.
#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub enum TryRecvError {
    /// A message could not be received because the channel is empty.
    Empty,

    /// The message could not be received because the channel is empty and disconnected.
    Disconnected,
}

pub struct Receiver<E, B> {
    sequence: Sequence,
    available: Option<Sequence>,
    ring_buffer: Arc<RingBuffer<E>>,
    shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
    consumer_cursor: Arc<Cursor>,
    barrier: Arc<B>,
}

pub struct Drain<'a, E> {
    sequence: i64,
    available: i64,
    ring_buffer: Arc<RingBuffer<E>>,
    consumer_cursor: Arc<Cursor>,
    _phantom: PhantomData<&'a E>,
}

impl<'a, E> Drain<'a, E> {
    pub(crate) fn new_empty(
        ring_buffer: Arc<RingBuffer<E>>,
        consumer_cursor: Arc<Cursor>,
    ) -> Self {
        Self {
            sequence: 0,
            available: -1,
            ring_buffer,
            consumer_cursor,
            _phantom: PhantomData,
        }
    }
}

impl<'a, E> Iterator for Drain<'a, E> where E: 'a {
    type Item = &'a E;

    fn next(&mut self) -> Option<Self::Item> {
        if self.available < self.sequence {
            return None
        }

        // SAFETY: Now, we have (shared) read access to the event at `sequence`.
        let event_ptr = self.ring_buffer.get(self.sequence);
        let event = unsafe { &*event_ptr };
        // Signal to producers or later consumers that we're done processing `sequence`.
        self.consumer_cursor.store(self.sequence);
        // Update next sequence to read.
        self.sequence += 1;

        Some(event)
    }
}

impl<'a, E> Drop for Drain<'a, E> where E: 'a {
    fn drop(&mut self) {
        if self.sequence < self.available {
            // Receiver::sequence was set to self.available already
            self.consumer_cursor.store(self.available);
        }
    }
}

impl<E, B> Receiver<E, B>
where
    E: 'static + Send + Sync,
    B: 'static + Barrier + Send + Sync,
{
    pub(crate) fn new(
        ring_buffer: Arc<RingBuffer<E>>,
        shutdown_at_sequence: Arc<CachePadded<AtomicI64>>,
        consumer_cursor: Arc<Cursor>,
        barrier: Arc<B>,
    ) -> Self {
        Self {
            sequence: 0,
            available: None,
            ring_buffer,
            shutdown_at_sequence,
            consumer_cursor,
            barrier,
        }
    }

    pub fn try_recv(&mut self) -> Result<&E, TryRecvError> {
        if let Some(available) = self.available {
            let end_of_batch = available == self.sequence;
            let event = self.get_next();

            if end_of_batch {
                self.available = None;
            }

            return Ok(event);
        }

        let available = self.barrier.get_after(self.sequence);
        let closed = self.shutdown_at_sequence.load(Ordering::Relaxed) == self.sequence;
        fence(Ordering::Acquire);

        if closed {
            return Err(TryRecvError::Disconnected);
        }

        if available < self.sequence {
            return Err(TryRecvError::Empty);
        }

        let end_of_batch = available == self.sequence;
        let event = self.get_next();

        if !end_of_batch {
            self.available = Some(available);
        }

        Ok(event)
    }

    pub fn drain(&mut self) -> Drain<'_, E> {
        let available = if let Some(available) = self.available.take() {
            available
        } else {
            let available = self.barrier.get_after(self.sequence);
            let closed = self.shutdown_at_sequence.load(Ordering::Relaxed) == self.sequence;
            fence(Ordering::Acquire);

            if closed || (available < self.sequence) {
                return Drain::new_empty(self.ring_buffer.clone(), self.consumer_cursor.clone());
            }

            available
        };

        // warn! `Drain` should update `consumer_cursor` to `available` in case of early drop
        let sequence = self.sequence;
        self.sequence = available + 1; // ready to read next message after drain

        Drain {
            available,
            sequence,
            ring_buffer: self.ring_buffer.clone(),
            // if we update `consumer_cursor` beforehand then `ring_buffer` could be overwritten in time of iterating
            consumer_cursor: self.consumer_cursor.clone(),
            _phantom: PhantomData,
        }
    }

    fn get_next<'a>(&mut self) -> &'a E {
        // SAFETY: Now, we have (shared) read access to the event at `sequence`.
        let event_ptr = self.ring_buffer.get(self.sequence);
        let event = unsafe { &*event_ptr };
        // Signal to producers or later consumers that we're done processing `sequence`.
        self.consumer_cursor.store(self.sequence);
        // Update next sequence to read.
        self.sequence += 1;

        event
    }
}
