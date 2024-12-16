use std::sync::{
    atomic::{fence, AtomicI64, Ordering},
    Arc,
};

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
