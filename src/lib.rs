//! Low latency library for inter-thread communication.
//!
//! Use it when a single thread is not enough and you need multiple threads to communicate
//! with the lowest latency possible.
//!
//! # General Usage
//!
//! The usage can be divided into three stages:
//! 1. **Setup:** Build the Disruptor and setup consumers including any interdependencies.
//! 2. **Publish:** Publish into the Disruptor.
//! 3. **Shutdown:** Stop all consumer threads and drop the Disruptor and the consumer(s).
//!
//! The Disruptor in this library can only be used once. I.e. it cannot be rewound and restarted.
//!
//! It also owns and manages the processing thread(s) for the convenience of the library users.
//!
//! ## Setup
//!
//! When the Disruptor is created, you choose whether publication to the Disruptor will happen from
//! one or multiple threads via [`Producer`] handles. The size of the
//! RingBuffer is also specified and you have to provide a "factory" closure for initializing the
//! events inside the RingBufffer. Then all consumers are added.
//!
//! Use either [`build_single_producer`] or [`build_multi_producer`] to get started.
//!
//! ## Publish
//!
//! Once the Disruptor is built, you have a [`Producer`] "handle" which can be used to publish into
//! the Disruptor. In case of a multi producer Disruptor, the [`Producer`] can be cloned so that
//! each publishing thread has its own handle.
//!
//! ## Shutdown
//!
//! Finally, when there's no more events to publish and the last Producer goes out of scope, all events
//! published are processed and then the processing thread(s) will be stopped and the entire Disruptor
//! will be dropped including consumers.
//!
//! # Examples
//!
//! ### Basic Usage:
//!
//! ```
//! use disruptor::*;
//!
//! // *** Phase SETUP ***
//!
//! // The data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! // Define a factory for populating the ring buffer with events.
//! let factory = || { Event { price: 0.0 }};
//!
//! // Define a closure for processing events. A thread, controlled by the disruptor, will run this
//! // processor closure each time an event is published.
//! let processor = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!     // Process the Event `e` published at `sequence`.
//!     // If `end_of_batch` is false, you can batch up events until it's invoked with
//!     // `end_of_batch` being true.
//! };
//!
//! // Create a Disruptor with a ring buffer of size 8 and use the `BusySpin` wait strategy.
//! let mut producer = build_single_producer(8, factory, BusySpin)
//!     .handle_events_with(processor)
//!     .build();
//!
//! // *** Phase PUBLISH ***
//!
//! // Publish into the Disruptor.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! // *** Phase SHUTDOWN ***
//!
//! // The Producer instance goes out of scope and the Disruptor, the processor (consumer) and
//! // the Producer are dropped.
//! ```
//!
//! ### Batch Publication:
//!
//! ```
//! use disruptor::*;
//!
//! // The data entity on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! let factory = || { Event { price: 0.0 }};
//!
//! let processor = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!     // Processing logic.
//! };
//!
//! let mut producer = build_single_producer(8, factory, BusySpin)
//!     .handle_events_with(processor)
//!     .build();
//!
//! // Batch publish into the Disruptor - 5 events at a time.
//! for i in 0..10 {
//!     producer.batch_publish(5, |iter| {
//!         // `iter` is guaranteed to yield 5 events.
//!         for e in iter {
//!             e.price = i as f64;
//!         }
//!     });
//! }
//! ```
//!
//! ###  Multiple Producers and Multiple, Pinned Consumers:
//! ```
//! use disruptor::*;
//! use std::thread;
//!
//! // The event on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! # #[cfg(miri)] fn main() {}
//! # #[cfg(not(miri))]
//! fn main() {
//!     // Factory closure for initializing events in the Ring Buffer.
//!     let factory = || { Event { price: 0.0 }};
//!
//!     // Closure for processing events.
//!     let h1 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // Processing logic here.
//!     };
//!     let h2 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // Some processing logic here.
//!     };
//!     let h3 = |e: &Event, sequence: Sequence, end_of_batch: bool| {
//!         // More processing logic here.
//!     };
//!
//!     let mut producer1 = disruptor::build_multi_producer(64, factory, BusySpin)
//!         // `h2` handles events concurrently with `h1`.
//!         .pin_at_core(1).handle_events_with(h1)
//!         .pin_at_core(2).handle_events_with(h2)
//!             .and_then()
//!             // `h3` handles events after `h1` and `h2`.
//!             .pin_at_core(3).handle_events_with(h3)
//!         .build();
//!
//!     // Create another producer.
//!     let mut producer2 = producer1.clone();
//!
//!     // Publish into the Disruptor.
//!     thread::scope(|s| {
//!         s.spawn(move || {
//!             for i in 0..10 {
//!                 producer1.publish(|e| {
//!                     e.price = i as f64;
//!                 });
//!             }
//!         });
//!         s.spawn(move || {
//!             for i in 10..20 {
//!                 producer2.publish(|e| {
//!                     e.price = i as f64;
//!                 });
//!             }
//!         });
//!     });
//! }// At this point, the Producers instances go out of scope and when the
//!  // processors are done handling all events then the Disruptor is dropped
//!  // as well.
//! ```
//!
//! ### Adding Custom State That is Neither `Send` Nor `Sync`:
//!
//! ```
//! use std::{cell::RefCell, rc::Rc};
//! use disruptor::*;
//!
//! // The event on the ring buffer.
//! struct Event {
//!     price: f64
//! }
//!
//! // Your custom state.
//! #[derive(Default)]
//! struct State {
//!     data: Rc<RefCell<i32>>
//! }
//!
//! let factory = || { Event { price: 0.0 }};
//! let initial_state = || { State::default() };
//!
//! // Closure for processing events *with* state.
//! let processor = |s: &mut State, e: &Event, _: Sequence, _: bool| {
//!     // Mutate your custom state:
//!     *s.data.borrow_mut() += 1;
//! };
//!
//! let size = 64;
//! let mut producer = disruptor::build_single_producer(size, factory, BusySpin)
//!     .handle_events_and_state_with(processor, initial_state)
//!     .build();
//!
//! // Publish into the Disruptor via the `Producer` handle.
//! for i in 0..10 {
//!     producer.publish(|e| {
//!         e.price = i as f64;
//!     });
//! }
//! ```

#![deny(rustdoc::broken_intra_doc_links)]
#![warn(missing_docs)]

/// The type for Sequence numbers in the Ring Buffer ([`i64`]).
pub type Sequence = i64;

mod affinity;
mod barrier;
pub mod builder;
mod consumer;
mod cursor;
mod producer;
mod receiver;
mod ringbuffer;
pub mod wait_strategies;

pub use crate::builder::{build_multi_producer, build_single_producer, ProcessorSettings};
pub use crate::consumer::{MultiConsumerBarrier, SingleConsumerBarrier};
pub use crate::producer::multi::MultiProducerBarrier;
pub use crate::producer::{multi::MultiProducer, single::SingleProducer};
pub use crate::producer::{MissingFreeSlots, Producer, RingBufferFull};
pub use crate::receiver::{Receiver, TryRecvError};
pub use crate::wait_strategies::{BusySpin, BusySpinWithSpinLoopHint};

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::HashSet;
    use std::rc::Rc;
    use std::sync::atomic::AtomicBool;
    use std::sync::atomic::Ordering::Relaxed;
    use std::sync::{mpsc, Arc};
    use std::thread;

    use producer::MissingFreeSlots;

    use super::*;

    #[derive(Debug)]
    struct Event {
        num: i64,
    }

    fn factory() -> impl Fn() -> Event {
        || Event { num: -1 }
    }

    #[test]
    #[should_panic(expected = "Size must be power of 2.")]
    fn size_not_a_factor_of_2() {
        build_single_producer(3, || 0, BusySpin);
    }

    #[test]
    fn spsc_full_ringbuffer() {
        let (s, r) = mpsc::channel();
        let barrier = Arc::new(AtomicBool::new(true));
        let processor = {
            let barrier = Arc::clone(&barrier);
            move |e: &Event, _, _| {
                while barrier.load(Relaxed) { /* Wait. */ }
                s.send(e.num).expect("Should be able to send.");
            }
        };
        let mut producer = build_single_producer(4, factory(), BusySpinWithSpinLoopHint)
            .handle_events_with(processor)
            .build();

        for i in 0..4 {
            producer.try_publish(|e| e.num = i).expect("Should publish");
        }
        // Now ring buffer is full.
        assert_eq!(
            RingBufferFull,
            producer.try_publish(|e| e.num = 4).err().unwrap()
        );
        // And it stays full.
        assert_eq!(
            RingBufferFull,
            producer.try_publish(|e| e.num = 4).err().unwrap()
        );
        // Until the processor continues reading events.
        barrier.store(false, Relaxed);
        producer.publish(|e| e.num = 4);

        drop(producer);
        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 2, 3, 4]);
    }

    #[test]
    fn mpsc_full_ringbuffer() {
        let (s, r) = mpsc::channel();
        let barrier = Arc::new(AtomicBool::new(true));
        let processor = {
            let barrier = Arc::clone(&barrier);
            move |e: &Event, _, _| {
                while barrier.load(Relaxed) { /* Wait. */ }
                s.send(e.num).expect("Should be able to send.");
            }
        };
        let mut producer1 = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint)
            .handle_events_with(processor)
            .build();

        let mut producer2 = producer1.clone();

        for i in 0..64 {
            producer1
                .try_publish(|e| e.num = i)
                .expect("Should publish");
        }

        // Now ring buffer is full.
        assert_eq!(
            RingBufferFull,
            producer1.try_publish(|e| e.num = 4).err().unwrap()
        );
        // And it is full also as seen from second producer.
        assert_eq!(
            RingBufferFull,
            producer2.try_publish(|e| e.num = 4).err().unwrap()
        );
        // Until the processor continues reading events.
        barrier.store(false, Relaxed);
        producer1.publish(|e| e.num = 64);
        producer2.publish(|e| e.num = 65);

        drop(producer1);
        drop(producer2);
        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, (0..=65).into_iter().collect::<Vec<i64>>());
    }

    #[test]
    fn spsc_insufficient_space_for_batch_publication() {
        let (s, r) = mpsc::channel();
        let barrier = Arc::new(AtomicBool::new(true));
        let processor = {
            let barrier = Arc::clone(&barrier);
            move |e: &Event, _, _| {
                while barrier.load(Relaxed) { /* Wait. */ }
                s.send(e.num).expect("Should be able to send.");
            }
        };
        let mut producer = build_single_producer(4, factory(), BusySpin)
            .handle_events_with(processor)
            .build();

        for i in 0..2 {
            producer.publish(|e| e.num = i);
        }
        assert_eq!(
            MissingFreeSlots(2),
            producer.try_batch_publish(4, |_iter| {}).err().unwrap()
        );
        assert_eq!(
            MissingFreeSlots(100),
            producer.try_batch_publish(102, |_iter| {}).err().unwrap()
        );

        barrier.store(false, Relaxed);
        producer
            .try_batch_publish(2, |iter| {
                for e in iter {
                    e.num = 2;
                }
            })
            .expect("Batch publication should now succeed.");

        drop(producer);
        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 2, 2]);
    }

    #[test]
    fn mpsc_insufficient_space_for_batch_publication() {
        let (s, r) = mpsc::channel();
        let barrier = Arc::new(AtomicBool::new(true));
        let processor = {
            let barrier = Arc::clone(&barrier);
            move |e: &Event, _, _| {
                while barrier.load(Relaxed) { /* Wait. */ }
                s.send(e.num).expect("Should be able to send.");
            }
        };
        let mut producer1 = build_multi_producer(64, factory(), BusySpin)
            .handle_events_with(processor)
            .build();
        let mut producer2 = producer1.clone();

        for i in 0..58 {
            producer1.publish(|e| e.num = i);
        }
        assert_eq!(
            MissingFreeSlots(2),
            producer1.try_batch_publish(8, |_iter| {}).err().unwrap()
        );
        assert_eq!(
            MissingFreeSlots(100),
            producer1.try_batch_publish(106, |_iter| {}).err().unwrap()
        );
        assert_eq!(
            MissingFreeSlots(2),
            producer2.try_batch_publish(8, |_iter| {}).err().unwrap()
        );
        assert_eq!(
            MissingFreeSlots(100),
            producer2.try_batch_publish(106, |_iter| {}).err().unwrap()
        );

        barrier.store(false, Relaxed);
        producer1
            .try_batch_publish(2, |iter| {
                for e in iter {
                    e.num = 2;
                }
            })
            .expect("Batch publication should now succeed.");
        producer2
            .try_batch_publish(2, |iter| {
                for e in iter {
                    e.num = 3;
                }
            })
            .expect("Batch publication should now succeed.");

        drop(producer1);
        drop(producer2);
        let mut result: Vec<_> = r.iter().collect();
        result.sort();
        // Initial events published.
        let mut expected = (0..58).into_iter().collect::<Vec<i64>>();
        // Now add the two successfull batch publications.
        expected.push(2);
        expected.push(2);
        expected.push(3);
        expected.push(3);
        expected.sort();
        assert_eq!(result, expected);
    }

    #[test]
    fn spsc_disruptor() {
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };
        let mut producer = build_single_producer(8, factory(), BusySpin)
            .handle_events_with(processor)
            .build();

        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..10 {
                    producer.publish(|e| e.num = i * i);
                }
            });
        });

        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn spsc_disruptor_with_pinned_and_named_thread() {
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };
        let mut producer = build_single_producer(8, factory(), BusySpin)
            .pin_at_core(0)
            .thread_name("my-processor")
            .handle_events_with(processor)
            .build();

        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..10 {
                    producer.publish(|e| e.num = i * i);
                }
            });
        });

        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
    }

    #[test]
    fn spsc_disruptor_with_batch_publication() {
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };
        let mut producer = build_single_producer(8, factory(), BusySpin)
            .handle_events_with(processor)
            .build();

        let mut i = 0;
        for _ in 0..3 {
            producer.batch_publish(3, |iter| {
                // We are guaranteed that the iterator will yield three elements:
                assert_eq!((3, Some(3)), iter.size_hint());

                // Publish.
                for e in iter {
                    e.num = i * i;
                    i += 1;
                }
            });
        }
        drop(producer);

        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64]);
    }

    #[test]
    fn spsc_disruptor_with_zero_batch_publication() {
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };
        let mut producer = build_single_producer(8, factory(), BusySpin)
            .handle_events_with(processor)
            .build();

        producer.batch_publish(0, |iter| {
            for e in iter {
                e.num = 1;
            }
        });
        drop(producer);

        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, []);
    }

    #[test]
    fn spsc_disruptor_with_state() {
        let (s, r) = mpsc::channel();
        let initialize_state = || Rc::new(RefCell::new(0));
        let processor = move |state: &mut Rc<RefCell<i64>>, e: &Event, _, _| {
            let mut ref_cell = state.borrow_mut();
            *ref_cell += e.num;
            s.send(*ref_cell).expect("Should be able to send.");
        };
        let mut producer = build_single_producer(8, factory(), BusySpin)
            .handle_events_and_state_with(processor, initialize_state)
            .build();

        for i in 0..10 {
            producer.publish(|e| e.num = i);
        }

        drop(producer);
        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 3, 6, 10, 15, 21, 28, 36, 45]);
    }

    #[test]
    fn pipeline_of_two_spsc_disruptors() {
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };

        // Last Disruptor.
        let mut producer = build_single_producer(8, factory(), BusySpin)
            .handle_events_with(processor)
            .build();
        let processor = move |e: &Event, _, _| {
            producer.publish(|e2| e2.num = e.num);
        };

        // First Disruptor.
        let mut producer = build_single_producer(8, factory(), BusySpin)
            .handle_events_with(processor)
            .build();
        for i in 0..10 {
            producer.publish(|e| e.num = i * i);
        }

        drop(producer);
        let result: Vec<_> = r.iter().collect();
        assert_eq!(result, [0, 1, 4, 9, 16, 25, 36, 49, 64, 81]);
    }

    #[test]
    fn multi_publisher_disruptor() {
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };

        let mut producer1 = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint)
            .handle_events_with(processor)
            .build();
        let mut producer2 = producer1.clone();

        let num_items = 100;

        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..num_items / 2 {
                    producer1.publish(|e| e.num = i);
                }
            });

            s.spawn(move || {
                for i in (num_items / 2)..num_items {
                    producer2.publish(|e| e.num = i);
                }
            });
        });

        let mut result: Vec<_> = r.iter().collect();
        result.sort();

        let expected: Vec<i64> = (0..num_items).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn multi_publisher_disruptor_with_receiver() {
        let (test_sender, r) = mpsc::channel();

        let producer1 = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint);
        let (producer1, mut receiver) = producer1.handle_events_with_receiver();
        let mut producer1 = producer1.build();

        let mut producer2 = producer1.clone();

        let num_items = 100;

        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..num_items / 2 {
                    producer1.publish(|e| e.num = i);
                }
            });

            s.spawn(move || {
                for i in (num_items / 2)..num_items {
                    producer2.publish(|e| e.num = i);
                }
            });

            s.spawn(move || loop {
                match receiver.try_recv() {
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => continue,
                    Ok(e) => {
                        test_sender.send(e.num).expect("Should be able to send.");
                    }
                }
            });
        });

        let mut result: Vec<_> = r.iter().collect();
        result.sort();

        let expected: Vec<i64> = (0..num_items).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn dropped_receiver_should_not_block_others() {
        let queue_size: usize = 64;

        let b = build_multi_producer(queue_size, factory(), BusySpinWithSpinLoopHint);
        let (b, mut rx) = b.handle_events_with_receiver();
        let (b, dropped_receiver) = b.handle_events_with_receiver();

        let mut tx = b.build();
        
        drop(dropped_receiver);

        tx.batch_publish(queue_size, |_| {}); // full queue
        assert!(rx.try_recv().is_ok()); // full minus one element
        assert!(tx.try_publish(|_| {}).is_ok()); // should be able send one new element if `dropped_receiver` do not block it
    }

    #[test]
    fn multi_publisher_disruptor_with_three_receivers() {
        let (test_sender_1, r1) = mpsc::channel();
        let (test_sender_2, r2) = mpsc::channel();
        let (test_sender_3, r3) = mpsc::channel();

        let producer1 = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint);

        let (producer1, mut receiver1) = producer1.handle_events_with_receiver();
        let (producer1, mut receiver2) = producer1.handle_events_with_receiver();
        let (producer1, mut receiver3) = producer1.handle_events_with_receiver();

        let mut producer1 = producer1.build();
        let mut producer2 = producer1.clone();

        let num_items = 250000;

        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..num_items / 2 {
                    producer1.publish(|e| e.num = i);
                }
            });

            s.spawn(move || {
                for i in (num_items / 2)..num_items {
                    producer2.publish(|e| e.num = i);
                }
            });

            s.spawn(move || loop {
                match receiver1.try_recv() {
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => continue,
                    Ok(e) => {
                        test_sender_1.send(e.num).expect("Should be able to send.");
                    }
                }
            });

            s.spawn(move || loop {
                match receiver2.try_recv() {
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => continue,
                    Ok(e) => {
                        test_sender_2.send(e.num).expect("Should be able to send.");
                    }
                }
            });

            s.spawn(move || loop {
                match receiver3.try_recv() {
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => continue,
                    Ok(e) => {
                        test_sender_3.send(e.num).expect("Should be able to send.");
                    }
                }
            });
        });

        for r in [r1, r2, r3] {
            let mut result: Vec<_> = r.iter().collect();
            result.sort();

            let expected: Vec<i64> = (0..num_items).collect();
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn multi_publisher_disruptor_receiver_drain() {
        let (test_sender, r) = mpsc::channel();

        let producer1 = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint);
        let (producer1, mut receiver) = producer1.handle_events_with_receiver();
        let mut producer1 = producer1.build();

        let mut producer2 = producer1.clone();

        let num_items = 250000;

        thread::scope(|s| {
            s.spawn(move || {
                for i in 0..num_items / 2 {
                    producer1.publish(|e| e.num = i);
                }
            });

            s.spawn(move || {
                for i in (num_items / 2)..num_items {
                    producer2.publish(|e| e.num = i);
                }
            });

            s.spawn(move || loop {
                match receiver.drain() {
                    Err(TryRecvError::Disconnected) => break,
                    Err(TryRecvError::Empty) => continue,
                    Ok(iter) => {
                        for e in iter {
                            test_sender.send(e.num).expect("Should be able to send.");
                        }
                    }
                }
            });
        });

        let mut result: Vec<_> = r.iter().collect();
        result.sort();

        let expected: Vec<i64> = (0..num_items).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn drain_should_take_all_events_at_moment_of_call() {
        let b = build_multi_producer(64, factory(), BusySpinWithSpinLoopHint);
        let (b, mut rx) = b.handle_events_with_receiver();
        let mut tx = b.build();

        tx.publish(|e| *e = Event { num: 1 });
        tx.publish(|e| *e = Event { num: 2 });
        tx.publish(|e| *e = Event { num: 3 });

        let mut iter = rx.drain().unwrap();
        assert_eq!(iter.next().map(|e| e.num), Some(1));
        assert_eq!(iter.next().map(|e| e.num), Some(2));

        tx.publish(|e| *e = Event { num: 4 });
        drop(iter);

        assert_eq!(rx.drain().unwrap().next().map(|e| e.num), Some(4));
    }

    #[test]
    fn early_drop_of_drain_should_fully_cleanup_queue() {
        let queue_size = 64;
        let b = build_multi_producer(queue_size, factory(), BusySpinWithSpinLoopHint);
        let (b, mut rx) = b.handle_events_with_receiver();
        let mut tx = b.build();

        tx.publish(|e| *e = Event { num: 1 });
        tx.publish(|e| *e = Event { num: 2 });
        tx.publish(|e| *e = Event { num: 3 });
        let iter = rx.drain();
        drop(iter);

        assert!(tx.try_batch_publish(queue_size, |_| {}).is_ok());
    }

    #[test]
    fn drain_returns_correct_error_on_empty() {
        let queue_size = 64;
        let b = build_multi_producer(queue_size, factory(), BusySpinWithSpinLoopHint);
        let (b, mut rx) = b.handle_events_with_receiver();
        let _tx = b.build();

        assert!(matches!(rx.drain(), Err(TryRecvError::Empty)));
    }

    #[test]
    fn drain_returns_correct_error_on_disconnect() {
        let queue_size = 64;
        let b = build_multi_producer(queue_size, factory(), BusySpinWithSpinLoopHint);
        let (b, mut rx) = b.handle_events_with_receiver();
        let tx = b.build();

        drop(tx);

        assert!(matches!(rx.drain(), Err(TryRecvError::Disconnected)));
    }

    #[test]
    fn multi_publisher_disruptor_with_batch_publication() {
        let (s, r) = mpsc::channel();
        let processor = move |e: &Event, _, _| {
            s.send(e.num).expect("Should be able to send.");
        };

        let mut producer1 = build_multi_producer(64, factory(), BusySpin)
            .handle_events_with(processor)
            .build();
        let mut producer2 = producer1.clone();

        let num_items = 100_i64;
        let batch_size = 5;

        thread::scope(|s| {
            s.spawn(move || {
                for b in 0..(num_items / 2) / batch_size {
                    producer1.batch_publish(batch_size as usize, |iter| {
                        for (i, e) in iter.enumerate() {
                            e.num = batch_size * b + i as i64;
                        }
                    });
                }
            });

            s.spawn(move || {
                for i in (num_items / 2)..num_items {
                    producer2.publish(|e| e.num = i as i64);
                }
            });
        });

        let mut result: Vec<_> = r.iter().collect();
        result.sort();

        let expected: Vec<i64> = (0..num_items).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn spmc_with_concurrent_consumers() {
        let (s, r) = mpsc::channel();

        let processor1 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 1).unwrap();
            }
        };
        let processor2 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 2).unwrap();
            }
        };
        let processor3 = {
            move |e: &Event, _, _| {
                s.send(e.num + 3).unwrap();
            }
        };

        let builder = build_single_producer(8, factory(), BusySpin);
        let mut producer = builder
            .handle_events_with(processor1)
            .handle_events_with(processor2)
            .handle_events_with(processor3)
            .build();

        producer.publish(|e| {
            e.num = 0;
        });
        drop(producer);

        let result: HashSet<i64> = r.iter().collect();
        let expected = HashSet::from([1, 2, 3]);
        assert_eq!(expected, result);
    }

    #[test]
    fn spmc_with_dependent_consumers_and_some_with_state() {
        let (s, r) = mpsc::channel();

        let processor1 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 0).unwrap();
            }
        };
        let processor2 = {
            let s = s.clone();
            move |state: &mut RefCell<i64>, e: &Event, _, _| {
                *state.borrow_mut() += e.num;
                s.send(*state.borrow() + 10).unwrap();
            }
        };
        let processor3 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 2).unwrap();
            }
        };
        let processor4 = {
            let s = s.clone();
            move |state: &mut RefCell<i64>, e: &Event, _, _| {
                *state.borrow_mut() += e.num;
                s.send(*state.borrow() + 20).unwrap();
            }
        };
        let processor5 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 4).unwrap();
            }
        };
        let processor6 = {
            move |state: &mut RefCell<i64>, e: &Event, _, _| {
                *state.borrow_mut() += e.num;
                s.send(*state.borrow() + 30).unwrap();
            }
        };

        let builder = build_single_producer(8, factory(), BusySpin);

        let mut producer = builder
            .handle_events_with(processor1)
            .handle_events_and_state_with(processor2, || RefCell::new(0))
            .and_then()
            .handle_events_with(processor3)
            .handle_events_and_state_with(processor4, || RefCell::new(0))
            .and_then()
            .handle_events_with(processor5)
            .handle_events_and_state_with(processor6, || RefCell::new(0))
            .build();

        producer.publish(|e| {
            e.num = 1;
        });
        drop(producer);

        let mut result: Vec<i64> = r.iter().collect();
        result.sort();
        assert_eq!(vec![1, 3, 5, 11, 21, 31], result);
    }

    #[test]
    fn mpmc_with_dependent_consumers_and_some_with_state() {
        let (s, r) = mpsc::channel();

        let processor1 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 0).unwrap();
            }
        };
        let processor2 = {
            let s = s.clone();
            move |state: &mut RefCell<i64>, e: &Event, _, _| {
                *state.borrow_mut() += e.num;
                s.send(*state.borrow() + 10).unwrap();
            }
        };
        let processor3 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 2).unwrap();
            }
        };
        let processor4 = {
            let s = s.clone();
            move |state: &mut RefCell<i64>, e: &Event, _, _| {
                *state.borrow_mut() += e.num;
                s.send(*state.borrow() + 20).unwrap();
            }
        };
        let processor5 = {
            let s = s.clone();
            move |e: &Event, _, _| {
                s.send(e.num + 4).unwrap();
            }
        };
        let processor6 = {
            move |state: &mut RefCell<i64>, e: &Event, _, _| {
                *state.borrow_mut() += e.num;
                s.send(*state.borrow() + 30).unwrap();
            }
        };

        let builder = build_multi_producer(64, factory(), BusySpin);
        let mut producer1 = builder
            .handle_events_with(processor1)
            .handle_events_and_state_with(processor2, || RefCell::new(0))
            .and_then()
            .handle_events_with(processor3)
            .handle_events_and_state_with(processor4, || RefCell::new(0))
            .and_then()
            .handle_events_with(processor5)
            .handle_events_and_state_with(processor6, || RefCell::new(0))
            .build();

        let mut producer2 = producer1.clone();

        thread::scope(|s| {
            s.spawn(move || {
                producer1.publish(|e| e.num = 1);
            });

            s.spawn(move || {
                producer2.publish(|e| e.num = 1);
            });
        });

        let mut result: Vec<i64> = r.iter().collect();
        result.sort();
        assert_eq!(vec![1, 1, 3, 3, 5, 5, 11, 12, 21, 22, 31, 32], result);
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn stress_test() {
        #[derive(Debug)]
        struct StressEvent {
            i: Sequence,
            a: i64,
            b: i64,
            s: String,
        }

        let (s_seq, r_seq) = mpsc::channel();
        let (s_error, r_error) = mpsc::channel();
        let num_events = 250_000;
        let producers = 4;
        let consumers = 3;

        let mut processors: Vec<_> = (0..consumers)
            .into_iter()
            .map(|pid| {
                let s_error = s_error.clone();
                let s_seq = s_seq.clone();
                let mut prev_seq = -1;
                Some(move |e: &StressEvent, sequence, _| {
                    if e.a != e.i - 5
                        || e.b != e.i + 7
                        || e.s != format!("Blackbriar {}", e.i).to_string()
                        || sequence != prev_seq + 1
                    {
                        s_error.send(1).expect("Should send.");
                    } else {
                        prev_seq = sequence;
                        let sequence_seen_by_pid = sequence * consumers + pid;
                        s_seq.send(sequence_seen_by_pid).expect("Should send.");
                    }
                })
            })
            .collect();

        // Drop unused Senders.
        drop(s_seq);
        drop(s_error);

        let factory = || StressEvent {
            i: -1,
            a: -1,
            b: -1,
            s: "".to_string(),
        };

        let producer = build_multi_producer(1 << 16, factory, BusySpin)
            .handle_events_with(processors[0].take().unwrap())
            .handle_events_with(processors[1].take().unwrap())
            .handle_events_with(processors[2].take().unwrap())
            .build();

        thread::scope(|s| {
            for _ in 0..producers {
                let mut producer = producer.clone();
                s.spawn(move || {
                    for i in 0..num_events {
                        producer.publish(|e| {
                            e.i = i;
                            e.a = i - 5;
                            e.b = i + 7;
                            e.s = format!("Blackbriar {}", i).to_string();
                        });
                    }
                });
            }
            drop(producer); // Drop excess producer not used.
        });

        let expected_sequence_reads = consumers * num_events * producers;
        let errors: Vec<_> = r_error.iter().collect();
        let mut seen_sequences: Vec<_> = r_seq.iter().collect();

        assert!(errors.is_empty());
        assert_eq!(expected_sequence_reads as usize, seen_sequences.len());
        // Assert that each consumer saw each sequence number.
        seen_sequences.sort();
        for seq_seen_by_pid in 0..expected_sequence_reads {
            assert_eq!(seq_seen_by_pid, seen_sequences[seq_seen_by_pid as usize]);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn stress_test_try_recv() {
        #[derive(Debug)]
        struct StressEvent {
            i: Sequence,
            a: i64,
            b: i64,
            s: String,
        }

        let (s_seq, r_seq) = mpsc::channel();
        let (s_error, r_error) = mpsc::channel();
        let num_events = 250_000;
        let producers = 4;
        let consumers = 3;

        let factory = || StressEvent {
            i: -1,
            a: -1,
            b: -1,
            s: "".to_string(),
        };

        let producer = build_multi_producer(1 << 16, factory, BusySpin);
        let (producer, tx1) = producer.handle_events_with_receiver();
        let (producer, tx2) = producer.handle_events_with_receiver();
        let (producer, tx3) = producer.handle_events_with_receiver();
        let producer = producer.build();

        thread::scope(move |s| {
            for _ in 0..producers {
                let mut producer = producer.clone();
                s.spawn(move || {
                    for i in 0..num_events {
                        producer.publish(|e| {
                            e.i = i;
                            e.a = i - 5;
                            e.b = i + 7;
                            e.s = format!("Blackbriar {}", i).to_string();
                        });
                    }
                });
            }
            drop(producer); // Drop excess producer not used.

            for (pid, mut rx) in [tx1, tx2, tx3].into_iter().enumerate() {
                let pid = pid as i64;

                let s_error = s_error.clone();
                let s_seq = s_seq.clone();

                s.spawn(move || {
                    let s_error = s_error.clone();
                    let s_seq = s_seq.clone();
                    std::thread::spawn(move || {
                        let mut seq = 0;
                        loop {
                            match rx.try_recv() {
                                Err(TryRecvError::Disconnected) => break,
                                Err(TryRecvError::Empty) => continue,
                                Ok(e) => {
                                    if e.a != e.i - 5
                                        || e.b != e.i + 7
                                        || e.s != format!("Blackbriar {}", e.i)
                                    {
                                        s_error.send(1).expect("Should send.");
                                    } else {
                                        let sequence_seen_by_pid = seq * consumers + pid;
                                        s_seq.send(sequence_seen_by_pid).expect("Should send.");
                                    }

                                    seq += 1;
                                }
                            }
                        }
                    });
                });
            }

            // Drop unused Senders.
            drop(s_seq);
            drop(s_error);
        });

        let expected_sequence_reads = consumers * num_events * producers;
        let errors: Vec<_> = r_error.iter().collect();
        let mut seen_sequences: Vec<_> = r_seq.iter().collect();

        assert!(errors.is_empty());
        assert_eq!(expected_sequence_reads as usize, seen_sequences.len());
        // Assert that each consumer saw each sequence number.
        seen_sequences.sort();
        for seq_seen_by_pid in 0..expected_sequence_reads {
            assert_eq!(seq_seen_by_pid, seen_sequences[seq_seen_by_pid as usize]);
        }
    }

    #[cfg_attr(miri, ignore)]
    #[test]
    fn stress_test_drain() {
        #[derive(Debug)]
        struct StressEvent {
            i: Sequence,
            a: i64,
            b: i64,
            s: String,
        }

        let (s_seq, r_seq) = mpsc::channel();
        let (s_error, r_error) = mpsc::channel();
        let num_events = 250_000;
        let producers = 4;
        let consumers = 3;

        let factory = || StressEvent {
            i: -1,
            a: -1,
            b: -1,
            s: "".to_string(),
        };

        let producer = build_multi_producer(1 << 16, factory, BusySpin);
        let (producer, tx1) = producer.handle_events_with_receiver();
        let (producer, tx2) = producer.handle_events_with_receiver();
        let (producer, tx3) = producer.handle_events_with_receiver();
        let producer = producer.build();

        thread::scope(move |s| {
            for _ in 0..producers {
                let mut producer = producer.clone();
                s.spawn(move || {
                    for i in 0..num_events {
                        producer.publish(|e| {
                            e.i = i;
                            e.a = i - 5;
                            e.b = i + 7;
                            e.s = format!("Blackbriar {}", i).to_string();
                        });
                    }
                });
            }
            drop(producer); // Drop excess producer not used.

            for (pid, mut rx) in [tx1, tx2, tx3].into_iter().enumerate() {
                let pid = pid as i64;

                let s_error = s_error.clone();
                let s_seq = s_seq.clone();

                s.spawn(move || {
                    let s_error = s_error.clone();
                    let s_seq = s_seq.clone();
                    std::thread::spawn(move || {
                        let mut seq = 0;
                        loop {
                            match rx.drain() {
                                Err(TryRecvError::Disconnected) => break,
                                Err(TryRecvError::Empty) => continue,
                                Ok(iter) => {
                                    for e in iter {
                                        if e.a != e.i - 5
                                            || e.b != e.i + 7
                                            || e.s != format!("Blackbriar {}", e.i)
                                        {
                                            s_error.send(1).expect("Should send.");
                                        } else {
                                            let sequence_seen_by_pid = seq * consumers + pid;
                                            s_seq.send(sequence_seen_by_pid).expect("Should send.");
                                        }

                                        seq += 1;
                                    }
                                }
                            }
                        }
                    });
                });
            }

            // Drop unused Senders.
            drop(s_seq);
            drop(s_error);
        });

        let expected_sequence_reads = consumers * num_events * producers;
        let errors: Vec<_> = r_error.iter().collect();
        let mut seen_sequences: Vec<_> = r_seq.iter().collect();

        assert!(errors.is_empty());
        assert_eq!(expected_sequence_reads as usize, seen_sequences.len());
        // Assert that each consumer saw each sequence number.
        seen_sequences.sort();
        for seq_seen_by_pid in 0..expected_sequence_reads {
            assert_eq!(seq_seen_by_pid, seen_sequences[seq_seen_by_pid as usize]);
        }
    }
}
