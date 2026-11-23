use std::sync::Arc;

use crate::{
    AppendableType, chunk::FrozenChunk, shared::InvalidDowncast, vector::VectorIter,
    writer_buffer::WriteBufferSnapshot,
};

pub(crate) struct Snapshot {
    head: Option<Arc<FrozenChunk>>,
    tail: Option<WriteBufferSnapshot>,
}

impl Snapshot {
    pub(crate) const fn new(head: Option<Arc<FrozenChunk>>, tail: WriteBufferSnapshot) -> Self {
        Self {
            head,
            tail: Some(tail),
        }
    }
}

impl Iterator for Snapshot {
    type Item = Node;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(head) = self.head.take() {
            self.head = head.next().cloned();
            Some(Node::Next(head))
        } else {
            self.tail.take().map(Node::Terminal)
        }
    }
}

// This is purely needed so that we can move each chunk to its own thread and have it processed in parallel
pub enum Node {
    Next(Arc<FrozenChunk>),
    Terminal(WriteBufferSnapshot),
}

impl Node {
    pub fn vectors<V: AppendableType>(
        &self,
    ) -> Result<Box<dyn Iterator<Item = VectorIter<'_, V::ArrayType>> + '_>, InvalidDowncast> {
        match self {
            Node::Next(forzen_chunk) => forzen_chunk.view().map_or(Err(InvalidDowncast), |view| {
                Ok(Box::new(std::iter::once(view.vectors())))
            }),
            Node::Terminal(writer_chunk) => writer_chunk.vectors().map(|vectors| {
                Box::new(vectors) as Box<dyn Iterator<Item = VectorIter<'_, V::ArrayType>>>
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::OnceCell, ops::Range, thread::JoinHandle, time::Instant};

    use crate::{
        AppendableType, column::Column, shared::InvalidDowncast, snapshot::Node, units::{LogicalOffset, VersionID}, vector::Validity,
    };

    fn column<V: AppendableType>() -> Column {
        Column::new(Box::new(|| VersionID::new(0)), V::builder())
    }

    #[test]
    fn correct_value_summing() {
        let mut column: Column = column::<u64>();
        const N: u64 = 10_000_000;
        let range: Range<u64> = 1..N;
        if let Err(InvalidDowncast) = column.write::<u64>(range) {
            panic!("Should not fail");
        }

        let mut cum_sum: u64 = 0;
        for node in column.snapshot() {
            // This is the thread boundary. Every node is sendable
            for vector in node.vectors::<u64>().unwrap() {
                for window in vector {
                    let sum: u64 = window.with(|window, validity| {
                        let mut local_sum: u64 = 0;

                        for (index, value) in window.iter().enumerate() {
                            if validity.is_valid(LogicalOffset::new(index as u64)) {
                                local_sum += value
                            }
                        }

                        local_sum
                    });
                    cum_sum += sum;
                }
            }
        }

        let n: u64 = N - 1;
        assert_eq!(cum_sum, n * (n + 1) / 2);
    }

    #[test]
    fn concurrent_aggregation() {
        let mut column: Column = column::<u64>();
        const N: u64 = u32::MAX as u64;

        println!("Beginning to write");

        let range: Range<u64> = 1..N;
        if let Err(InvalidDowncast) = column.write::<u64>(range) {
            panic!("Should not fail");
        }

        println!("Writing is done");

        use rayon::prelude::*;

        let sum: u64 = column
            .snapshot()
            .par_bridge()
            .map(|node| {
                node.vectors::<u64>()
                    .unwrap()
                    .flat_map(|vector| vector)
                    .map(|window| {
                        window.with(|window, validity| {
                            window
                                .iter()
                                .enumerate()
                                .filter(|(index, _)| {
                                    validity.is_valid(LogicalOffset::new(*index as u64))
                                })
                                .map(|(_, value)| *value)
                                .sum::<u64>()
                        })
                    })
                    .sum::<u64>()
            })
            .sum();

        let n: u64 = N - 1;
        assert_eq!(sum, n * (n + 1) / 2);
    }

    #[test]
    fn concurrent_aggregation_is_consistent() {
        use rayon::prelude::*;

        const MAX_N: u64 = u32::MAX as u64;
        const ITERATIONS: usize = 1000;

        fn test_n(n: u64, iterations: usize) -> bool {
            let mut column: Column = column::<u64>();

            let range = std::iter::repeat_n(1u64, n as usize);

            if let Err(InvalidDowncast) = column.write::<u64>(range) {
                panic!("Write failed for N={n}");
            }

            // Verify initial contents.
            for node in column.snapshot() {
                for vectors in node.vectors::<u64>().unwrap() {
                    for vector in vectors {
                        assert!(
                            vector.with(|window, _| window.iter().all(|v| *v == 1)),
                            "Initial data corruption at N={n}"
                        );
                    }
                }
            }

            for iteration in 0..iterations {
                // Collect the snapshot sequentially first.
                let vectors: Vec<_> = column.snapshot().collect();

                // Process the collected vectors in parallel.
                let sum: u64 = vectors
                    .into_par_iter()
                    .map(|node| {
                        node.vectors::<u64>()
                            .unwrap()
                            .flat_map(|vectors| {
                                vectors.map(|vector| {
                                    vector.with(|window, _| {
                                        let mut local_sum = 0u64;

                                        for (local_index, value) in window.iter().enumerate() {
                                            assert_eq!(
                                                *value, 1,
                                                "CORRUPTION: N={}, iteration={}, local_index={}",
                                                n, iteration, local_index,
                                            );

                                            local_sum += *value;
                                        }

                                        local_sum
                                    })
                                })
                            })
                            .sum::<u64>()
                    })
                    .sum();

                assert_eq!(sum, n, "INVALID SUM: N={}, iteration={}", n, iteration,);
            }

            true
        }

        let mut n = 1u64;

        loop {
            println!("Testing N={n}");

            if !std::panic::catch_unwind(|| test_n(n, ITERATIONS)).unwrap_or(false) {
                panic!("Failure at N={n}");
            }

            if n >= MAX_N {
                break;
            }

            n = (n * 2).min(MAX_N);
        }
    }

    #[test]
    fn custom_concurrent_aggregation_is_consistent() {
        const MAX_N: u64 = u32::MAX as u64;
        const ITERATIONS: usize = 1000;

        fn test_n(n: u64, iterations: usize) -> bool {
            let mut column: Column = column::<u64>();

            let range = std::iter::repeat_n(1u64, n as usize);

            if let Err(InvalidDowncast) = column.write::<u64>(range) {
                panic!("Write failed for N={n}");
            }

            // Verify initial contents.
            for node in column.snapshot() {
                for vectors in node.vectors::<u64>().unwrap() {
                    for vector in vectors {
                        assert!(
                            vector.with(|window, _| window.iter().all(|v| *v == 1)),
                            "Initial data corruption at N={n}"
                        );
                    }
                }
            }

            for iteration in 0..iterations {
                // Collect the snapshot sequentially first.
                let nodes: Vec<Node> = column.snapshot().collect();
                let mut partial_sums: Vec<JoinHandle<u64>> = Vec::with_capacity(nodes.len());

                for node in nodes {
                    // Be aware this nodes vector can have thousands of chunks
                    let partial_sum: std::thread::JoinHandle<u64> = std::thread::spawn(move || {
                        let mut sum: u64 = 0;
                        for vectors in node.vectors::<u64>() {
                            for vector in vectors {
                                for window in vector {
                                    sum += window.with(|window, _| {
                                        window.iter().sum::<u64>()
                                    });
                                }
                            }
                        }

                        sum
                    });

                    partial_sums.push(partial_sum);
                }

                let mut sum: u64 = 0;

                for partial_sum in partial_sums {
                    sum += partial_sum.join().expect("Joining failed");
                }

                assert_eq!(sum, n, "Failed at {iteration}");
            }

            true
        }

        let mut n = u32::MAX as u64;

        loop {
            println!("Testing N={n}");

            if !std::panic::catch_unwind(|| test_n(n, ITERATIONS)).unwrap_or(false) {
                panic!("Failure at N={n}");
            }

            if n >= MAX_N {
                break;
            }

            n = (n * 2).min(MAX_N);
        }
    }
}
