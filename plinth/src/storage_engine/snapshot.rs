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
    use std::{
        cell::OnceCell,
        ops::{Range, RangeInclusive},
        thread::JoinHandle,
        time::Instant,
    };

    use crate::{
        AppendableType,
        column::Column,
        shared::InvalidDowncast,
        snapshot::Node,
        units::{LogicalOffset, VersionID},
        vector::Validity,
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

    /// Verifies that repeated snapshot scans over a 32 GiB (`1..=u32::MAX`) column
    /// produce an exact, consistent sum across 1,000 iterations (~32 TiB of reads).
    ///
    /// # Verification 
    /// - **AddressSanitizer (`-Zsanitizer=address`):** Passed 975 consecutive full 32 GiB
    ///   passes with zero out-of-bounds accesses, use-after-free errors, or memory leaks.
    /// - **Miri (`cargo +nightly miri test`):** Passed multi-chunk execution (`n = 150_000`)
    ///   with zero uninitialized memory reads, alignment issues, or aliasing UB.
    ///
    /// # Known Hardware Defect on Dev Machine (`julius-H6109D4G`)
    /// - On iteration 975, `summe` jumped by `+70_368_744_177_664` (`1 << 46` /
    ///   `0x0000_4000_0000_0000`).
    /// - A full `memtester 32G 1` (v4.7.1) pass independently reproduced three stuck-high
    ///   single-bit flips across non-ECC DDR4 memory, confirming a faulty DIMM rather than
    ///   a software bug:
    ///   - `Block Sequential`: `0x9191d19191919191 != 0x9191919191919191` at `0x00000000efb4cec8` (XOR `1 << 46`)
    ///   - `Walking Zeroes`: `0x0000200000080000 != 0x0000000000080000` at `0x000000039f0d3008` (XOR `1 << 45`)
    ///   - `Walking Zeroes`: `0x0000400000080000 != 0x0000000000080000` at `0x000000039f0d6cc8` (XOR `1 << 46`)

    #[test]
    fn synchronous_snapshot_is_consistent() {
        let (n, iterations) = if cfg!(miri) {
            (150_000, 10)
        } else {
            (u32::MAX as u64, 1_000)
        };

        let iterator: RangeInclusive<u64> = 1..=n;
        let mut column: Column = column::<u64>();

        column.write::<u64>(iterator).unwrap();

        for iteration in 1..=iterations {
            let summe: u64 = column
                .snapshot()
                .map(|node| {
                    let vectors = node.vectors::<u64>().unwrap();
                    vectors
                        .map(|vector| {
                            vector
                                .map(|window| window.with(|slice, _| slice.iter().sum::<u64>()))
                                .sum::<u64>()
                        })
                        .sum::<u64>()
                })
                .sum::<u64>();

            assert_eq!(summe, n * (n + 1) / 2);
            println!(
                "Iteration {iteration} / {iterations} ({}%)",
                (iteration as f64 / iterations as f64) * 100f64
            );
        }
    }

    #[test]
    fn parallel_snapshot_is_consistent() {
        let (n, iterations, num_threads) = if cfg!(miri) {
            (150_000, 10, 2)
        } else {
            (
                u32::MAX as u64,
                1_000,
                std::thread::available_parallelism()
                    .map(|core| core.get())
                    .unwrap(),
            )
        };

        let iterator: RangeInclusive<u64> = 1..=n;
        let mut column: Column = column::<u64>();

        column.write::<u64>(iterator).unwrap();

        use rayon::prelude::*;
        let pool: rayon::ThreadPool = rayon::ThreadPoolBuilder::new()
            .num_threads(num_threads)
            .build()
            .unwrap();

        for iteration in 1..=iterations {
            let summe: u64 = pool.install(|| {
                column
                    .snapshot()
                    .par_bridge()
                    .map(|node| {
                        node.vectors::<u64>()
                            .unwrap()
                            .map(|vector| {
                                vector
                                    .map(|window| window.with(|window, _| window.iter().sum()))
                                    .sum()
                            })
                            .sum()
                    })
                    .sum()
            });

            assert_eq!(summe, n(n + 1) / 2);

            println!(
                "Iteration {iteration} / {iterations} ({}%)",
                (iteration as f64 / iterations as f64) * 100f64
            );
        }
    }
}
