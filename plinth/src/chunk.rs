use crate::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
use std::mem::ManuallyDrop;

use crate::{
    buffer::{Buffer, ByteOffset},
    column::ColumnState,
};

const VECTOR_SIZE: usize = 1024;

#[derive(Clone, Copy)]
pub(crate) struct LogicalOffset(usize);

impl LogicalOffset {
    pub(crate) const fn new(offset: usize) -> Self {
        Self(offset)
    }

    pub(crate) const fn get(&self) -> usize {
        self.0
    }

    pub(crate) fn increment(&mut self, num_elements: usize) {
        self.0 += num_elements
    }
}

pub struct SharedMutableChunk(Arc<RwLock<MutableChunk>>);

// The entire Chunk relies for (safe) freezing on it being managed by an Arc.
// It needs the RwLock to prevent the case where we have read while the write hasnt succeeded (e.g reading uninitialized Bytes). This is crucial for snapshots
struct MutableChunk {
    inner: ManuallyDrop<Buffer>,
    element_size: usize,
    current: LogicalOffset,
    column_state: Arc<Mutex<ColumnState>>,
}

impl MutableChunk {
    pub(crate) fn write(mut self, data: &[u8]) -> Option<Self> {
        assert!(data.len() == self.element_size);
        let offset: ByteOffset = ByteOffset::new(self.current.get() * self.element_size);

        self.inner.write(offset, data);
        self.current.increment(1);

        if offset.get() + self.element_size == self.inner.size() {
            // Returning None causes this MutableChunk to be dropped.
            // If this is the last Arc owning it, Drop will freeze the chunk.
            None
        } else {
            Some(self)
        }
    }
}

impl Drop for MutableChunk {
    fn drop(&mut self) {
        let buffer: Buffer = unsafe { ManuallyDrop::take(&mut self.inner) };
        let frozen: FrozenChunk = FrozenChunk {
            inner: buffer,
            element_size: self.element_size,
            next: OnceLock::new(),
        };
        let new_entry: Arc<FrozenChunk> = Arc::new(frozen);
        let mut inner_state: MutexGuard<ColumnState> = self.column_state.lock().unwrap();

        if let Some(ref previous_last) = inner_state.frozen_tail {
            previous_last
                .next
                .set(new_entry.clone())
                .expect("Chunk has already been written to");
        }

        inner_state.frozen_tail = Some(new_entry);
    }
}

#[derive(Debug)]
pub struct FrozenChunk {
    inner: Buffer,
    element_size: usize,
    // Thank you: https://doc.rust-lang.org/beta/std/sync/struct.OnceLock.html
    next: OnceLock<Arc<Self>>,
}

impl FrozenChunk {
    pub fn iter(&self) -> impl Iterator<Item = Vector> {
        assert!(
            self.inner.size() % self.element_size == 0,
            "Invariant of the chunk consisting of only entries of the same type is not being upheld"
        );

        VectorIter {
            inner: self,
            end: self.inner.size() / self.element_size,
            current: LogicalOffset::new(0),
        }
    }
}

pub(crate) struct VectorIter<'vector> {
    inner: &'vector FrozenChunk,
    end: usize,
    current: LogicalOffset,
}

/// This is the primary unit of work
pub struct Vector<'a>(&'a [u8]);

impl<'vector> Iterator for VectorIter<'vector> {
    type Item = Vector<'vector>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current.get() == self.end {
            return None;
        }

        let slice_length = (self.end - self.current.get()).min(VECTOR_SIZE);
        let vector: Vector = Vector(self.inner.inner.read(
            ByteOffset::new(self.current.get() * self.inner.element_size),
            slice_length * self.inner.element_size,
        ));
        self.current.increment(slice_length);
        Some(vector)
    }
}
