use arrow::array::{Array, ArrayBuilder};
use std::{marker::PhantomData, sync::Arc};

use crate::{
    storage_engine::{
        chunk::{Append, AppendableType, ChunkWriter, FrozenChunk, MutableChunk},
        units::VersionID,
        writer_buffer::WriteBufferSnapshot,
    },
    writer_buffer::{CheckedBuilder, ChunkBuffer, ExceedsChunkCapacity, Full, Partial},
};

#[derive(Debug)]
pub struct InvalidDowncast;

pub struct Column {
    head: Option<Arc<FrozenChunk>>,
    frozen_tail: Option<Arc<FrozenChunk>>,
    tail: Option<MutableChunk>,
    next_version: Box<dyn Fn() -> VersionID + Send>,
    // `Column` is intentionally !Sync. Synchronization must be provided by
    // the owner when accessing it concurrently.
    _marker: PhantomData<*const ()>,
}

impl Column {
    pub(crate) fn new<B: ArrayBuilder>(
        next_version: Box<dyn Fn() -> VersionID + Send>,
        builder: B,
    ) -> Self {
        Self {
            head: None,
            frozen_tail: None,
            tail: Some(MutableChunk::new(Box::new(builder), next_version())),
            next_version,
            _marker: PhantomData,
        }
    }

    pub(crate) fn write<V: AppendableType + Send>(
        &mut self,
        values: impl Iterator<Item = <<V as AppendableType>::Builder as Append<V>>::Element>,
    ) -> Result<(), InvalidDowncast> {
        let mut writer: ChunkWriter<V> = match self
            .tail
            .take()
            .expect("Impossible to fail since we manually set a MutableChunks")
            .get_writer::<V>()
        {
            Ok(res) => res,
            Err(err) => {
                self.tail = Some(err);
                return Err(InvalidDowncast);
            }
        };

        for value in values {
            match writer.append(value) {
                Ok(new_writer) => {
                    writer = new_writer;
                }

                Err((builder, chunk_buffer, chunk_id)) => {
                    let resetted_builder: Box<V::Builder> = match self.fuse_and_publish::<V>(
                        chunk_id,
                        builder,
                        chunk_buffer,
                    ) {
                        Ok(builder) => builder,
                        Err(ExceedsChunkCapacity) => unreachable!(
                            "Writer invariant violated: chunk capacity exceeded unexpectedly. Check ChunkWriter::append."
                        ),
                    };

                    writer = MutableChunk::new(resetted_builder, (self.next_version)())
                        .get_writer()
                        .or(Err(InvalidDowncast))?;
                }
            }
        }

        // Lazy flush — rows stay in the builder until a snapshot is requested.
        let (builder, chunk_buffer, chunk_id) = writer.into_parts();
        self.tail = Some(MutableChunk::from_raw(builder, chunk_buffer, chunk_id));

        Ok(())
    }

    fn fuse_and_publish<B: AppendableType + Send>(
        &mut self,
        chunk_id: VersionID,
        builder: Box<B::Builder>,
        buffer: ChunkBuffer,
    ) -> Result<Box<B::Builder>, ExceedsChunkCapacity> {
        let checked_builder: CheckedBuilder<B::Builder> = buffer.make_builder(builder)?;

        match buffer.append_from_builder(checked_builder) {
            Err(Full { array, builder }) => {
                self.publish_frozen_chunk(array, chunk_id);
                Ok(builder)
            }
            Ok(_) => unreachable!(
                "chunk_buffer.size + builder.len == CHUNK_SIZE, so append_from_builder must fuse"
            ),
        }
    }

    pub(crate) fn write_values<B: AppendableType + Send>(
        &mut self,
        mut values: &[<<B as AppendableType>::Builder as Append<B>>::Element],
    ) -> Result<(), InvalidDowncast> {
        let mut writer: ChunkWriter<B> = match self
            .tail
            .take()
            .expect("Impossible to fail since we manually set a MutableChunks")
            .get_writer::<B>()
        {
            Ok(res) => res,
            Err(err) => {
                self.tail = Some(err);
                return Err(InvalidDowncast);
            }
        };

        while !values.is_empty() {
            match writer.append_values(values) {
                Ok(new_writer) => {
                    writer = new_writer;
                    break;
                }
                Err((builder, chunk_buffer, chunk_id, remainder)) => {
                    values = remainder;
                    let resetted_builder: Box<B::Builder> = match self.fuse_and_publish::<B>(
                        chunk_id,
                        builder,
                        chunk_buffer,
                    ) {
                        Ok(builder) => builder,
                        Err(ExceedsChunkCapacity) => unreachable!(
                            "Writer invariant violated: chunk capacity exceeded unexpectedly. Check ChunkWriter::append."
                        ),
                    };

                    writer = MutableChunk::new(resetted_builder, (self.next_version)())
                        .get_writer()
                        .or(Err(InvalidDowncast))?;
                }
            }
        }

        // Lazy flush — rows stay in the builder until a snapshot is requested.
        let (builder, chunk_buffer, chunk_id) = writer.into_parts();
        self.tail = Some(MutableChunk::from_raw(builder, chunk_buffer, chunk_id));

        Ok(())
    }

    /// Returns the oldest frozen chunk, or `None` if no chunks have been frozen.
    pub(crate) fn read_frozen(&self) -> Option<Arc<FrozenChunk>> {
        self.head.clone()
    }

    /// Returns a zero-copy snapshot of the committed rows in the mutable write
    /// buffer. Pending rows that have accumulated in the builder since the last
    /// snapshot are flushed to a single `Arc<dyn Array>` (zero-copy `finish()`)
    /// before the snapshot is taken, so the caller always sees up-to-date state.
    ///
    /// Returns `None` when the tail has temporarily been taken by a writer.
    pub(crate) fn read_mutable(&mut self) -> Option<WriteBufferSnapshot> {
        let chunk: MutableChunk = self.tail.take()?;
        let (builder, chunk_buffer, chunk_id) = chunk.into_parts();

        if builder.is_empty() {
            // Nothing pending — return what is already committed.
            let snap: WriteBufferSnapshot = chunk_buffer.snapshot();
            self.tail = Some(MutableChunk::from_raw(builder, chunk_buffer, chunk_id));
            return Some(snap);
        }

        // Flush pending rows to a single Arc — zero-copy (finish() transfers the
        // builder's Vec<T> into an Arrow Buffer without copying).
        let checked: CheckedBuilder<dyn ArrayBuilder> = match chunk_buffer.make_builder(builder) {
            Ok(c) => c,
            Err(_) => unreachable!(
                "pending builder rows always fit: write() enforces the capacity invariant"
            ),
        };

        match chunk_buffer.append_from_builder(checked) {
            Ok(Partial { builder, buffer }) => {
                let snap: WriteBufferSnapshot = buffer.snapshot();
                self.tail = Some(MutableChunk::from_raw(builder, buffer, chunk_id));
                Some(snap)
            }
            Err(Full { builder, array }) => {
                // Pending rows exactly filled the chunk — publish and start a new tail.
                self.publish_frozen_chunk(array, chunk_id);
                self.tail = Some(MutableChunk::new(builder, (self.next_version)()));
                Some(self.tail.as_ref().unwrap().snapshot())
            }
        }
    }

    fn publish_frozen_chunk(&mut self, array: Arc<dyn Array>, chunk_id: VersionID) {
        let current: Arc<FrozenChunk> = Arc::new(FrozenChunk::new(array, chunk_id));
        match self.frozen_tail.take() {
            Some(previous) => {
                previous
                    .set_next(current.clone())
                    .expect("frozen chunk next link must only be initialized once");
                self.frozen_tail = Some(current);
            }
            None => {
                self.head = Some(current.clone());
                self.frozen_tail = Some(current);
            }
        }
    }
}

// `Column` is intentionally !Sync because concurrent access must be
// synchronized externally. It remains Send so ownership can be transferred
// between threads and synchronized there.
unsafe impl Send for Column {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use arrow::array::{Array, Int64Array};

    use crate::storage_engine::{
        chunk::{AppendableType, CHUNK_SIZE},
        column::{Column, InvalidDowncast},
        units::VersionID,
    };

    fn version_generator() -> Box<dyn Fn() -> VersionID + Send> {
        let next_id: AtomicU64 = AtomicU64::new(0);

        Box::new(move || VersionID::new(next_id.fetch_add(1, Ordering::Relaxed)))
    }

    #[test]
    fn new_column_has_mutable_tail() {
        let mut column = Column::new(version_generator(), i64::builder());

        assert!(column.read_frozen().is_none());

        let snapshot = column
            .read_mutable()
            .expect("new column should have a mutable tail");

        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn mutable_snapshot_reflects_current_state() {
        let mut column = Column::new(version_generator(), i64::builder());

        column
            .write::<i64>([10i64, 20i64, 30i64].into_iter())
            .unwrap();

        let snapshot = column.read_mutable().expect("mutable tail should exist");

        // Each write() commits exactly one Arc into the buffer.
        let mut arrays = snapshot.iter();
        let array = arrays
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot should be an Int64Array");
        assert!(arrays.next().is_none());
        assert_eq!(array.values(), &[10, 20, 30]);
    }

    #[test]
    fn mutable_snapshot_is_point_in_time() {
        let mut column = Column::new(version_generator(), i64::builder());

        column.write::<i64>([10, 20, 30].into_iter()).unwrap();
        let first = column.read_mutable().expect("mutable tail should exist");

        column.write::<i64>([40, 50].into_iter()).unwrap();
        let second = column.read_mutable().expect("mutable tail should exist");

        // first snapshot was taken after one write: one array [10, 20, 30]
        let mut first_iter = first.iter();
        let first_array = first_iter
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(first_iter.next().is_none());
        assert_eq!(first_array.values(), &[10, 20, 30]);

        // second snapshot was taken after two writes: two separate per-commit arrays
        let mut second_iter = second.iter();
        let second_a = second_iter
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let second_b = second_iter
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(second_iter.next().is_none());
        assert_eq!(second_a.values(), &[10, 20, 30]);
        assert_eq!(second_b.values(), &[40, 50]);
    }

    #[test]
    fn writing_less_than_chunk_size_keeps_data_mutable() {
        let mut column = Column::new(version_generator(), i64::builder());

        column.write::<i64>(0..100).unwrap();

        assert!(column.read_frozen().is_none());

        let snapshot = column.read_mutable().expect("mutable tail should exist");

        assert_eq!(snapshot.len(), 100);
    }

    #[test]
    fn filling_chunk_publishes_frozen_chunk_and_creates_new_tail() {
        let mut column = Column::new(version_generator(), i64::builder());

        column.write::<i64>(0..CHUNK_SIZE.get() as i64).unwrap();

        let frozen = column.read_frozen().expect("full chunk should be frozen");

        assert_eq!(frozen.chunk_id(), VersionID::new(0));

        assert_eq!(
            frozen.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );

        let mutable = column
            .read_mutable()
            .expect("new mutable tail should exist");

        assert_eq!(mutable.len(), 0);
    }

    #[test]
    fn write_crossing_multiple_chunks_builds_correct_chain() {
        let mut column = Column::new(version_generator(), i64::builder());

        let total = CHUNK_SIZE.get() * 2 + 10;

        column.write::<i64>(0..total as i64).unwrap();

        let first = column
            .read_frozen()
            .expect("first frozen chunk should exist");

        let second = first.next().expect("second frozen chunk should exist");

        assert!(second.next().is_none());

        assert_eq!(first.chunk_id(), VersionID::new(0));
        assert_eq!(second.chunk_id(), VersionID::new(1));

        assert_eq!(
            first.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );

        assert_eq!(
            second.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );

        let mutable = column
            .read_mutable()
            .expect("remaining values should be in mutable tail");

        assert_eq!(mutable.len(), 10);
    }

    #[test]
    fn frozen_chunks_preserve_insertion_order() {
        let mut column = Column::new(version_generator(), i64::builder());

        let chunk_size = CHUNK_SIZE.get() as i64;

        column.write::<i64>(0..chunk_size).unwrap();

        column.write::<i64>(chunk_size..chunk_size * 2).unwrap();

        let first = column.read_frozen().unwrap();
        let second = first.next().unwrap();

        let first_array = first.view::<Int64Array>().unwrap();
        let second_array = second.view::<Int64Array>().unwrap();

        assert_eq!(first_array.array().value(0), 0);
        assert_eq!(
            first_array.array().value(CHUNK_SIZE.get() as usize - 1),
            chunk_size - 1
        );

        assert_eq!(second_array.array().value(0), chunk_size);
        assert_eq!(
            second_array.array().value(CHUNK_SIZE.get() as usize - 1),
            chunk_size * 2 - 1
        );
    }

    #[test]
    fn version_ids_are_generated_once_per_chunk() {
        let mut column = Column::new(version_generator(), i64::builder());

        let total = CHUNK_SIZE.get() * 3;

        column.write::<i64>(0..total as i64).unwrap();

        let first = column.read_frozen().unwrap();
        let second = first.next().unwrap();
        let third = second.next().unwrap();

        assert_eq!(first.chunk_id(), VersionID::new(0));
        assert_eq!(second.chunk_id(), VersionID::new(1));
        assert_eq!(third.chunk_id(), VersionID::new(2));

        assert!(third.next().is_none());

        assert_eq!(column.read_mutable().unwrap().len(), 0);
    }

    #[test]
    fn partial_final_chunk_remains_mutable() {
        let mut column = Column::new(version_generator(), i64::builder());

        let total = CHUNK_SIZE.get() + 123;

        column.write::<i64>(0..total as i64).unwrap();

        let frozen = column.read_frozen().expect("first chunk should be frozen");

        assert_eq!(
            frozen.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );

        let mutable = column
            .read_mutable()
            .expect("partial final chunk should remain mutable");

        assert_eq!(mutable.len(), 123);
    }

    #[test]
    fn write_values_less_than_chunk_size_keeps_data_mutable() {
        let mut column = Column::new(version_generator(), i64::builder());

        let values: Vec<i64> = (0..100).collect();
        column.write_values::<i64>(&values).unwrap();

        assert!(column.read_frozen().is_none());
        assert_eq!(column.read_mutable().unwrap().len(), 100);
    }

    #[test]
    fn write_values_snapshot_reflects_written_data() {
        let mut column = Column::new(version_generator(), i64::builder());

        column.write_values::<i64>(&[10, 20, 30]).unwrap();

        let snapshot = column.read_mutable().expect("mutable tail should exist");

        // write_values commits one Arc per call into the buffer.
        let mut arrays = snapshot.iter();
        let array = arrays
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot should be Int64Array");
        assert!(arrays.next().is_none());
        assert_eq!(array.values(), &[10, 20, 30]);
    }

    #[test]
    fn write_values_snapshot_is_point_in_time() {
        let mut column = Column::new(version_generator(), i64::builder());

        column.write_values::<i64>(&[10, 20, 30]).unwrap();
        let first = column.read_mutable().unwrap();

        column.write_values::<i64>(&[40, 50]).unwrap();
        let second = column.read_mutable().unwrap();

        // first snapshot: one array [10, 20, 30]
        let mut first_iter = first.iter();
        let first_array = first_iter
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(first_iter.next().is_none());
        assert_eq!(first_array.values(), &[10, 20, 30]);

        // second snapshot: two separate per-commit arrays
        let mut second_iter = second.iter();
        let second_a = second_iter
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let second_b = second_iter
            .next()
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert!(second_iter.next().is_none());
        assert_eq!(second_a.values(), &[10, 20, 30]);
        assert_eq!(second_b.values(), &[40, 50]);
    }

    #[test]
    fn write_values_filling_chunk_publishes_frozen() {
        let mut column = Column::new(version_generator(), i64::builder());

        let values: Vec<i64> = (0..CHUNK_SIZE.get() as i64).collect();
        column.write_values::<i64>(&values).unwrap();

        let frozen = column.read_frozen().expect("full chunk should be frozen");

        assert_eq!(frozen.chunk_id(), VersionID::new(0));
        assert_eq!(
            frozen.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );
        assert_eq!(column.read_mutable().unwrap().len(), 0);
    }

    #[test]
    fn write_values_crossing_multiple_chunks_builds_chain() {
        let mut column = Column::new(version_generator(), i64::builder());

        let total = CHUNK_SIZE.get() * 2 + 10;
        let values: Vec<i64> = (0..total as i64).collect();
        column.write_values::<i64>(&values).unwrap();

        let first = column
            .read_frozen()
            .expect("first frozen chunk should exist");
        let second = first.next().expect("second frozen chunk should exist");

        assert!(second.next().is_none());
        assert_eq!(first.chunk_id(), VersionID::new(0));
        assert_eq!(second.chunk_id(), VersionID::new(1));
        assert_eq!(
            first.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );
        assert_eq!(
            second.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );
        assert_eq!(column.read_mutable().unwrap().len(), 10);
    }

    #[test]
    fn write_values_preserves_insertion_order() {
        let mut column = Column::new(version_generator(), i64::builder());

        let chunk_size = CHUNK_SIZE.get() as i64;
        let values: Vec<i64> = (0..chunk_size * 2).collect();
        column.write_values::<i64>(&values).unwrap();

        let first = column.read_frozen().unwrap();
        let second = first.next().unwrap();

        let first_array = first.view::<Int64Array>().unwrap();
        let second_array = second.view::<Int64Array>().unwrap();

        assert_eq!(first_array.array().value(0), 0);
        assert_eq!(
            first_array.array().value(CHUNK_SIZE.get() as usize - 1),
            chunk_size - 1
        );
        assert_eq!(second_array.array().value(0), chunk_size);
        assert_eq!(
            second_array.array().value(CHUNK_SIZE.get() as usize - 1),
            chunk_size * 2 - 1
        );
    }

    #[test]
    fn write_values_version_ids_per_chunk() {
        let mut column = Column::new(version_generator(), i64::builder());

        let values: Vec<i64> = (0..CHUNK_SIZE.get() as i64 * 3).collect();
        column.write_values::<i64>(&values).unwrap();

        let first = column.read_frozen().unwrap();
        let second = first.next().unwrap();
        let third = second.next().unwrap();

        assert_eq!(first.chunk_id(), VersionID::new(0));
        assert_eq!(second.chunk_id(), VersionID::new(1));
        assert_eq!(third.chunk_id(), VersionID::new(2));
        assert!(third.next().is_none());
        assert_eq!(column.read_mutable().unwrap().len(), 0);
    }

    #[test]
    fn write_values_partial_final_chunk_remains_mutable() {
        let mut column = Column::new(version_generator(), i64::builder());

        let total = CHUNK_SIZE.get() + 123;
        let values: Vec<i64> = (0..total as i64).collect();
        column.write_values::<i64>(&values).unwrap();

        let frozen = column.read_frozen().expect("first chunk should be frozen");
        assert_eq!(
            frozen.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );
        assert_eq!(column.read_mutable().unwrap().len(), 123);
    }

    #[test]
    fn write_values_wrong_type_returns_invalid_downcast() {
        let mut column = Column::new(version_generator(), i64::builder());

        let result = column.write_values::<f32>(&[1.0, 2.0, 3.0]);

        assert!(
            matches!(result, Err(InvalidDowncast)),
            "mismatched builder type must return InvalidDowncast"
        );
    }
}
