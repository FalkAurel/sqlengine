use arrow::array::{Array, ArrayBuilder};
use std::{marker::PhantomData, sync::Arc};

use crate::storage_engine::{
    chunk::{Append, ChunkWriter, FrozenChunk, MutableChunk},
    units::VersionID,
};

#[derive(Debug)]
pub struct InvalidDowncast;

pub struct Column {
    head: Option<Arc<FrozenChunk>>,
    frozen_tail: Option<Arc<FrozenChunk>>,
    tail: Option<MutableChunk>,
    next_version: Box<dyn Fn() -> VersionID>,
    // `Column` is intentionally !Sync. Synchronization must be provided by
    // the owner when accessing it concurrently.
    _marker: PhantomData<*const ()>,
}

impl Column {
    pub(crate) fn new<B: ArrayBuilder>(
        next_version: Box<dyn Fn() -> VersionID>,
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

    #[inline(always)]
    pub(crate) fn write<B: ArrayBuilder + Append + Send>(
        &mut self,
        values: impl Iterator<Item = <B as Append>::Element>,
    ) -> Result<(), InvalidDowncast> {
        let mut writer: ChunkWriter<B> = match self
            .tail
            .take()
            .expect("Impossible to fail since we manually set a MutableChunks")
            .builder()
        {
            Ok(res) => res,
            Err(err) => {
                self.tail = Some(err);
                return Err(InvalidDowncast);
            }
        };

        for value in values {
            match writer.append(value) {
                Ok(new_builder) => {
                    writer = new_builder;
                }

                Err((mut previous_builder, chunk_id)) => {
                    let array: Arc<dyn Array> = previous_builder.finish(); // Resets the builder
                    self.publish_frozen_chunk(array, chunk_id);

                    match MutableChunk::new(previous_builder, (self.next_version)()).builder() {
                        Ok(res) => writer = res,
                        Err(_) => unreachable!(
                            "builder type invariant violated: ChunkWriter returned a builder that MutableChunk could not recover"
                        ),
                    }
                }
            }
        }

        self.tail = Some(MutableChunk::from(writer));
        Ok(())
    }

    #[inline(always)]
    pub(crate) fn write_values<B: ArrayBuilder + Append + Send>(
        &mut self,
        mut values: &[<B as Append>::Element],
    ) -> Result<(), InvalidDowncast> {
        let mut writer: ChunkWriter<B> = match self
            .tail
            .take()
            .expect("Impossible to fail since we manually set a MutableChunks")
            .builder()
        {
            Ok(res) => res,
            Err(err) => {
                self.tail = Some(err);
                return Err(InvalidDowncast);
            }
        };

        while !values.is_empty() {
            match writer.append_values(values) {
                Ok(new_builder) => {
                    writer = new_builder;
                    break;
                }
                Err((mut builder, chunk_id, remainder)) => {
                    let array: Arc<dyn Array> = builder.finish();
                    self.publish_frozen_chunk(array, chunk_id);

                    values = remainder;

                    match MutableChunk::new(builder, (self.next_version)()).builder() {
                        Ok(res) => writer = res,
                        Err(_) => unreachable!(
                            "builder type invariant violated: ChunkWriter returned a builder that MutableChunk could not recover"
                        ),
                    }
                }
            }
        }

        self.tail = Some(MutableChunk::from(writer));
        Ok(())
    }

    /// Returns the oldest frozen chunk, or `None` if no chunks have been frozen.
    pub(crate) fn read_frozen(&self) -> Option<Arc<FrozenChunk>> {
        self.head.clone()
    }

    /// Returns a snapshot of the mutable tail if it is currently owned by the
    /// column. `None` indicates that the tail has temporarily been taken by a
    /// writer.
    ///
    /// Callers are expected to synchronize access externally (e.g. via MVCC).
    /// However, even if external synchronization is not upheld, this method
    /// prevents inconsistent reads by returning `None` while the tail is
    /// temporarily unavailable.
    pub(crate) fn read_mutable(&self) -> Option<Arc<dyn Array>> {
        self.tail.as_ref().map(|inner| inner.get_snapshot())
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

    use arrow::array::{Array, Float32Builder, Int64Array, Int64Builder};

    use crate::storage_engine::{
        chunk::CHUNK_SIZE,
        column::{Column, InvalidDowncast},
        units::VersionID,
    };

    fn version_generator() -> Box<dyn Fn() -> VersionID> {
        let next_id: AtomicU64 = AtomicU64::new(0);

        Box::new(move || VersionID::new(next_id.fetch_add(1, Ordering::Relaxed)))
    }

    #[test]
    fn new_column_has_mutable_tail() {
        let column = Column::new(version_generator(), Int64Builder::new());

        assert!(column.read_frozen().is_none());

        let snapshot = column
            .read_mutable()
            .expect("new column should have a mutable tail");

        assert_eq!(snapshot.len(), 0);
    }

    #[test]
    fn mutable_snapshot_reflects_current_state() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        column
            .write::<Int64Builder>([10, 20, 30].into_iter())
            .unwrap();

        let snapshot = column.read_mutable().expect("mutable tail should exist");

        let array = snapshot
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot should be an Int64Array");

        assert_eq!(array.values(), &[10, 20, 30]);
    }

    #[test]
    fn mutable_snapshot_is_point_in_time() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        column
            .write::<Int64Builder>([10, 20, 30].into_iter())
            .unwrap();

        let first = column.read_mutable().expect("mutable tail should exist");

        column.write::<Int64Builder>([40, 50].into_iter()).unwrap();

        let second = column.read_mutable().expect("mutable tail should exist");

        let first = first.as_any().downcast_ref::<Int64Array>().unwrap();

        let second = second.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(first.values(), &[10, 20, 30]);
        assert_eq!(second.values(), &[10, 20, 30, 40, 50]);
    }

    #[test]
    fn writing_less_than_chunk_size_keeps_data_mutable() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        column.write::<Int64Builder>(0..100).unwrap();

        assert!(column.read_frozen().is_none());

        let snapshot = column.read_mutable().expect("mutable tail should exist");

        assert_eq!(snapshot.len(), 100);
    }

    #[test]
    fn filling_chunk_publishes_frozen_chunk_and_creates_new_tail() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        column
            .write::<Int64Builder>(0..CHUNK_SIZE.get() as i64)
            .unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let total = CHUNK_SIZE.get() * 2 + 10;

        column.write::<Int64Builder>(0..total as i64).unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let chunk_size = CHUNK_SIZE.get() as i64;

        column.write::<Int64Builder>(0..chunk_size).unwrap();

        column
            .write::<Int64Builder>(chunk_size..chunk_size * 2)
            .unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let total = CHUNK_SIZE.get() * 3;

        column.write::<Int64Builder>(0..total as i64).unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let total = CHUNK_SIZE.get() + 123;

        column.write::<Int64Builder>(0..total as i64).unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let values: Vec<i64> = (0..100).collect();
        column.write_values::<Int64Builder>(&values).unwrap();

        assert!(column.read_frozen().is_none());
        assert_eq!(column.read_mutable().unwrap().len(), 100);
    }

    #[test]
    fn write_values_snapshot_reflects_written_data() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        column.write_values::<Int64Builder>(&[10, 20, 30]).unwrap();

        let snapshot = column
            .read_mutable()
            .expect("mutable tail should exist")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("snapshot should be Int64Array")
            .values()
            .to_vec();

        assert_eq!(snapshot, vec![10, 20, 30]);
    }

    #[test]
    fn write_values_snapshot_is_point_in_time() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        column.write_values::<Int64Builder>(&[10, 20, 30]).unwrap();
        let first = column.read_mutable().unwrap();

        column.write_values::<Int64Builder>(&[40, 50]).unwrap();
        let second = column.read_mutable().unwrap();

        let first = first.as_any().downcast_ref::<Int64Array>().unwrap();
        let second = second.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(first.values(), &[10, 20, 30]);
        assert_eq!(second.values(), &[10, 20, 30, 40, 50]);
    }

    #[test]
    fn write_values_filling_chunk_publishes_frozen() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let values: Vec<i64> = (0..CHUNK_SIZE.get() as i64).collect();
        column.write_values::<Int64Builder>(&values).unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let total = CHUNK_SIZE.get() * 2 + 10;
        let values: Vec<i64> = (0..total as i64).collect();
        column.write_values::<Int64Builder>(&values).unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let chunk_size = CHUNK_SIZE.get() as i64;
        let values: Vec<i64> = (0..chunk_size * 2).collect();
        column.write_values::<Int64Builder>(&values).unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let values: Vec<i64> = (0..CHUNK_SIZE.get() as i64 * 3).collect();
        column.write_values::<Int64Builder>(&values).unwrap();

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
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let total = CHUNK_SIZE.get() + 123;
        let values: Vec<i64> = (0..total as i64).collect();
        column.write_values::<Int64Builder>(&values).unwrap();

        let frozen = column.read_frozen().expect("first chunk should be frozen");
        assert_eq!(
            frozen.view::<Int64Array>().unwrap().array().len(),
            CHUNK_SIZE.get() as usize
        );
        assert_eq!(column.read_mutable().unwrap().len(), 123);
    }

    #[test]
    fn write_values_wrong_type_returns_invalid_downcast() {
        let mut column = Column::new(version_generator(), Int64Builder::new());

        let result = column.write_values::<Float32Builder>(&[1.0, 2.0, 3.0]);

        assert!(
            matches!(result, Err(InvalidDowncast)),
            "mismatched builder type must return InvalidDowncast"
        );
    }
}
