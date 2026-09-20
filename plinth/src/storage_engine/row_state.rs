use std::{
    marker::PhantomData,
    mem::MaybeUninit,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
};

use crate::{chunk::CHUNK_SIZE, units::LogicalOffset};

/// Per-row metadata stored by [`RowState`].
///
/// `is_visible` describes whether the row itself currently exists from the
/// storage engine's perspective. Other metadata, such as permissions or
/// flags, can be added to concrete implementations without changing the
/// row-state storage model.
pub(crate) trait RowMetadata: Send + Sync + 'static {
    #[inline(always)]
    fn is_visible(&self) -> bool;
}

/// Default row metadata.
///
/// This currently only tracks whether a row is live. Additional metadata
/// can be added here later if this becomes the default metadata representation.
pub(crate) struct DefaultRowMetadata {
    visibility: AtomicBool,
}

impl Default for DefaultRowMetadata {
    fn default() -> Self {
        Self {
            visibility: AtomicBool::new(true),
        }
    }
}

impl RowMetadata for DefaultRowMetadata {
    #[inline(always)]
    fn is_visible(&self) -> bool {
        self.visibility.load(Ordering::Acquire)
    }
}

pub(crate) struct RowState<M: RowMetadata> {
    start: OnceLock<Arc<RowStateChunk<M>>>,
    tail: Option<Arc<RowStateChunk<M>>>,
    writer: MutableRowStateChunk<M>,
}

impl<M: RowMetadata> RowState<M> {
    pub(crate) fn new() -> Self {
        Self {
            start: OnceLock::new(),
            tail: None,
            writer: Default::default(),
        }
    }

    pub(crate) fn insert(mut self, entry: M) -> Self {
        match self.writer.insert(entry) {
            Ok(writer) => {
                self.writer = writer;
            }
            Err((chunk, entry)) => {
                // `insert` consumes the current writer. Restore `self.writer`
                // before accessing any other part of `self`.
                self.writer = Default::default();

                self.freeze_chunk(chunk);
                self = self.insert(entry);
            }
        }

        self
    }

    fn freeze_chunk(&mut self, chunk: Box<[M; CHUNK_SIZE.as_usize()]>) {
        let new_tail: Arc<RowStateChunk<M>> = Arc::new(RowStateChunk {
            rows: Arc::from(chunk),
            next: OnceLock::new(),
        });

        if let Some(tail) = self.tail.take() {
            assert!(
                tail.next.set(new_tail.clone()).is_ok(),
                "RowStateChunk already has a successor"
            );

            self.tail = Some(new_tail);
        } else {
            assert!(
                self.start.set(new_tail.clone()).is_ok(),
                "RowState already has a starting chunk"
            );

            self.tail = Some(new_tail);
        }
    }
}

struct RowStateChunk<M: RowMetadata> {
    rows: Arc<[M; CHUNK_SIZE.as_usize()]>,
    next: OnceLock<Arc<RowStateChunk<M>>>,
}

#[derive(Debug)]
struct MutableRowStateChunk<M: RowMetadata> {
    current: LogicalOffset,
    rows: Box<[MaybeUninit<M>; CHUNK_SIZE.as_usize()]>,
    _marker: PhantomData<*const ()>,
}

unsafe impl<M: RowMetadata> Send for MutableRowStateChunk<M> {}

impl<M: RowMetadata> Default for MutableRowStateChunk<M> {
    fn default() -> Self {
        Self {
            current: LogicalOffset::new(0),
            rows: Box::new([const { MaybeUninit::uninit() }; CHUNK_SIZE.as_usize()]),
            _marker: PhantomData,
        }
    }
}

impl<M: RowMetadata> MutableRowStateChunk<M> {
    #[inline(always)]
    fn insert(mut self, entry: M) -> Result<Self, (Box<[M; CHUNK_SIZE.as_usize()]>, M)> {
        let index: usize = self.current.as_usize();

        if index < CHUNK_SIZE.as_usize() {
            self.rows[index].write(entry);
            self.current = self.current + LogicalOffset::new(1);

            Ok(self)
        } else {
            debug_assert_eq!(
                index,
                CHUNK_SIZE.as_usize(),
                "MutableRowStateChunk cannot advance beyond a full chunk"
            );

            let rows: Box<[M; CHUNK_SIZE.as_usize()]> = unsafe {
                std::mem::transmute::<
                    Box<[MaybeUninit<M>; CHUNK_SIZE.as_usize()]>,
                    Box<[M; CHUNK_SIZE.as_usize()]>,
                >(self.rows)
            };

            Err((rows, entry))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};

    #[derive(Debug)]
    struct TestMetadata {
        id: usize,
        visibility: AtomicBool,
    }

    impl TestMetadata {
        fn new(id: usize) -> Self {
            Self {
                id,
                visibility: AtomicBool::new(true),
            }
        }
    }

    impl RowMetadata for TestMetadata {
        fn is_visible(&self) -> bool {
            self.visibility.load(Ordering::Acquire)
        }
    }

    fn metadata(id: usize) -> TestMetadata {
        TestMetadata::new(id)
    }

    #[test]
    fn new_row_state_has_empty_chain_and_empty_writer() {
        let state = RowState::<TestMetadata>::new();

        assert!(state.start.get().is_none());
        assert!(state.tail.is_none());
        assert_eq!(state.writer.current.as_usize(), 0);
    }

    #[test]
    fn inserting_one_row_only_updates_mutable_writer() {
        let state = RowState::<TestMetadata>::new().insert(metadata(42));

        // The chunk is not visible/frozen until it is full.
        assert!(state.start.get().is_none());
        assert!(state.tail.is_none());

        assert_eq!(state.writer.current.as_usize(), 1);
        assert_eq!(unsafe { state.writer.rows[0].assume_init_ref().id }, 42);
    }

    #[test]
    fn row_metadata_defaults_to_visible() {
        let row = DefaultRowMetadata::default();

        assert!(row.is_visible());
    }

    #[test]
    fn row_metadata_visibility_can_change() {
        let row = DefaultRowMetadata::default();

        assert!(row.is_visible());

        row.visibility.store(false, Ordering::Release);

        assert!(!row.is_visible());

        row.visibility.store(true, Ordering::Release);

        assert!(row.is_visible());
    }

    #[test]
    fn filling_one_chunk_freezes_exactly_one_chunk() {
        let mut state = RowState::<TestMetadata>::new();

        for id in 0..CHUNK_SIZE.as_usize() + 1 {
            state = state.insert(metadata(id));
        }

        let start = state.start.get().expect("full chunk should be published");

        assert!(Arc::ptr_eq(
            start,
            state.tail.as_ref().expect("tail should be set")
        ));

        assert_eq!(start.rows.len(), CHUNK_SIZE.as_usize());
        assert!(start.next.get().is_none());

        for id in 0..CHUNK_SIZE.as_usize() {
            let row = &start.rows[id];

            assert_eq!(row.id, id);
            assert!(row.is_visible());
        }

        assert_eq!(state.writer.current.as_usize(), 1);
    }

    #[test]
    fn inserting_after_full_chunk_creates_new_writer() {
        let mut state = RowState::<TestMetadata>::new();

        for id in 0..CHUNK_SIZE.as_usize() {
            state = state.insert(metadata(id));
        }

        state = state.insert(metadata(CHUNK_SIZE.as_usize()));

        let start = state.start.get().expect("first chunk should be published");

        assert_eq!(start.rows.len(), CHUNK_SIZE.as_usize());

        assert_eq!(state.writer.current.as_usize(), 1);

        let row = unsafe { state.writer.rows[0].assume_init_ref() };

        assert_eq!(row.id, CHUNK_SIZE.as_usize());
    }

    #[test]
    fn second_chunk_is_linked_to_first() {
        let mut state = RowState::<TestMetadata>::new();

        let total = CHUNK_SIZE.as_usize() * 2 + 1;

        for id in 0..total {
            state = state.insert(metadata(id));
        }

        let first = state.start.get().expect("first chunk should be published");

        let second = first.next.get().expect("second chunk should be linked");

        assert!(second.next.get().is_some() == false);

        for id in 0..CHUNK_SIZE.as_usize() {
            assert_eq!(first.rows[id].id, id);
        }

        for id in 0..CHUNK_SIZE.as_usize() {
            assert_eq!(second.rows[id].id, CHUNK_SIZE.as_usize() + id);
        }

        // The final row remains in the mutable writer.
        assert_eq!(state.writer.current.as_usize(), 1);

        let final_row = unsafe { state.writer.rows[0].assume_init_ref() };

        assert_eq!(final_row.id, total - 1);
    }

    #[test]
    fn exact_chunk_boundary_does_not_create_empty_next_chunk() {
        let mut state = RowState::<TestMetadata>::new();

        for id in 0..CHUNK_SIZE.as_usize() + 1 {
            state = state.insert(metadata(id));
        }

        let start = state.start.get().expect("chunk should be published");

        assert!(start.next.get().is_none());
        assert_eq!(state.writer.current.as_usize(), 1);
    }

    #[test]
    fn mutable_chunk_rejects_insert_after_it_is_full() {
        let mut writer = MutableRowStateChunk::<TestMetadata>::default();

        for id in 0..CHUNK_SIZE.as_usize() {
            writer = writer
                .insert(metadata(id))
                .expect("chunk should still have capacity");
        }

        let result = writer.insert(metadata(CHUNK_SIZE.as_usize()));

        assert!(result.is_err());

        let (rows, entry) = result.expect_err("full chunk must reject another row");

        assert_eq!(entry.id, CHUNK_SIZE.as_usize());

        for id in 0..CHUNK_SIZE.as_usize() {
            assert_eq!(rows[id].id, id);
        }
    }

    #[test]
    fn row_state_supports_custom_metadata() {
        let mut state = RowState::<TestMetadata>::new();

        state = state.insert(TestMetadata {
            id: 123,
            visibility: AtomicBool::new(false),
        });

        let row = unsafe { state.writer.rows[0].assume_init_ref() };

        assert_eq!(row.id, 123);
        assert!(!row.is_visible());
    }
}
