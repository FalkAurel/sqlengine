use std::{
    marker::PhantomData,
    mem::MaybeUninit,
    sync::{Arc, OnceLock},
};

use crate::{
    chunk::CHUNK_SIZE,
    units::{LogicalOffset, LogicalSize},
};

/// Per-row metadata stored by [`RowState`].
///
/// `is_visible` describes whether the row itself currently exists from the
/// storage engine's perspective. Other metadata, such as permissions or
/// flags, can be added to concrete implementations without changing the
/// row-state storage model.
pub trait RowMetadata: Send + Sync + 'static {
    fn is_alive(&self) -> bool;
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

    /// Inserts `n` metadata entries produced by `f`, filling chunk storage
    /// in bulk rather than one call at a time.
    pub(crate) fn insert_n(mut self, n: LogicalSize, f: &impl Fn() -> M) -> Self {
        match self.writer.insert_n(&f, n) {
            Ok(writer) => {
                self.writer = writer;
            }
            Err((chunk, remainder)) => {
                self.writer = Default::default();

                self.freeze_chunk(chunk);
                self = self.insert_n(remainder, f);
            }
        }

        self
    }

    #[inline]
    fn freeze_chunk(&mut self, chunk: Arc<[M; CHUNK_SIZE.as_usize()]>) {
        let new_tail: Arc<RowStateChunk<M>> = Arc::new(RowStateChunk {
            rows: chunk,
            next: OnceLock::new(),
        });

        if let Some(tail) = self.tail.take() {
            debug_assert!(
                tail.next.set(new_tail.clone()).is_ok(),
                "RowStateChunk already has a successor"
            );

            self.tail = Some(new_tail);
        } else {
            debug_assert!(
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
    // Arc rather than Box so we can hand the allocation directly to
    // RowStateChunk without a realloc+copy on freeze. We are the sole
    // owner while current < CHUNK_SIZE; !Sync enforces no concurrent access.
    // I know that this is kinda sketchy. But it gives 30x performance boost and
    // given the state machine design it should be sound and safe as long as everyone is using safe rust.
    rows: Arc<[MaybeUninit<M>; CHUNK_SIZE.as_usize()]>,
    _marker: PhantomData<*const ()>,
}

unsafe impl<M: RowMetadata> Send for MutableRowStateChunk<M> {}

impl<M: RowMetadata> Default for MutableRowStateChunk<M> {
    fn default() -> Self {
        Self {
            current: LogicalOffset::new(0),
            rows: unsafe { Arc::new_uninit().assume_init() },
            _marker: PhantomData,
        }
    }
}

impl<M: RowMetadata> MutableRowStateChunk<M> {
    #[inline(always)]
    fn insert_n<F: Fn() -> M>(
        mut self,
        generator: &F,
        n: LogicalSize,
    ) -> Result<Self, (Arc<[M; CHUNK_SIZE.as_usize()]>, LogicalSize)> {
        let iteration: u64 = n.get().min(CHUNK_SIZE.get() - self.current.get());

        let base: &mut [MaybeUninit<M>; CHUNK_SIZE.as_usize()] = Arc::get_mut(&mut self.rows)
            .expect("Invariant is violated. There are multiple accessors of the MutableChunk");

        for index in self.current.get()..self.current.get() + iteration {
            base[index as usize].write(generator());
        }

        self.current = LogicalOffset::new(iteration) + self.current;

        if iteration == n.get() {
            Ok(self)
        } else {
            let rows: Arc<[M; CHUNK_SIZE.as_usize()]> = unsafe { std::mem::transmute(self.rows) };

            Err((rows, LogicalSize::new(n.get() - iteration)))
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
        fn is_alive(&self) -> bool {
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
        let state = RowState::<TestMetadata>::new()
            .insert_n(LogicalSize::new(1), &Box::new(|| metadata(42)));

        // The chunk is not visible/frozen until it is full.
        assert!(state.start.get().is_none());
        assert!(state.tail.is_none());

        assert_eq!(state.writer.current.as_usize(), 1);
        assert_eq!(unsafe { state.writer.rows[0].assume_init_ref().id }, 42);
    }

    #[test]
    fn filling_one_chunk_freezes_exactly_one_chunk() {
        let mut state = RowState::<TestMetadata>::new();

        for id in 0..CHUNK_SIZE.as_usize() + 1 {
            state = state.insert_n(LogicalSize::new(1), &Box::new(move || metadata(id)));
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
            assert!(row.is_alive());
        }

        assert_eq!(state.writer.current.as_usize(), 1);
    }

    #[test]
    fn inserting_after_full_chunk_creates_new_writer() {
        let mut state = RowState::<TestMetadata>::new();

        for id in 0..CHUNK_SIZE.as_usize() {
            state = state.insert_n(LogicalSize::new(1), &Box::new(|| metadata(id)));
        }

        state = state.insert_n(LogicalSize::new(1), &Box::new(|| metadata(42)));

        let start = state.start.get().expect("first chunk should be published");

        assert_eq!(start.rows.len(), CHUNK_SIZE.as_usize());

        assert_eq!(state.writer.current.as_usize(), 1);

        let row = unsafe { state.writer.rows[0].assume_init_ref() };

        assert_eq!(row.id, 42);
    }

    #[test]
    fn second_chunk_is_linked_to_first() {
        let mut state = RowState::<TestMetadata>::new();

        let total = CHUNK_SIZE.as_usize() * 2 + 1;

        for id in 0..total {
            state = state.insert_n(LogicalSize::new(1), &Box::new(|| metadata(id)));
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
            state = state.insert_n(LogicalSize::new(1), &Box::new(|| metadata(id)));
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
                .insert_n(&Box::new(|| metadata(id)), LogicalSize::new(1))
                .expect("chunk should still have capacity");
        }

        let result = writer.insert_n(&Box::new(|| metadata(42)), LogicalSize::new(1));

        assert!(result.is_err());

        let (rows, entry) = result.expect_err("full chunk must reject another row");

        assert_eq!(entry.as_usize(), 1);

        for id in 0..CHUNK_SIZE.as_usize() {
            assert_eq!(rows[id].id, id);
        }
    }

    // #[test]
    // fn row_state_supports_custom_metadata() {
    //     let mut state = RowState::<TestMetadata>::new();

    //     state = state.insert(TestMetadata {
    //         id: 123,
    //         visibility: AtomicBool::new(false),
    //     });

    //     let row = unsafe { state.writer.rows[0].assume_init_ref() };

    //     assert_eq!(row.id, 123);
    //     assert!(!row.is_alive());
    // }
}
