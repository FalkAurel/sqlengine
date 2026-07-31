use std::{
    alloc::Layout,
    sync::{Arc, Mutex},
};

use crate::chunk::{FrozenChunk, SharedMutableChunk};

pub(crate) struct Column {
    name: usize,
    frozen_head: Option<Arc<FrozenChunk>>,
    mutable_chunk: SharedMutableChunk,
    shared_column_state: Arc<Mutex<ColumnState>>,
}

pub(crate) struct ColumnState {
    pub(crate) frozen_tail: Option<Arc<FrozenChunk>>,
    layout: Layout,
}
