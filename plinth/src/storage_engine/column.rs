use arrow::array::ArrayBuilder;

use crate::storage_engine::{
    chunk::{FrozenChunk, MutableChunk},
    units::VersionID,
};

pub struct Column {
    head: Option<FrozenChunk>,
    tail: Option<MutableChunk>,
}

impl Column {
    pub(crate) fn new<B: ArrayBuilder>(id: VersionID, builder: B) -> Self {
        Self {
            head: None,
            tail: Some(MutableChunk::new(Box::new(builder), id)),
        }
    }
}
