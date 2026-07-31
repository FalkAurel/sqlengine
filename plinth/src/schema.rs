use std::{alloc::Layout, num::NonZeroUsize, ptr::NonNull, sync::atomic::AtomicPtr};

use crate::column::Column;

pub trait IntoSchema<'schema> {
    fn get_columns(&self) -> Schema<'schema>;
    fn get_chunk_size() -> NonZeroUsize {
        NonZeroUsize::new(u16::MAX as usize).expect("Shouldn't fail")
    }
}

pub struct Schema<'schema> {
    columns: Vec<Column>,
    chunk_size: NonZeroUsize,
}
