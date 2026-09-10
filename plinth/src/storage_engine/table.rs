use crate::{
    storage_engine::{
        chunk::{AppendableType},
        column::{Column, InvalidDowncast},
        units::LogicalOffset,
    }, table::sealed::FieldVisitor,
};

mod sealed {
    use crate::{
        chunk::AppendableType, units::LogicalOffset,
    };

    pub(crate) trait FieldVisitor {
        fn visit_field<V: AppendableType>(
            &mut self,
            index: LogicalOffset,
            value: V,
        );
    }
}

struct TrivialIterator<Value: AppendableType> {
    inner: Option<Value>,
}

impl<V: AppendableType> Iterator for TrivialIterator<V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.take()
    }
}

pub(crate) struct SingleFieldVisitor<'a> {
    columns: &'a mut [Column],
}

impl<'a> FieldVisitor for SingleFieldVisitor<'a> {
    fn visit_field<V: AppendableType>(
        &mut self,
        index: LogicalOffset,
        value: V,
    ) {
        if let Err(InvalidDowncast) = self
            .columns
            .get_mut(index.get() as usize)
            .expect("Schema mapping is invalid")
            .write::<V::Builder>(
                TrivialIterator {
                    inner: Some(value),
                },
            )
        {
            todo!("Do some fucking tracing");
        }
    }
}