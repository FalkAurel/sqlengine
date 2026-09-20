use std::marker::PhantomData;

use crate::table::schema::SchemaList;

pub struct RowIndex<'table, Schema: SchemaList> {
    _marker: PhantomData<&'table Schema>,
}
