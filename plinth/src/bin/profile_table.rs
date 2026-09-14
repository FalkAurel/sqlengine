use std::ops::Range;

use plinth::table::{
    Empty, Node, StreamingTableRow, Table, TableBuilder, TableRow,
};

fn main() {
    let mut user_stream: Table<UserStream> = TableBuilder::default()
        .add::<i32>("values")
        .unwrap()
        .finish();

    for _ in 0..10000 {
        std::hint::black_box(
            user_stream
                .streaming_insert(UserStream {
                    values: 0..(1024 * 1024),
                })
                .unwrap(),
        );
    }
}

struct UserStream {
    values: Range<i32>,
}

impl TableRow for UserStream {
    type Schema = Node<i32, Empty>;
}

impl StreamingTableRow for UserStream {
    fn visit_columns_streaming<'a, V: plinth::table::StreamingFieldVisitor<'a>>(
        self,
        visitor: &mut V,
    ) -> Result<(), plinth::table::VisitorError>
    where
        Self: 'a,
    {
        visitor.visit_fields::<usize, i32>(0, self.values)
    }
}