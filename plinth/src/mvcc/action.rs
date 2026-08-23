pub(crate) enum Action {
    Deletion(DeleteAction),
    Insert(InsertAction),
    Update(UpdateAction)
}


pub enum Predicate {
    Eq(Column, Value),
    Gt(Column, Value),
    Lt(Column, Value),
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
    Not(Box<Predicate>),
}


pub(crate) struct DeleteAction {
    predicate: Predicate
}

pub(crate) struct InsertAction {
    record: Vec<(Column, Value)>
}

pub(crate) struct UpdateAction {
    predicate: Predicate
}