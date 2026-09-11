pub(crate) trait Write {
    fn write_iter<T>(iter: impl Iterator<Item = T>);
    fn write_vectored<T>(iter: &[T]);
}
