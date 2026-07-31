mod buffer;
mod chunk;
mod column;
mod schema;

#[cfg(not(test))]
mod sync {
    pub use std::sync::{Arc, Mutex, MutexGuard, OnceLock, RwLock};
}

#[cfg(test)]
mod sync {
    pub use shuttle::sync::{Arc, Mutex, MutexGuard, RwLock};

    #[derive(Debug)]
    pub struct OnceLock<T> {
        inner: Mutex<Option<T>>,
    }

    impl<T> OnceLock<T> {
        pub fn new() -> Self {
            Self {
                inner: Mutex::new(None),
            }
        }

        pub fn set(&self, value: T) -> Result<(), T> {
            let mut lock = self.inner.lock().unwrap();

            if lock.is_some() {
                let previous: T = lock.take().unwrap();
                *lock = Some(value);
                return Err(previous);
            }

            *lock = Some(value);

            Ok(())
        }
    }
}
