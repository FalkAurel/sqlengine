```text
                         Column
                           │
              ┌────────────┴────────────┐
              │                         │
         MutableChunk<T>          FrozenChunk
              │                         │
        T::ArrayBuilder            Arc<dyn Array>
              │                         │
          append()                      │
              │                         │
              └────── finish() ─────────┘
                                        │
                                typed::<A>()  ← downcast once
                                        │
                                        ▼
                                TypedChunk<A>
                                        │
                                   VectorIter<A>
                                        │
                              ┌─────────┼─────────┐
                              ▼         ▼         ▼
                          Vector<A> Vector<A> Vector<A>
                              │         │         │
                              └──────── SIMD ─────┘
```