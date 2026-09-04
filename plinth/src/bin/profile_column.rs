fn main() {
    #[cfg(not(feature = "bench"))]
    assert!(
        false,
        "CARGO_PROFILE_RELEASE_DEBUG=true \
cargo flamegraph --release --features bench --bin profile_column"
    );

    #[cfg(feature = "bench")]
    for _ in 0..1000 {
        plinth::benchmark::profile_append_1m();
    }
}
