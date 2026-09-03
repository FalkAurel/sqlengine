fn main() {
    #[cfg(feature = "bench")]
    for _ in 0..1000 {
        plinth::benchmark::profile_append_64k();
    }
}