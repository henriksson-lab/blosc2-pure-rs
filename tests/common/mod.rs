pub mod ffi {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(clippy::all)]

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[link(name = "blosc2", kind = "static")]
extern "C" {}

/// RAII guard that initializes C-Blosc2 for FFI comparison tests.
pub struct Blosc2 {
    _private: (),
}

impl Blosc2 {
    pub fn new() -> Self {
        let mut refs = blosc2_refs().lock().expect("Blosc2 init mutex poisoned");
        if *refs == 0 {
            unsafe {
                ffi::blosc2_init();
            }
        }
        *refs += 1;
        Blosc2 { _private: () }
    }
}

impl Default for Blosc2 {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Blosc2 {
    fn drop(&mut self) {
        let mut refs = blosc2_refs().lock().expect("Blosc2 destroy mutex poisoned");
        *refs -= 1;
        if *refs == 0 {
            unsafe {
                ffi::blosc2_destroy();
            }
        }
    }
}

fn blosc2_refs() -> &'static std::sync::Mutex<usize> {
    static REFS: std::sync::OnceLock<std::sync::Mutex<usize>> = std::sync::OnceLock::new();
    REFS.get_or_init(|| std::sync::Mutex::new(0))
}
