use std::ffi::{c_void, CStr};
use std::sync::{Mutex, MutexGuard, Once};

pub mod ffi {
    #![allow(non_upper_case_globals)]
    #![allow(non_camel_case_types)]
    #![allow(non_snake_case)]
    #![allow(dead_code)]
    #![allow(clippy::all)]

    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
}

#[link(name = "blosc2", kind = "dylib")]
extern "C" {}

/// Guard that gives an FFI comparison test exclusive access to C-Blosc2 globals.
///
/// C-Blosc2 owns process-global state, and its public entry points may also
/// auto-initialize that state.  Destroying it when one Rust test's guard drops
/// can invalidate C globals while other parallel tests or raw C-owned objects
/// still exist, so the test process intentionally initializes once and leaves
/// teardown to process exit.  The guard serializes tests that call the raw C
/// global API and restores the process-wide settings that C exposes between
/// tests.
pub struct Blosc2 {
    _lock: MutexGuard<'static, ()>,
}

impl Blosc2 {
    pub fn new() -> Self {
        let lock = blosc2_test_lock()
            .lock()
            .expect("Blosc2 test mutex poisoned");
        blosc2_init_once().call_once(|| unsafe {
            ffi::blosc2_set_threads_callback(
                c_blosc2_test_threads_callback(),
                std::ptr::null_mut::<c_void>(),
            );
            ffi::blosc2_init();
        });
        reset_c_blosc2_process_globals();
        Blosc2 { _lock: lock }
    }
}

impl Default for Blosc2 {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Blosc2 {
    fn drop(&mut self) {
        reset_c_blosc2_process_globals();
    }
}

fn blosc2_init_once() -> &'static Once {
    static INIT: Once = Once::new();
    &INIT
}

fn blosc2_test_lock() -> &'static Mutex<()> {
    static LOCK: Mutex<()> = Mutex::new(());
    &LOCK
}

unsafe extern "C" fn serial_threads_callback(
    _callback_data: *mut c_void,
    dojob: Option<unsafe extern "C" fn(*mut c_void)>,
    numjobs: i32,
    jobdata_elsize: usize,
    jobdata: *mut c_void,
) {
    let Some(dojob) = dojob else {
        return;
    };
    for i in 0..numjobs {
        let job = unsafe {
            jobdata
                .cast::<u8>()
                .add(i as usize * jobdata_elsize)
                .cast::<c_void>()
        };
        unsafe {
            dojob(job);
        }
    }
}

fn c_blosc2_test_threads_callback() -> ffi::blosc_threads_callback {
    match std::env::var_os("BLOSC_TEST_CALLBACK").and_then(|value| value.into_string().ok()) {
        Some(value) if value == "yes" => Some(serial_threads_callback),
        _ => None,
    }
}

fn reset_c_blosc2_process_globals() {
    static BLOSCLZ: &CStr = c"blosclz";

    unsafe {
        // C stores invalid nthread values before check_nthreads rejects them.
        // Once the global context is invalid, the public setter cannot repair
        // the context in-place, so refresh the context after restoring the
        // process-wide stored value.
        if ffi::blosc2_get_nthreads() <= 0 {
            let _ = ffi::blosc2_set_nthreads(1);
            ffi::blosc2_destroy();
            ffi::blosc2_set_threads_callback(
                c_blosc2_test_threads_callback(),
                std::ptr::null_mut::<c_void>(),
            );
            ffi::blosc2_init();
        }
        let _ = ffi::blosc1_set_compressor(BLOSCLZ.as_ptr());
        ffi::blosc1_set_blocksize(0);
        ffi::blosc1_set_splitmode(ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32);
        ffi::blosc2_set_delta(0);
        // If a test used a custom thread callback and started a callback-backed
        // pool, C's release path must still see callback mode while nthreads is
        // reduced.  Clear it after release so no test-local callback/data
        // pointer can leak into the next guard.  Then re-install the same
        // callback that C's test_common.h would install for BLOSC_TEST_CALLBACK.
        let _ = ffi::blosc2_set_nthreads(1);
        reset_c_blosc2_global_context();
        ffi::blosc2_set_threads_callback(
            c_blosc2_test_threads_callback(),
            std::ptr::null_mut::<c_void>(),
        );
    }
}

unsafe fn reset_c_blosc2_global_context() {
    let mut cparams: ffi::blosc2_cparams = std::mem::zeroed();
    cparams.compcode = ffi::BLOSC_BLOSCLZ as u8;
    cparams.typesize = 1;
    cparams.nthreads = 1;
    cparams.splitmode = ffi::BLOSC_FORWARD_COMPAT_SPLIT as i32;

    let mut chunk = [0u8; ffi::BLOSC_EXTENDED_HEADER_LENGTH as usize];
    let csize = unsafe {
        ffi::blosc2_chunk_zeros(
            cparams,
            1,
            chunk.as_mut_ptr().cast::<c_void>(),
            chunk.len() as i32,
        )
    };
    assert_eq!(csize, chunk.len() as i32);

    let mut out = [0xa5u8; 1];
    let dsize = unsafe {
        ffi::blosc2_decompress(
            chunk.as_ptr().cast::<c_void>(),
            csize,
            out.as_mut_ptr().cast::<c_void>(),
            out.len() as i32,
        )
    };
    assert_eq!(dsize, out.len() as i32);
    assert_eq!(out, [0]);
}
