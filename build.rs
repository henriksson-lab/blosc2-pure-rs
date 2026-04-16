fn main() {
    #[cfg(feature = "_ffi")]
    build_ffi();
}

#[cfg(feature = "_ffi")]
fn build_ffi() {
    use std::env;
    use std::path::{Path, PathBuf};

    if !Path::new("c-blosc2").exists() {
        println!(
            "cargo:warning=_ffi feature enabled, but c-blosc2/ is not present; skipping C-Blosc2 FFI build"
        );
        return;
    }

    let dst = cmake::Config::new("c-blosc2")
        .define("BUILD_TESTS", "OFF")
        .define("BUILD_FUZZERS", "OFF")
        .define("BUILD_BENCHMARKS", "OFF")
        .define("BUILD_EXAMPLES", "OFF")
        .define("BUILD_SHARED", "OFF")
        .define("BUILD_STATIC", "ON")
        .define("BUILD_PLUGINS", "ON")
        .build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-search=native={}/lib64", dst.display());
    println!("cargo:rustc-link-lib=static=blosc2");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=m");

    let include_path = format!("{}/include", dst.display());
    let bindings = bindgen::Builder::default()
        .header("c-blosc2/include/blosc2.h")
        .header("c-blosc2/include/b2nd.h")
        .clang_arg(format!("-I{}", include_path))
        .clang_arg("-Ic-blosc2/include")
        .allowlist_function("blosc.*")
        .allowlist_function("b2nd.*")
        .allowlist_type("blosc.*")
        .allowlist_type("b2nd.*")
        .allowlist_var("BLOSC.*")
        .allowlist_var("BLOSC2.*")
        .derive_default(true)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
