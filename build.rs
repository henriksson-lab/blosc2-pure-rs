fn main() {
    #[cfg(feature = "_ffi")]
    build_ffi();
}

#[cfg(feature = "_ffi")]
fn build_ffi() {
    use std::env;
    use std::path::PathBuf;

    println!("cargo:rerun-if-env-changed=BLOSC2_C_SOURCE_DIR");
    let c_blosc2_dir = env::var_os("BLOSC2_C_SOURCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("c-blosc2"));
    if !c_blosc2_dir.exists() {
        panic!(
            "_ffi feature requires the C-Blosc2 source tree. Set \
             BLOSC2_C_SOURCE_DIR=/path/to/c-blosc2, fetch c-blosc2/ in this \
             checkout, or build without _ffi."
        );
    }
    let c_blosc2_dir = c_blosc2_dir
        .canonicalize()
        .unwrap_or_else(|err| panic!("failed to canonicalize C-Blosc2 source directory: {err}"));
    for header in ["include/blosc2.h", "include/b2nd.h"] {
        let header_path = c_blosc2_dir.join(header);
        if !header_path.exists() {
            panic!(
                "_ffi feature requires missing C-Blosc2 header: {}",
                header_path.display()
            );
        }
    }
    println!(
        "cargo:rustc-env=BLOSC2_C_SOURCE_DIR_RESOLVED={}",
        c_blosc2_dir.display()
    );

    let dst = cmake::Config::new(&c_blosc2_dir)
        .define("BUILD_TESTS", "OFF")
        .define("BUILD_FUZZERS", "OFF")
        .define("BUILD_BENCHMARKS", "OFF")
        .define("BUILD_EXAMPLES", "OFF")
        .define("BUILD_SHARED", "ON")
        .define("BUILD_STATIC", "OFF")
        .define("BUILD_PLUGINS", "ON")
        .build();

    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-search=native={}/lib64", dst.display());
    println!("cargo:rustc-link-lib=dylib=blosc2");
    println!("cargo:rustc-link-lib=pthread");
    println!("cargo:rustc-link-lib=m");

    let include_path = format!("{}/include", dst.display());
    let source_include_path = c_blosc2_dir.join("include");
    let bindings = bindgen::Builder::default()
        .header(c_blosc2_dir.join("include/blosc2.h").display().to_string())
        .header(c_blosc2_dir.join("include/b2nd.h").display().to_string())
        .clang_arg(format!("-I{}", include_path))
        .clang_arg(format!("-I{}", source_include_path.display()))
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
