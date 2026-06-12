fn main() {
    #[cfg(feature = "_ffi")]
    build_ffi();
}

#[cfg(feature = "_ffi")]
fn build_ffi() {
    use std::env;
    use std::path::{Path, PathBuf};

    println!("cargo:rerun-if-env-changed=BLOSC2_C_SOURCE_DIR");
    let manifest_dir = PathBuf::from(env::var_os("CARGO_MANIFEST_DIR").unwrap());
    let c_blosc2_dir = env::var_os("BLOSC2_C_SOURCE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("c-blosc2"));
    let c_blosc2_dir = if c_blosc2_dir.is_absolute() {
        c_blosc2_dir
    } else {
        manifest_dir.join(c_blosc2_dir)
    };
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
    emit_c_blosc2_rerun_if_changed(&c_blosc2_dir);
    for header in required_c_blosc2_headers() {
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
        .define("BUILD_LITE", "OFF")
        .define("DEACTIVATE_IPP", "ON")
        .define("PREFER_EXTERNAL_LZ4", "OFF")
        .define("PREFER_EXTERNAL_ZLIB", "OFF")
        .define("PREFER_EXTERNAL_ZSTD", "OFF")
        .define("BLOSC_INSTALL", "ON")
        .build();

    let target = env::var("TARGET").unwrap_or_default();
    println!("cargo:rustc-link-search=native={}/lib", dst.display());
    println!("cargo:rustc-link-search=native={}/lib64", dst.display());
    println!("cargo:rustc-link-search=native={}/bin", dst.display());
    let lib_name = if target.contains("windows") {
        "libblosc2"
    } else {
        "blosc2"
    };
    println!("cargo:rustc-link-lib=dylib={lib_name}");
    if !target.contains("windows") {
        println!("cargo:rustc-link-lib=pthread");
    }
    if target.contains("linux") || target.contains("android") {
        println!("cargo:rustc-link-lib=dl");
        println!("cargo:rustc-link-lib=rt");
        println!("cargo:rustc-link-lib=m");
    }

    let include_path = format!("{}/include", dst.display());
    let generated_blosc_include_path = Path::new(&dst).join("build/blosc");
    let source_include_path = c_blosc2_dir.join("include");
    let bindings = bindgen::Builder::default()
        .header(c_blosc2_dir.join("include/blosc2.h").display().to_string())
        .header(c_blosc2_dir.join("include/b2nd.h").display().to_string())
        .clang_arg(format!("-I{}", include_path))
        .clang_arg(format!("-I{}", generated_blosc_include_path.display()))
        .clang_arg(format!("-I{}", source_include_path.display()))
        .allowlist_function("blosc.*")
        .allowlist_function("b2nd.*")
        .allowlist_type("blosc.*")
        .allowlist_type("b2nd.*")
        .allowlist_var("B2ND.*")
        .allowlist_var("BLOSC.*")
        .allowlist_var("BLOSC2.*")
        .allowlist_var("DTYPE.*")
        .derive_default(true)
        .generate()
        .expect("Unable to generate bindings");

    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

#[cfg(feature = "_ffi")]
fn required_c_blosc2_headers() -> &'static [&'static str] {
    &[
        "include/blosc2.h",
        "include/b2nd.h",
        "include/blosc2/blosc2-common.h",
        "include/blosc2/blosc2-export.h",
        "include/blosc2/blosc2-stdio.h",
        "include/blosc2/codecs-registry.h",
        "include/blosc2/filters-registry.h",
        "include/blosc2/tuners-registry.h",
    ]
}

#[cfg(feature = "_ffi")]
fn emit_c_blosc2_rerun_if_changed(c_blosc2_dir: &std::path::Path) {
    for path in [
        "CMakeLists.txt",
        "Blosc2Config.cmake.in",
        "blosc2.pc.in",
        "cmake",
        "cmake_uninstall.cmake.in",
        "include",
        "blosc",
        "plugins",
        "internal-complibs/lz4-1.10.0",
        "internal-complibs/zlib-ng-2.0.7",
        "internal-complibs/zstd-1.5.7/libzstd.pc.in",
        "internal-complibs/zstd-1.5.7/zdict.h",
        "internal-complibs/zstd-1.5.7/zstd.h",
        "internal-complibs/zstd-1.5.7/zstd_errors.h",
        "internal-complibs/zstd-1.5.7/common",
        "internal-complibs/zstd-1.5.7/compress",
        "internal-complibs/zstd-1.5.7/decompress",
        "internal-complibs/zstd-1.5.7/dictBuilder",
    ] {
        emit_rerun_if_changed(&c_blosc2_dir.join(path));
    }
}

#[cfg(feature = "_ffi")]
fn emit_rerun_if_changed(path: &std::path::Path) {
    if path.is_dir() {
        let entries = std::fs::read_dir(path)
            .unwrap_or_else(|err| panic!("failed to read C-Blosc2 path {}: {err}", path.display()));
        for entry in entries {
            let entry =
                entry.unwrap_or_else(|err| panic!("failed to read C-Blosc2 path entry: {err}"));
            emit_rerun_if_changed(&entry.path());
        }
    } else if is_c_blosc2_build_input(path) {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

#[cfg(feature = "_ffi")]
fn is_c_blosc2_build_input(path: &std::path::Path) -> bool {
    if path
        .file_name()
        .is_some_and(|name| name == "CMakeLists.txt")
    {
        return true;
    }

    matches!(
        path.extension().and_then(|extension| extension.to_str()),
        Some("S" | "c" | "cmake" | "cmakein" | "h" | "in" | "inc")
    )
}
