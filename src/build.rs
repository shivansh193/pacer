// build.rs
// Compiles the C++ ABtree glue layer into a static library
// that Rust links against via abtree_ffi.rs.
//
// Requires: `cc` crate in [build-dependencies] (see Cargo.toml)
// Requires: cpp/brown_ext_abtree_lf_impl.h  (download separately — see README)
// Requires: cpp/abtree_glue.cpp              (included in this repo)

fn main() {
    println!("cargo:rerun-if-changed=cpp/abtree_glue.cpp");
    println!("cargo:rerun-if-changed=cpp/brown_ext_abtree_lf_impl.h");

    // brown_ext_abtree_lf_impl.h is written against Brown's setbench, which
    // assumes a POSIX toolchain (unistd.h, pthreads, GCC-style -mcx16, etc.)
    // — see README.md. It cannot compile under MSVC. Skip the C++ build
    // entirely off Linux so `cargo build --lib` / `cargo test` (the pure-Rust
    // PACER core) still work; only the `bench` binary needs this and it will
    // fail to link with a clear reason instead of a bare compiler error.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("linux") {
        println!(
            "cargo:warning=skipping cpp/abtree_glue.cpp build: requires a Linux/POSIX toolchain \
             (see README.md). `bench` will fail to link on this platform; `cargo build --lib` \
             and `cargo test` are unaffected."
        );
        return;
    }

    // Even on Linux the header includes setbench's record_manager.h, which is
    // not vendored in this repo (verified: building here on Ubuntu without it
    // fails with "record_manager.h: No such file or directory"). Point
    // SETBENCH_DIR at a setbench checkout to build the FFI bench; otherwise
    // skip, so `cargo build --lib` / `cargo test` still work on Linux.
    let setbench = std::env::var("SETBENCH_DIR").ok();
    println!("cargo:rerun-if-env-changed=SETBENCH_DIR");
    if setbench.is_none() {
        println!(
            "cargo:warning=skipping cpp/abtree_glue.cpp build: set SETBENCH_DIR to a checkout of \
             https://gitlab.com/trbot86/setbench (its headers are not vendored here). `bench` \
             will fail to link without it; `cargo build --lib` and `cargo test` are unaffected."
        );
        return;
    }
    let sb = setbench.unwrap();

    let mut build = cc::Build::new();
    for sub in ["common", "common/recordmgr", "common/rq", "common/papi",
                "common/descriptors", "ds/brown_ext_abtree_lf"] {
        build.include(format!("{sb}/{sub}"));
    }
    build
        .define("MAX_THREADS_POW2", "512")
        .define("CPU_FREQ_GHZ", "2.1")
        .file("cpp/abtree_glue.cpp")
        .include("cpp")             // so glue.cpp can #include the header
        .cpp(true)                  // compile as C++
        .flag_if_supported("-std=c++17")
        .flag_if_supported("-O3")
        .flag_if_supported("-march=native")
        .flag_if_supported("-fno-omit-frame-pointer")  // for profiling
        // Brown's ABtree uses these — suppress warnings we don't own
        .flag_if_supported("-Wno-unused-variable")
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-sign-compare")
        .compile("abtree_glue");

    // Link against libstdc++ (required for C++ stdlib in the glue layer)
    println!("cargo:rustc-link-lib=stdc++");
}