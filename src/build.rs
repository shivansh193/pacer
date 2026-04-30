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

    cc::Build::new()
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