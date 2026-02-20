fn main() {
    println!("cargo:rerun-if-changed=../../proto/flourine_wire.proto");
    let mut config = prost_build::Config::new();
    config
        .compile_protos(&["../../proto/flourine_wire.proto"], &["../../proto"])
        .expect("compile protobuf schema");
}
