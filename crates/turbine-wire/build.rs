fn main() {
    println!("cargo:rerun-if-changed=../../proto/turbine_wire.proto");
    let mut config = prost_build::Config::new();
    config
        .compile_protos(&["../../proto/turbine_wire.proto"], &["../../proto"])
        .expect("compile protobuf schema");
}
