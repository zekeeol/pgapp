fn main() -> Result<(), Box<dyn std::error::Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    unsafe {
        std::env::set_var("PROTOC", protoc);
    }

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .compile_protos(
            &[
                "../../proto/pgapp/v1/common.proto",
                "../../proto/pgapp/v1/health.proto",
                "../../proto/pgapp/v1/cache.proto",
                "../../proto/pgapp/v1/mq.proto",
            ],
            &["../../proto"],
        )?;

    println!("cargo:rerun-if-changed=../../proto/pgapp/v1/common.proto");
    println!("cargo:rerun-if-changed=../../proto/pgapp/v1/health.proto");
    println!("cargo:rerun-if-changed=../../proto/pgapp/v1/cache.proto");
    println!("cargo:rerun-if-changed=../../proto/pgapp/v1/mq.proto");
    Ok(())
}
