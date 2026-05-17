fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true) // Server for the CP stub in integration tests.
        .build_client(true)
        .compile_protos(&["../../proto/vasal/v1/dispatch.proto"], &["../../proto"])?;
    Ok(())
}
