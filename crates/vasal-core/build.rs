fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false) // Agent is a client, not a server.
        .build_client(true)
        .compile_protos(
            &["../../proto/vasal/v1/dispatch.proto"],
            &["../../proto"],
        )?;
    Ok(())
}
