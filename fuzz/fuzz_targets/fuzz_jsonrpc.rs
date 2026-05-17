#![no_main]
use libfuzzer_sys::fuzz_target;
use vasal_protocol::jsonrpc::{Request, Response};

fuzz_target!(|data: &[u8]| {
    // Attempt to parse arbitrary bytes as JSON-RPC messages — should never panic.
    let _ = serde_json::from_slice::<Request>(data);
    let _ = serde_json::from_slice::<Response>(data);
});
