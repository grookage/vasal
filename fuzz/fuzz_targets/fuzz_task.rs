#![no_main]
use libfuzzer_sys::fuzz_target;
use vasal_protocol::task::{Task, TaskChain, TaskResult};

fuzz_target!(|data: &[u8]| {
    // Attempt to parse arbitrary bytes as protocol types — should never panic.
    let _ = serde_json::from_slice::<Task>(data);
    let _ = serde_json::from_slice::<TaskChain>(data);
    let _ = serde_json::from_slice::<TaskResult>(data);
});
