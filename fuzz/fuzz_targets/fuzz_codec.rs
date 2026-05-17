#![no_main]
use libfuzzer_sys::fuzz_target;
use bytes::{BytesMut, BufMut};
use tokio_util::codec::Decoder;
use vasal_sidecar_sdk::codec::LengthPrefixCodec;

fuzz_target!(|data: &[u8]| {
    let mut codec = LengthPrefixCodec::new();
    let mut buf = BytesMut::from(data);
    // Attempt to decode arbitrary bytes — should never panic.
    let _ = codec.decode(&mut buf);
});
