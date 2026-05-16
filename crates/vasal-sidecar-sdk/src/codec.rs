//! Length-prefixed framing codec for sidecar IPC.
//!
//! # Wire Format
//!
//! ```text
//! ┌──────────────────────┬──────────────────────────────────────────┐
//! │  4 bytes (big-endian) │         N bytes (JSON payload)           │
//! │    payload length     │                                          │
//! └──────────────────────┴──────────────────────────────────────────┘
//! ```
//!
//! The 4-byte length prefix encodes `N`, the byte length of the JSON payload
//! that follows. Maximum frame size is [`MAX_MESSAGE_SIZE`] (4 MB, per DD-15).
//!
//! This codec operates at the byte level — JSON parsing is handled by the
//! [`crate::server`] layer above.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Maximum allowed message payload: 4 MB (4,194,304 bytes).
pub const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;

/// Size of the frame header in bytes.
const HEADER_LEN: usize = 4;

/// A codec that frames messages with a 4-byte big-endian length prefix.
///
/// Implements [`Decoder`] (producing `BytesMut`) and [`Encoder<Bytes>`].
/// The maximum payload size is enforced in both directions — oversized
/// frames are rejected with [`io::ErrorKind::InvalidData`].
#[derive(Debug, Clone)]
pub struct LengthPrefixCodec {
    max_size: usize,
}

impl LengthPrefixCodec {
    /// Create a codec with the default max message size ([`MAX_MESSAGE_SIZE`]).
    pub fn new() -> Self {
        Self {
            max_size: MAX_MESSAGE_SIZE,
        }
    }

    /// Create a codec with a custom max message size.
    ///
    /// Useful for testing with smaller limits.
    pub fn with_max_size(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl Default for LengthPrefixCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for LengthPrefixCodec {
    type Item = BytesMut;
    type Error = io::Error;

    /// Decode a single length-prefixed frame from the buffer.
    ///
    /// Returns `Ok(None)` if there isn't enough data yet (the buffer is
    /// automatically reserved for the expected remainder).
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.len() < HEADER_LEN {
            return Ok(None);
        }

        // Peek at the length without advancing the cursor.
        let payload_len =
            u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if payload_len > self.max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frame payload {payload_len} bytes exceeds limit of {} bytes",
                    self.max_size,
                ),
            ));
        }

        let frame_len = HEADER_LEN + payload_len;
        if src.len() < frame_len {
            // Reserve enough space for the rest of the frame to avoid
            // repeated small allocations on partial reads.
            src.reserve(frame_len - src.len());
            return Ok(None);
        }

        // Consume the header.
        src.advance(HEADER_LEN);
        // Split off exactly the payload.
        Ok(Some(src.split_to(payload_len)))
    }
}

impl Encoder<Bytes> for LengthPrefixCodec {
    type Error = io::Error;

    /// Encode a payload into a length-prefixed frame.
    fn encode(&mut self, data: Bytes, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if data.len() > self.max_size {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "frame payload {} bytes exceeds limit of {} bytes",
                    data.len(),
                    self.max_size,
                ),
            ));
        }

        dst.reserve(HEADER_LEN + data.len());
        dst.put_u32(data.len() as u32);
        dst.extend_from_slice(&data);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let mut codec = LengthPrefixCodec::new();
        let payload = Bytes::from(r#"{"jsonrpc":"2.0","method":"health","id":1}"#);

        // Encode.
        let mut buf = BytesMut::new();
        codec.encode(payload.clone(), &mut buf).unwrap();

        // First 4 bytes should be the payload length in big-endian.
        let expected_len = payload.len() as u32;
        assert_eq!(
            &buf[..HEADER_LEN],
            &expected_len.to_be_bytes(),
        );

        // Decode.
        let decoded = codec.decode(&mut buf).unwrap().expect("complete frame");
        assert_eq!(&decoded[..], &payload[..]);
    }

    #[test]
    fn decode_partial_header() {
        let mut codec = LengthPrefixCodec::new();
        let mut buf = BytesMut::from(&[0u8, 0][..]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_partial_payload() {
        let mut codec = LengthPrefixCodec::new();
        let mut buf = BytesMut::new();
        // Header says 100 bytes, but we only provide 10.
        buf.put_u32(100);
        buf.extend_from_slice(&[0u8; 10]);
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn decode_rejects_oversized_frame() {
        let mut codec = LengthPrefixCodec::with_max_size(64);
        let mut buf = BytesMut::new();
        buf.put_u32(128); // exceeds 64-byte limit
        buf.extend_from_slice(&[0u8; 128]);
        let err = codec.decode(&mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn encode_rejects_oversized_payload() {
        let mut codec = LengthPrefixCodec::with_max_size(16);
        let data = Bytes::from(vec![0u8; 32]);
        let mut buf = BytesMut::new();
        let err = codec.encode(data, &mut buf).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn multiple_frames_in_buffer() {
        let mut codec = LengthPrefixCodec::new();
        let mut buf = BytesMut::new();

        let msg_a = Bytes::from("aaa");
        let msg_b = Bytes::from("bbbbb");

        codec.encode(msg_a.clone(), &mut buf).unwrap();
        codec.encode(msg_b.clone(), &mut buf).unwrap();

        let decoded_a = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded_a[..], &msg_a[..]);

        let decoded_b = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(&decoded_b[..], &msg_b[..]);

        // Buffer should be empty now.
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn empty_payload() {
        let mut codec = LengthPrefixCodec::new();
        let mut buf = BytesMut::new();

        codec.encode(Bytes::new(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert!(decoded.is_empty());
    }
}
