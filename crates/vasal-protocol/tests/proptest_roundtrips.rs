//! Property-based tests for protocol type serialization roundtrips.
//!
//! Every protocol type must survive a serialize → deserialize cycle without
//! data loss. These tests use proptest to generate arbitrary instances and
//! verify this invariant.

use proptest::prelude::*;
use vasal_protocol::error::ProtocolError;
use vasal_protocol::jsonrpc::{ErrorObject, Request, RequestId, Response};
use vasal_protocol::task::*;

// ── Strategies ─────────────────────────────────────────────────────────────

fn arb_priority() -> impl Strategy<Value = Priority> {
    prop_oneof![
        Just(Priority::Critical),
        Just(Priority::High),
        Just(Priority::Normal),
        Just(Priority::Low),
    ]
}

fn arb_executor() -> impl Strategy<Value = Executor> {
    prop_oneof![Just(Executor::Shell), Just(Executor::Sidecar),]
}

fn arb_exec_kind() -> impl Strategy<Value = ExecKind> {
    prop_oneof![Just(ExecKind::Oneshot), Just(ExecKind::Continuous),]
}

fn arb_status() -> impl Strategy<Value = TaskResultStatus> {
    prop_oneof![
        Just(TaskResultStatus::Success),
        Just(TaskResultStatus::Failed),
        Just(TaskResultStatus::Cancelled),
        Just(TaskResultStatus::Timeout),
        Just(TaskResultStatus::RolledBack),
    ]
}

fn arb_uuid() -> impl Strategy<Value = uuid::Uuid> {
    (any::<u128>()).prop_map(uuid::Uuid::from_u128)
}

fn arb_tags() -> impl Strategy<Value = std::collections::HashMap<String, String>> {
    proptest::collection::hash_map("[a-z]{1,8}", "[a-z0-9]{1,16}", 0..4)
}

fn arb_request_id() -> impl Strategy<Value = RequestId> {
    prop_oneof![
        (1i64..10000).prop_map(RequestId::Integer),
        "[a-z]{1,8}".prop_map(|s| RequestId::String(s)),
    ]
}

// ── Tests ──────────────────────────────────────────────────────────────────

proptest! {
    #[test]
    fn task_result_roundtrip(
        task_id in arb_uuid(),
        chain_id in proptest::option::of(arb_uuid()),
        step_index in proptest::option::of(0u32..100),
        status in arb_status(),
        exit_code in proptest::option::of(-128i32..128),
        stdout in "[a-z ]{0,64}",
        stderr in "[a-z ]{0,64}",
        duration_ms in 0u64..1_000_000,
        timestamp in 0u64..u64::MAX,
        error in proptest::option::of("[a-z ]{0,32}"),
    ) {
        let original = TaskResult {
            task_id,
            chain_id,
            step_index,
            status,
            exit_code,
            stdout,
            stderr,
            duration_ms,
            timestamp,
            error,
        };
        let json = serde_json::to_string(&original).unwrap();
        let recovered: TaskResult = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(original, recovered);
    }

    #[test]
    fn exec_task_roundtrip(
        id in arb_uuid(),
        priority in arb_priority(),
        kind in arb_exec_kind(),
        executor in arb_executor(),
        timeout_ms in 1u64..600_000,
        tags in arb_tags(),
    ) {
        let task = Task::Exec(ExecTask {
            id,
            priority,
            tags,
            kind,
            executor,
            target: None,
            method: None,
            payload: serde_json::json!({"script": "echo test"}),
            interval_ms: None,
            timeout_ms,
            credentials: vec![],
        });
        let json = serde_json::to_string(&task).unwrap();
        let recovered: Task = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(task, recovered);
    }

    #[test]
    fn jsonrpc_request_roundtrip(
        method in "[a-z_]{1,16}",
        id in arb_request_id(),
    ) {
        let req = Request::new(method, None, id);
        let json = serde_json::to_string(&req).unwrap();
        let recovered: Request = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(req, recovered);
    }

    #[test]
    fn jsonrpc_response_success_roundtrip(
        id in arb_request_id(),
    ) {
        let resp = Response::success(id, serde_json::json!({"key": "value"}));
        let json = serde_json::to_string(&resp).unwrap();
        let recovered: Response = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(resp, recovered);
    }

    #[test]
    fn jsonrpc_response_error_roundtrip(
        id in arb_request_id(),
        code in -32700i32..-32000,
        message in "[a-z ]{1,32}",
    ) {
        let resp = Response::error(id, ErrorObject {
            code,
            message,
            data: None,
        });
        let json = serde_json::to_string(&resp).unwrap();
        let recovered: Response = serde_json::from_str(&json).unwrap();
        prop_assert_eq!(resp, recovered);
    }

    #[test]
    fn protocol_error_roundtrip(
        code in -32700i32..-32000,
        message in "[a-z ]{1,32}",
    ) {
        let original = ProtocolError::new(code, message);
        let obj: ErrorObject = original.clone().into();
        let recovered: ProtocolError = obj.into();
        prop_assert_eq!(original.code, recovered.code);
        prop_assert_eq!(original.message, recovered.message);
    }
}
