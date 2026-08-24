import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from server import (
    AppError,
    KeyPool,
    MiniMaxService,
    RequestHandler,
    TaskStore,
    UpstreamError,
    UpstreamTransportError,
    key_id_for,
    load_dotenv_key_values,
    quota_day_token,
    parse_key_values,
    validate_generation_payload,
)


class FakeTransport:
    def __init__(self, actions):
        self.actions = list(actions)
        self.calls = []

    def request_json(self, key, method, path, payload=None):
        self.calls.append((key, method, path, payload))
        action = self.actions.pop(0)
        if isinstance(action, Exception):
            raise action
        return action


class MiniMaxServerTests(unittest.TestCase):
    def test_log_message_handles_request_without_path(self):
        handler = object.__new__(RequestHandler)
        handler.command = "GET"
        handler.log_date_time_string = lambda: "now"

        with patch("builtins.print") as output:
            handler.log_message("%s", "bad request")

        output.assert_called_once_with("[now] GET - - bad request")

    def test_admin_page_is_public_but_api_requires_configured_token(self):
        handler = object.__new__(RequestHandler)
        handler.path = "/admin"
        handler.client_address = ("203.0.113.10", 1234)
        response = {}
        handler._send_json = lambda status, payload: response.update(status=status, payload=payload)
        handler._serve_index = lambda view: response.update(status=200, view=view)

        handler.do_GET()

        self.assertEqual((response["status"], response["view"]), (200, "admin"))

        handler.headers = {}
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(AppError) as missing:
                handler._require_admin()
        self.assertEqual((missing.exception.http_status, missing.exception.code), (503, "ADMIN_TOKEN_NOT_CONFIGURED"))

        with patch.dict(os.environ, {"MINIMAX_ADMIN_TOKEN": "expected"}, clear=True):
            with self.assertRaises(AppError) as invalid:
                handler._require_admin()
            self.assertEqual((invalid.exception.http_status, invalid.exception.code), (401, "INVALID_ADMIN_TOKEN"))
            handler.headers = {"X-Admin-Token": "expected"}
            handler._require_admin()

    def test_legacy_api_guard_blocks_remote_clients(self):
        handler = object.__new__(RequestHandler)
        handler.client_address = ("203.0.113.10", 1234)
        with self.assertRaises(AppError) as raised:
            handler._require_legacy_local()
        self.assertEqual((raised.exception.http_status, raised.exception.code), (403, "LEGACY_LOCAL_ONLY"))

        handler.client_address = ("127.0.0.1", 1234)
        handler._require_legacy_local()

    def test_parse_keys_deduplicates_and_supports_comma_or_newline(self):
        self.assertEqual(parse_key_values(" a,b\n b, c "), ["a", "b", "c"])

    def test_key_id_is_stable_without_containing_secret(self):
        secret = "fixture-secret-value"
        identifier = key_id_for(secret)
        self.assertEqual(identifier, key_id_for(secret))
        self.assertNotIn(secret, identifier)
        self.assertEqual(len(identifier), 8)

    def test_validate_generation_payload(self):
        payload = validate_generation_payload({
            "model": "MiniMax-Hailuo-2.3",
            "prompt": "  cat  ",
            "duration": 6,
            "resolution": "768P",
        })
        self.assertEqual(payload["prompt"], "cat")
        self.assertTrue(payload["prompt_optimizer"])

        with self.assertRaises(AppError):
            validate_generation_payload({
                "model": "MiniMax-Hailuo-2.3",
                "prompt": "cat",
                "duration": 10,
                "resolution": "1080P",
            })

    def test_create_fails_over_only_on_explicit_retryable_error(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TaskStore(Path(directory) / "tasks.json")
            pool = KeyPool(["first", "second"])
            transport = FakeTransport([
                UpstreamError("auth", http_status=401, minimax_code=1004, retryable_create=True, category="auth"),
                {"task_id": "task-1", "base_resp": {"status_code": 0}},
            ])
            service = MiniMaxService(pool, store, transport)
            result = service.create_task({
                "model": "MiniMax-Hailuo-2.3",
                "prompt": "cat",
                "duration": 6,
                "resolution": "768P",
            })
            self.assertEqual(result["task_id"], "task-1")
            self.assertEqual(len(transport.calls), 2)
            self.assertEqual(transport.calls[0][0], "first")
            self.assertEqual(transport.calls[1][0], "second")

    def test_transport_failure_is_not_retried(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TaskStore(Path(directory) / "tasks.json")
            pool = KeyPool(["first", "second"])
            transport = FakeTransport([UpstreamTransportError()])
            service = MiniMaxService(pool, store, transport)
            with self.assertRaises(AppError):
                service.create_task({
                    "model": "MiniMax-Hailuo-2.3",
                    "prompt": "cat",
                    "duration": 6,
                    "resolution": "768P",
                })
            self.assertEqual(len(transport.calls), 1)

    def test_query_uses_the_key_bound_at_creation(self):
        with tempfile.TemporaryDirectory() as directory:
            task_path = Path(directory) / "tasks.json"
            store = TaskStore(task_path)
            pool = KeyPool(["first", "second"])
            transport = FakeTransport([
                {"task_id": "task-1", "base_resp": {"status_code": 0}},
                {
                    "task_id": "task-1",
                    "status": "Success",
                    "file_id": "file-1",
                    "video_width": 1280,
                    "video_height": 768,
                    "base_resp": {"status_code": 0},
                },
            ])
            service = MiniMaxService(pool, store, transport)
            service.create_task({
                "model": "MiniMax-Hailuo-2.3",
                "prompt": "cat",
                "duration": 6,
                "resolution": "768P",
            })
            result = service.query_task("task-1")
            self.assertEqual(result["file_id"], "file-1")
            self.assertEqual(transport.calls[0][0], transport.calls[1][0])
            saved = json.loads(task_path.read_text(encoding="utf-8"))
            self.assertNotIn("first", task_path.read_text(encoding="utf-8"))
            self.assertEqual(saved["tasks"]["task-1"]["file_id"], "file-1")

    def test_fast_uses_v1_create_query_and_download(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TaskStore(Path(directory) / "tasks.json")
            transport = FakeTransport([
                {"task_id": "fast-task-1", "base_resp": {"status_code": 0}},
                {
                    "task_id": "fast-task-1",
                    "status": "Success",
                    "file_id": "fast-file-1",
                    "base_resp": {"status_code": 0},
                },
                {"file": {"download_url": "https://example.test/fast.mp4"}},
            ])
            service = MiniMaxService(KeyPool(["token-plan-key"]), store, transport)
            created = service.create_task({
                "model": "MiniMax-Hailuo-2.3-Fast",
                "mode": "first_frame",
                "prompt": "cat",
                "duration": 6,
                "resolution": "1080P",
                "first_frame_image": "https://example.test/first.png",
                "fast_pretreatment": True,
            })
            result = service.query_task(created["task_id"])
            self.assertEqual(transport.calls[0][2], "/v1/video_generation")
            self.assertNotIn("api_version", transport.calls[0][3])
            self.assertTrue(transport.calls[0][3]["fast_pretreatment"])
            self.assertEqual(transport.calls[1][2], "/v1/query/video_generation?task_id=fast-task-1")
            self.assertEqual(result["file_id"], "fast-file-1")
            self.assertEqual(service.download_url("fast-task-1", "fast-file-1"), "https://example.test/fast.mp4")

    def test_removed_h3_is_rejected_before_upstream(self):
        with tempfile.TemporaryDirectory() as directory:
            transport = FakeTransport([])
            service = MiniMaxService(
                KeyPool(["token-plan-key"]),
                TaskStore(Path(directory) / "tasks.json"),
                transport,
                h3_key_pool=KeyPool(["paygo-key"]),
            )
            with self.assertRaisesRegex(AppError, "不支持"):
                service.create_task({
                    "model": "MiniMax-H3", "mode": "text", "prompt": "cat",
                    "duration": 4, "resolution": "768P", "ratio": "16:9",
                })
            self.assertEqual(transport.calls, [])

    def test_missing_bound_key_does_not_switch_accounts(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TaskStore(Path(directory) / "tasks.json")
            store.upsert({
                "task_id": "task-1",
                "key_id": key_id_for("removed"),
                "key_label": "Key #1",
                "status": "Processing",
                "file_id": None,
            })
            service = MiniMaxService(KeyPool(["other"]), store, FakeTransport([]))
            with self.assertRaisesRegex(AppError, "无法安全切换账号查询"):
                service.query_task("task-1")

    def test_tasks_are_written_atomically_without_raw_keys(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tasks.json"
            store = TaskStore(path)
            store.upsert({
                "task_id": "task-1",
                "key_id": key_id_for("secret"),
                "key_label": "Key #1",
                "status": "Preparing",
                "file_id": None,
            })
            contents = path.read_text(encoding="utf-8")
            self.assertNotIn("secret", contents)
            self.assertEqual(list(Path(directory).glob("*.tmp")), [])

    def test_environment_pool_takes_precedence(self):
        with patch.dict(os.environ, {"MINIMAX_API_KEYS": "one,two", "MINIMAX_API_KEY": "fallback"}, clear=False):
            pool = KeyPool.from_environment()
        self.assertEqual(pool.status()["configured_keys"], 2)

    def test_dotenv_supports_one_bare_key_per_line(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("first\n# comment\nsecond\nMINIMAX_API_KEYS=third,fourth\n", encoding="utf-8")
            self.assertEqual(load_dotenv_key_values(path), ["first", "second", "third", "fourth"])

    def test_daily_quota_skips_exhausted_key_and_reports_remaining(self):
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"MINIMAX_DAILY_KEY_QUOTA": "3"}, clear=False):
            store = TaskStore(Path(directory) / "tasks.json")
            pool = KeyPool(["first", "second"])
            first = pool.get(key_id_for("first"))
            self.assertIsNotNone(first)
            for index in range(3):
                store.upsert({
                    "task_id": f"old-{index}",
                    "key_id": first.key_id,
                    "key_label": first.label,
                    "status": "Success",
                    "file_id": f"file-{index}",
                    "created_at": "2026-08-19T00:00:00+00:00",
                    "quota_day": quota_day_token(),
                })
            service = MiniMaxService(pool, store, FakeTransport([]))
            status = service.status()
            first_status = next(item for item in status["keys"] if item["key_id"] == first.key_id)
            self.assertEqual(first_status["used_today"], 3)
            self.assertEqual(first_status["remaining_today"], 0)
            self.assertEqual(first_status["state"], "quota_exhausted")

    def test_history_contains_prompt_and_preview_url(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TaskStore(Path(directory) / "tasks.json")
            pool = KeyPool(["first"])
            transport = FakeTransport([
                {"task_id": "task-history", "base_resp": {"status_code": 0}},
                {
                    "task_id": "task-history",
                    "status": "Success",
                    "file_id": "file-history",
                    "base_resp": {"status_code": 0},
                },
            ])
            service = MiniMaxService(pool, store, transport)
            service.create_task({
                "model": "MiniMax-Hailuo-2.3",
                "prompt": "history cat",
                "duration": 6,
                "resolution": "768P",
            })
            service.query_task("task-history")
            item = service.history()["items"][0]
            self.assertEqual(item["prompt"], "history cat")
            self.assertIn("task-history", item["preview_url"])


if __name__ == "__main__":
    unittest.main()
