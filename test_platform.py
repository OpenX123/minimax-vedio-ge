import http.client
import json
import os
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path

from server import (
    AppError,
    Application,
    KeyPool,
    MiniMaxService,
    PlatformStore,
    RequestHandler,
    TaskStore,
    UpstreamTransportError,
    VideoPlatform,
    official_video_price_fen,
    quote_video_fen,
    video_model_catalog,
    validate_video_request,
)


class FakeProvider:
    def __init__(self, create_result=None, query_result=None, create_error=None):
        self.create_result = create_result or {
            "task_id": "upstream-1",
            "key_id": "deadbeef",
            "key_label": "Key #1",
            "api_version": "v1",
        }
        self.query_result = query_result or {
            "task_id": "upstream-1",
            "status": "succeeded",
            "file_id": None,
            "result_url": "https://example.test/video.mp4",
            "usage": {"output_seconds": 4},
            "error": None,
        }
        self.create_error = create_error
        self.create_calls = []
        self.query_calls = []

    def create_task(self, payload):
        self.create_calls.append(payload)
        if self.create_error:
            raise self.create_error
        return dict(self.create_result)

    def query_task(self, task_id):
        self.query_calls.append(task_id)
        return dict(self.query_result)

    def download_url(self, task_id, file_id=""):
        return "https://example.test/video.mp4"


class PlatformTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        database_path = Path(self.directory.name) / "platform.db"
        self.store = PlatformStore(f"sqlite+pysqlite:///{database_path.as_posix()}")
        self.store.create_schema()
        self.issued = self.store.create_token("验收令牌")
        self.store.recharge_token(self.issued["id"], 2_000, "acceptance-credit")

    def tearDown(self):
        self.store.close()
        self.directory.cleanup()

    def wallet(self):
        token = self.store.get_token(self.issued["id"])
        return {key: token[key] for key in ("balance_fen", "reserved_fen", "available_fen")}

    def test_only_hailuo_23_and_fast_are_available(self):
        hailuo = validate_video_request({
            "model": "MiniMax-Hailuo-2.3",
            "mode": "text",
            "prompt": "A cat looks out of a window.",
            "duration": 6,
            "resolution": "1080P",
        })
        self.assertEqual(hailuo["api_version"], "v1")

        fast = validate_video_request({
            "model": "MiniMax-Hailuo-2.3-Fast",
            "mode": "first_frame",
            "prompt": "The cat turns toward the camera.",
            "duration": 10,
            "resolution": "768P",
            "first_frame_image": "https://example.test/first.png",
            "fast_pretreatment": True,
        })
        self.assertEqual(fast["first_frame_image"], "https://example.test/first.png")
        self.assertTrue(fast["fast_pretreatment"])
        self.assertEqual(
            [item["id"] for item in video_model_catalog()],
            ["MiniMax-Hailuo-2.3", "MiniMax-Hailuo-2.3-Fast"],
        )

        for removed_model in ("MiniMax-H3", "MiniMax-Hailuo-02", "T2V-01"):
            with self.assertRaisesRegex(AppError, "不支持"):
                validate_video_request({
                    "model": removed_model,
                    "mode": "text",
                    "prompt": "invalid",
                    "duration": 6,
                    "resolution": "768P",
                })

    def test_fast_requires_first_frame(self):
        with self.assertRaisesRegex(AppError, "不支持text"):
            validate_video_request({
                "model": "MiniMax-Hailuo-2.3-Fast",
                "mode": "text",
                "prompt": "invalid",
                "duration": 6,
                "resolution": "768P",
            })

    def test_video_prices_are_official_half_price(self):
        expected = {
            ("MiniMax-Hailuo-2.3-Fast", 6, "768P"): (135, 68),
            ("MiniMax-Hailuo-2.3-Fast", 10, "768P"): (225, 113),
            ("MiniMax-Hailuo-2.3-Fast", 6, "1080P"): (231, 116),
            ("MiniMax-Hailuo-2.3", 6, "768P"): (200, 100),
            ("MiniMax-Hailuo-2.3", 10, "768P"): (400, 200),
            ("MiniMax-Hailuo-2.3", 6, "1080P"): (350, 175),
        }
        for options, prices in expected.items():
            self.assertEqual(official_video_price_fen(*options), prices[0])
            self.assertEqual(quote_video_fen(*options), prices[1])

    def test_every_advertised_model_mode_duration_and_resolution_validates(self):
        for model in video_model_catalog():
            if model["api_version"] == "v2":
                for mode in model["modes"]:
                    for duration in model["durations"]:
                        for resolution in model["resolutions"]:
                            payload = {
                                "model": model["id"], "mode": mode, "prompt": "cat",
                                "duration": duration, "resolution": resolution,
                                "ratio": "16:9" if mode == "text" else "adaptive",
                            }
                            if mode in {"first_frame", "first_last"}:
                                payload["first_frame_image"] = "https://example.test/first.png"
                            if mode == "first_last":
                                payload["last_frame_image"] = "https://example.test/last.png"
                            if mode == "reference":
                                payload["references"] = [{"type": "image", "url": "https://example.test/ref.png"}]
                            validate_video_request(payload)
            else:
                for mode, combinations in model["mode_combinations"].items():
                    for option in combinations:
                        payload = {
                            "model": model["id"], "mode": mode, "prompt": "cat",
                            "duration": option["duration"], "resolution": option["resolution"],
                        }
                        if mode in {"first_frame", "first_last"}:
                            payload["first_frame_image"] = "https://example.test/first.png"
                        if mode == "first_last":
                            payload["last_frame_image"] = "https://example.test/last.png"
                        if mode == "subject":
                            payload["references"] = [{"type": "image", "url": "https://example.test/subject.png"}]
                        validate_video_request(payload)

    def test_token_is_returned_once_and_only_hash_is_stored(self):
        raw_key = self.issued["api_key"]
        self.assertTrue(raw_key.startswith("mmx_live_"))
        principal = self.store.authenticate_token(raw_key)
        self.assertEqual(principal["token_id"], self.issued["id"])
        self.assertNotIn(raw_key, self.store.debug_dump())

    def test_create_is_idempotent_and_reserves_once(self):
        provider = FakeProvider()
        platform = VideoPlatform(self.store, provider)
        payload = {
            "model": "MiniMax-Hailuo-2.3",
            "mode": "text",
            "prompt": "A cat looks out of a window.",
            "duration": 6,
            "resolution": "768P",
        }
        first = platform.create_video(self.issued["api_key"], payload, "request-1")
        second = platform.create_video(self.issued["api_key"], payload, "request-1")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len(provider.create_calls), 1)
        self.assertEqual(provider.create_calls[0]["prompt"], "A cat looks out of a window.")
        self.assertEqual(first["quoted_fen"], 100)
        self.assertEqual(self.wallet()["reserved_fen"], 100)
        with self.assertRaisesRegex(AppError, "冻结额度"):
            self.store.delete_token(self.issued["id"])

    def test_success_captures_charge_and_history_is_token_scoped(self):
        provider = FakeProvider()
        platform = VideoPlatform(self.store, provider)
        task = platform.create_video(self.issued["api_key"], {
            "model": "MiniMax-Hailuo-2.3",
            "mode": "text",
            "prompt": "A cat looks out of a window.",
            "duration": 6,
            "resolution": "768P",
        }, "request-success")
        result = platform.get_video(self.issued["api_key"], task["id"], refresh=True)
        self.assertEqual(result["status"], "succeeded")
        self.assertEqual(result["charged_fen"], 100)
        self.assertEqual(result["download_url"], f"/v1/videos/{task['id']}/download")
        self.assertEqual(self.wallet(), {"balance_fen": 1900, "reserved_fen": 0, "available_fen": 1900})
        self.assertEqual([item["id"] for item in platform.list_videos(self.issued["api_key"])["items"]], [task["id"]])

        other_key = self.store.create_token("其他令牌")["api_key"]
        with self.assertRaises(AppError) as raised:
            platform.get_video(other_key, task["id"], refresh=False)
        self.assertEqual(raised.exception.http_status, 404)

    def test_background_poll_settles_without_token_request(self):
        provider = FakeProvider()
        platform = VideoPlatform(self.store, provider)
        task = platform.create_video(self.issued["api_key"], {
            "model": "MiniMax-Hailuo-2.3", "mode": "text", "prompt": "cat", "duration": 6,
            "resolution": "768P",
        }, "background-poll")
        self.assertEqual(platform.poll_pending_once(), 1)
        saved = self.store.get_task(task["id"])
        self.assertEqual((saved["status"], saved["charged_fen"], saved["reserved_fen"]), ("succeeded", 100, 0))

    def test_explicit_rejection_releases_money_but_unknown_submission_keeps_hold(self):
        rejected = VideoPlatform(self.store, FakeProvider(create_error=AppError("参数错误", 400, "UPSTREAM_REJECTED")))
        with self.assertRaises(AppError):
            rejected.create_video(self.issued["api_key"], {
                "model": "MiniMax-Hailuo-2.3",
                "mode": "text",
                "prompt": "cat",
                "duration": 6,
                "resolution": "768P",
            }, "request-rejected")
        self.assertEqual(self.wallet()["reserved_fen"], 0)

        unknown = VideoPlatform(self.store, FakeProvider(create_error=AppError("网络超时", 504, "UPSTREAM_SUBMIT_UNKNOWN")))
        with self.assertRaises(AppError):
            unknown.create_video(self.issued["api_key"], {
                "model": "MiniMax-Hailuo-2.3",
                "mode": "text",
                "prompt": "cat",
                "duration": 6,
                "resolution": "768P",
            }, "request-unknown")
        self.assertEqual(
            self.wallet()["reserved_fen"],
            quote_video_fen("MiniMax-Hailuo-2.3", 6, "768P"),
        )

    def test_token_crud_video_account_and_history(self):
        provider = FakeProvider()
        platform = VideoPlatform(self.store, provider)
        app = Application(provider, platform)
        handler = type("TestRequestHandler", (RequestHandler,), {"app": app})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)

        def request(method, path, body=None, headers=None):
            request_headers = {"Content-Type": "application/json", **(headers or {})}
            connection.request(method, path, json.dumps(body).encode() if body is not None else None, request_headers)
            response = connection.getresponse()
            raw = response.read()
            return response.status, json.loads(raw) if raw else None

        try:
            connection.request("GET", "/")
            root_response = connection.getresponse()
            root_html = root_response.read().decode("utf-8")
            self.assertEqual(root_response.status, 200)
            self.assertIn('data-view="token"', root_html)
            self.assertIn('id="sidebar"', root_html)
            self.assertIn('data-target="create"', root_html)
            self.assertIn('<details class="advanced" open>', root_html)
            self.assertNotIn('id="ratio"', root_html)
            self.assertIn('model.value = "MiniMax-Hailuo-2.3-Fast"', root_html)
            self.assertIn('id="first-frame-drop"', root_html)
            self.assertIn('firstFrameDrop.addEventListener("drop"', root_html)

            connection.request("GET", "/admin")
            admin_response = connection.getresponse()
            admin_html = admin_response.read().decode("utf-8")
            self.assertEqual(admin_response.status, 200)
            self.assertIn('data-view="admin"', admin_html)
            self.assertIn('data-target="keys"', admin_html)
            self.assertIn('data-target="tokens"', admin_html)
            self.assertIn('初始充值（元，1:1）', admin_html)
            self.assertNotIn('充值（分）', admin_html)
            self.assertNotIn('客户', admin_html)

            status, created_token = request("POST", "/api/admin/tokens", {
                "name": "HTTP 令牌", "initial_balance_yuan": 10,
            })
            self.assertEqual(status, 201)
            self.assertIsInstance(created_token["id"], int)
            self.assertGreater(created_token["id"], self.issued["id"])
            token_headers = {"Authorization": "Bearer " + created_token["api_key"]}

            status, tokens = request("GET", "/api/admin/tokens")
            self.assertEqual(status, 200)
            self.assertIn(created_token["id"], [item["id"] for item in tokens["items"]])
            self.assertNotIn("api_key", tokens["items"][-1])
            status, saved = request("PATCH", f"/api/admin/tokens/{created_token['id']}", {
                "name": "HTTP 令牌（已修改）", "enabled": False,
            })
            self.assertEqual((status, saved["name"], saved["enabled"]), (200, "HTTP 令牌（已修改）", False))
            status, error = request("GET", "/v1/account", headers=token_headers)
            self.assertEqual((status, error["error"]["code"]), (401, "INVALID_TOKEN"))
            status, saved = request("PATCH", f"/api/admin/tokens/{created_token['id']}", {"enabled": True})
            self.assertEqual((status, saved["enabled"]), (200, True))

            status, video = request("POST", "/v1/videos", {
                "model": "MiniMax-Hailuo-2.3", "mode": "text", "prompt": "cat", "duration": 6,
                "resolution": "768P",
            }, {**token_headers, "Idempotency-Key": "http-request-1"})
            self.assertEqual(status, 202)
            status, account = request("GET", "/v1/account", headers=token_headers)
            self.assertEqual(status, 200)
            self.assertEqual(account["token_id"], created_token["id"])
            self.assertEqual({key: account[key] for key in ("balance_fen", "reserved_fen", "available_fen")}, {
                "balance_fen": 1_000, "reserved_fen": 100, "available_fen": 900,
            })
            status, wallet = request("POST", f"/api/admin/tokens/{created_token['id']}/recharge", {
                "amount_yuan": 1.25,
            })
            self.assertEqual((status, wallet["balance_fen"]), (200, 1_125))
            status, error = request("POST", f"/api/admin/tokens/{created_token['id']}/recharge", {
                "amount_yuan": 1.001,
            })
            self.assertEqual((status, error["error"]["code"]), (400, "INVALID_AMOUNT"))
            status, history = request("GET", "/v1/videos", headers=token_headers)
            self.assertEqual((status, history["items"][0]["id"]), (200, video["id"]))

            status, disposable = request("POST", "/api/admin/tokens", {"name": "待删除令牌", "initial_balance_yuan": 0})
            self.assertEqual(status, 201)
            self.assertGreater(disposable["id"], created_token["id"])
            disposable_headers = {"Authorization": "Bearer " + disposable["api_key"]}
            status, deleted = request("DELETE", f"/api/admin/tokens/{disposable['id']}")
            self.assertEqual((status, deleted), (200, {"id": disposable["id"], "deleted": True}))
            status, error = request("GET", f"/api/admin/tokens/{disposable['id']}")
            self.assertEqual((status, error["error"]["code"]), (404, "TOKEN_NOT_FOUND"))
            status, error = request("GET", "/v1/account", headers=disposable_headers)
            self.assertEqual((status, error["error"]["code"]), (401, "INVALID_TOKEN"))
        finally:
            connection.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_provider_keys_are_managed_in_database_without_api_leakage(self):
        task_store = TaskStore(Path(self.directory.name) / "provider-tasks.json")
        service = MiniMaxService(KeyPool([]), task_store)
        app = Application(service, VideoPlatform(self.store, service))
        handler = type("ProviderKeyRequestHandler", (RequestHandler,), {"app": app})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        connection = http.client.HTTPConnection("127.0.0.1", server.server_port, timeout=5)

        def request(method, path, body=None):
            connection.request(
                method,
                path,
                json.dumps(body).encode() if body is not None else None,
                {"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            raw = response.read()
            return response.status, json.loads(raw) if raw else None

        try:
            first_secret = "fixture-minimax-provider-key-1"
            status, created = request("POST", "/api/admin/keys", {
                "label": "主上游", "api_key": first_secret,
            })
            self.assertEqual(status, 201)
            self.assertNotIn(first_secret, json.dumps(created))
            self.assertNotIn("secret_value", created)
            self.assertEqual(self.store.load_provider_keys()[0]["secret_value"], first_secret)
            self.assertEqual(service.key_pool.candidates()[0].value, first_secret)

            status, keys = request("GET", "/api/admin/keys")
            self.assertEqual((status, keys["configured_keys"], keys["keys"][0]["label"]), (200, 1, "主上游"))
            self.assertNotIn(first_secret, json.dumps(keys))

            replacement = "fixture-minimax-provider-key-2"
            status, updated = request("PATCH", f"/api/admin/keys/{created['id']}", {
                "label": "备用上游", "enabled": False, "api_key": replacement,
            })
            self.assertEqual((status, updated["label"], updated["enabled"]), (200, "备用上游", False))
            self.assertEqual(updated["key_id"], created["key_id"])
            self.assertEqual(service.key_pool.get(created["key_id"]).value, replacement)
            self.assertEqual(service.key_pool.candidates(), [])
            self.assertNotIn(replacement, self.store.debug_dump())

            task_store.upsert({"task_id": "bound-task", "key_id": created["key_id"], "status": "Preparing"})
            status, error = request("DELETE", f"/api/admin/keys/{created['id']}")
            self.assertEqual((status, error["error"]["code"]), (409, "PROVIDER_KEY_IN_USE"))

            disposable_secret = "fixture-minimax-provider-key-disposable"
            status, disposable = request("POST", "/api/admin/keys", {
                "label": "待删除", "api_key": disposable_secret,
            })
            self.assertEqual(status, 201)
            status, deleted = request("DELETE", f"/api/admin/keys/{disposable['id']}")
            self.assertEqual((status, deleted["deleted"]), (200, True))
            self.assertIsNone(service.key_pool.get(disposable["key_id"]))
        finally:
            connection.close()
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_provider_key_environment_seed_runs_only_once(self):
        self.assertEqual(self.store.seed_provider_keys(["fixture-seed-key"]), 1)
        provider_key = self.store.list_provider_keys()[0]
        self.store.delete_provider_key(provider_key["id"])
        self.assertEqual(self.store.seed_provider_keys(["fixture-key-that-must-not-return"]), 0)
        self.assertEqual(self.store.list_provider_keys(), [])


@unittest.skipUnless(os.environ.get("MINIMAX_TEST_DATABASE_URL"), "set MINIMAX_TEST_DATABASE_URL for PostgreSQL integration")
class PostgreSQLPlatformTests(unittest.TestCase):
    def test_postgresql_token_reserve_capture_and_history(self):
        store = PlatformStore(os.environ["MINIMAX_TEST_DATABASE_URL"])
        store.metadata.drop_all(store.engine)
        store.create_schema()
        try:
            issued = store.create_token("PostgreSQL 验收令牌")
            store.recharge_token(issued["id"], 1_000, "postgres-acceptance-credit")
            platform = VideoPlatform(store, FakeProvider())
            task = platform.create_video(issued["api_key"], {
                "model": "MiniMax-Hailuo-2.3", "mode": "text", "prompt": "cat", "duration": 6,
                "resolution": "768P",
            }, "postgres-request-1")
            completed = platform.get_video(issued["api_key"], task["id"], refresh=True)
            self.assertEqual((completed["status"], completed["charged_fen"]), ("succeeded", 100))
            token = store.get_token(issued["id"])
            self.assertEqual({key: token[key] for key in ("balance_fen", "reserved_fen", "available_fen")}, {
                "balance_fen": 900, "reserved_fen": 0, "available_fen": 900,
            })
            self.assertEqual(platform.list_videos(issued["api_key"])["items"][0]["id"], task["id"])
            self.assertNotIn(issued["api_key"], store.debug_dump())
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
