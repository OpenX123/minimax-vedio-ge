from __future__ import annotations

import hashlib
import json
import math
import mimetypes
import os
import re
import secrets
import socket
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote, unquote, urlencode, urlparse
from urllib.request import Request, urlopen

from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    delete,
    insert,
    select,
    update,
)


API_BASE_URL = "https://api.minimaxi.com"
TASKS_PATH = Path(__file__).with_name("tasks.json")
ENV_PATH = Path(__file__).with_name(".env")
PLATFORM_DB_PATH = Path(__file__).with_name("platform.db")
MAX_REQUEST_BYTES = 40 * 1024 * 1024
BEIJING_TZ = timezone(timedelta(hours=8), "Asia/Shanghai")
DEFAULT_DAILY_KEY_QUOTA = 3
POLLABLE_STATUSES = {"preparing", "queueing", "processing", "queued", "running", "submitted"}
SUCCESS_STATUSES = {"success", "succeeded"}
FAIL_STATUSES = {"fail", "failed", "cancelled"}
RETRYABLE_CREATE_HTTP_STATUSES = {401, 402, 429}
RETRYABLE_CREATE_CODES = {1002, 1004, 1008, 2049}

MODEL_COMBINATIONS: dict[str, set[tuple[int, str]]] = {
    "MiniMax-Hailuo-2.3": {(6, "768P"), (10, "768P"), (6, "1080P")},
    "MiniMax-Hailuo-02": {(6, "768P"), (10, "768P"), (6, "1080P")},
    "T2V-01-Director": {(6, "720P")},
    "T2V-01": {(6, "720P")},
}

H3_RATIOS = {"adaptive", "21:9", "16:9", "4:3", "1:1", "3:4", "9:16"}
VIDEO_CAPABILITIES: dict[str, dict[str, Any]] = {
    "MiniMax-H3": {
        "api_version": "v2",
        "modes": {"text", "first_frame", "first_last", "reference"},
        "resolutions": {"768P", "2K"},
    },
    "MiniMax-Hailuo-2.3": {
        "api_version": "v1",
        "modes": {"text", "first_frame"},
        "combinations": {(6, "768P"), (10, "768P"), (6, "1080P")},
    },
    "MiniMax-Hailuo-2.3-Fast": {
        "api_version": "v1",
        "modes": {"first_frame"},
        "combinations": {(6, "768P"), (10, "768P"), (6, "1080P")},
    },
    "MiniMax-Hailuo-02": {
        "api_version": "v1",
        "modes": {"text", "first_frame", "first_last"},
        "combinations": {
            (6, "512P"), (10, "512P"), (6, "768P"),
            (10, "768P"), (6, "1080P"),
        },
        "mode_combinations": {
            "text": {(6, "768P"), (10, "768P"), (6, "1080P")},
            "first_frame": {
                (6, "512P"), (10, "512P"), (6, "768P"),
                (10, "768P"), (6, "1080P"),
            },
            "first_last": {(6, "768P"), (10, "768P"), (6, "1080P")},
        },
    },
    "T2V-01-Director": {"api_version": "v1", "modes": {"text"}, "combinations": {(6, "720P")}},
    "T2V-01": {"api_version": "v1", "modes": {"text"}, "combinations": {(6, "720P")}},
    "I2V-01-Director": {"api_version": "v1", "modes": {"first_frame"}, "combinations": {(6, "720P")}},
    "I2V-01-live": {"api_version": "v1", "modes": {"first_frame"}, "combinations": {(6, "720P")}},
    "I2V-01": {"api_version": "v1", "modes": {"first_frame"}, "combinations": {(6, "720P")}},
    "S2V-01": {"api_version": "v1", "modes": {"subject"}, "combinations": {(6, "720P")}},
}
ACTIVE_VIDEO_MODELS = ("MiniMax-Hailuo-2.3", "MiniMax-Hailuo-2.3-Fast")
OFFICIAL_VIDEO_PRICES_FEN = {
    ("MiniMax-Hailuo-2.3-Fast", 6, "768P"): 135,
    ("MiniMax-Hailuo-2.3-Fast", 10, "768P"): 225,
    ("MiniMax-Hailuo-2.3-Fast", 6, "1080P"): 231,
    ("MiniMax-Hailuo-2.3", 6, "768P"): 200,
    ("MiniMax-Hailuo-2.3", 10, "768P"): 400,
    ("MiniMax-Hailuo-2.3", 6, "1080P"): 350,
}


class AppError(Exception):
    def __init__(self, message: str, http_status: int = 400, code: str = "BAD_REQUEST"):
        super().__init__(message)
        self.message = message
        self.http_status = http_status
        self.code = code


class UpstreamError(AppError):
    def __init__(
        self,
        message: str,
        *,
        http_status: int | None = None,
        minimax_code: int | None = None,
        retryable_create: bool = False,
        category: str = "upstream",
        has_task_id: bool = False,
    ):
        super().__init__(message, http_status or 502, "UPSTREAM_ERROR")
        self.http_status_from_upstream = http_status
        self.minimax_code = minimax_code
        self.retryable_create = retryable_create
        self.category = category
        self.has_task_id = has_task_id


class UpstreamTransportError(UpstreamError):
    def __init__(self, message: str = "MiniMax 网络请求失败"):
        super().__init__(
            message,
            http_status=504,
            category="transport",
        )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def quota_day_token(moment: datetime | None = None) -> str:
    moment = moment or datetime.now(timezone.utc)
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=timezone.utc)
    return moment.astimezone(BEIJING_TZ).date().isoformat()


def daily_key_quota() -> int:
    try:
        value = int(os.environ.get("MINIMAX_DAILY_KEY_QUOTA", str(DEFAULT_DAILY_KEY_QUOTA)))
    except ValueError:
        value = DEFAULT_DAILY_KEY_QUOTA
    return max(1, value)


def key_id_for(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:8]


def parse_key_values(raw: str | None) -> list[str]:
    if not raw:
        return []
    values = re.split(r"[,\r\n]+", raw)
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = value.strip()
        if value and value not in seen:
            result.append(value)
            seen.add(value)
    return result


def load_dotenv_key_values(path: Path = ENV_PATH) -> list[str]:
    """Load only MiniMax keys from .env, including one-bare-key-per-line files."""
    if not path.exists():
        return []
    configured: list[str] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise AppError(f"无法读取 .env：{exc}", 500, "ENV_READ_FAILED") from exc

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        if "=" not in line:
            configured.extend(parse_key_values(line))
            continue
        name, value = line.split("=", 1)
        if name.strip() not in {"MINIMAX_API_KEYS", "MINIMAX_API_KEY"}:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        configured.extend(parse_key_values(value))
    return parse_key_values(",".join(configured))


def load_dotenv_named_values(names: set[str], path: Path = ENV_PATH) -> list[str]:
    if not path.exists():
        return []
    configured: list[str] = []
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.lower().startswith("export "):
            line = line[7:].lstrip()
        name, value = line.split("=", 1)
        if name.strip() not in names:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        configured.extend(parse_key_values(value))
    return parse_key_values(",".join(configured))


@dataclass(frozen=True)
class KeyInfo:
    key_id: str
    value: str = field(repr=False)
    index: int = 0
    name: str = ""
    enabled: bool = True

    @property
    def label(self) -> str:
        return self.name or f"Key #{self.index + 1}"


class KeyPool:
    def __init__(self, values: list[str]):
        self._lock = threading.Lock()
        self._infos = [KeyInfo(key_id_for(value), value, index) for index, value in enumerate(values)]
        self._by_id = {info.key_id: info for info in self._infos}
        self._next_index = 0
        self._unavailable_until: dict[str, float] = {}

    @classmethod
    def from_environment(cls) -> "KeyPool":
        if "MINIMAX_API_KEYS" in os.environ:
            values = parse_key_values(os.environ.get("MINIMAX_API_KEYS"))
        elif "MINIMAX_API_KEY" in os.environ:
            values = parse_key_values(os.environ.get("MINIMAX_API_KEY"))
        else:
            values = load_dotenv_key_values()
        if not values:
            raise AppError(
                "未配置 MiniMax API Key，请设置 MINIMAX_API_KEYS 或 MINIMAX_API_KEY",
                500,
                "KEYS_NOT_CONFIGURED",
            )
        return cls(values)

    @classmethod
    def from_records(cls, records: list[dict[str, Any]]) -> "KeyPool":
        pool = cls([])
        pool._infos = [
            KeyInfo(
                str(record["key_id"]),
                str(record["secret_value"]),
                index,
                str(record.get("label") or ""),
                bool(record.get("enabled")),
            )
            for index, record in enumerate(records)
        ]
        pool._by_id = {info.key_id: info for info in pool._infos}
        return pool

    @classmethod
    def optional_from_environment(cls, multi_name: str, single_name: str) -> "KeyPool | None":
        if multi_name in os.environ:
            values = parse_key_values(os.environ.get(multi_name))
        elif single_name in os.environ:
            values = parse_key_values(os.environ.get(single_name))
        else:
            values = load_dotenv_named_values({multi_name, single_name})
        return cls(values) if values else None

    def candidates(self) -> list[KeyInfo]:
        with self._lock:
            if not self._infos:
                return []
            now = time.monotonic()
            start = self._next_index
            result: list[KeyInfo] = []
            for offset in range(len(self._infos)):
                info = self._infos[(start + offset) % len(self._infos)]
                if not info.enabled:
                    continue
                blocked_until = self._unavailable_until.get(info.key_id, 0)
                if blocked_until == math.inf or blocked_until > now:
                    continue
                result.append(info)
            self._next_index = (start + 1) % len(self._infos)
            return result

    def get(self, key_id: str) -> KeyInfo | None:
        return self._by_id.get(key_id)

    def infos(self) -> list[KeyInfo]:
        return list(self._infos)

    def availability(self, key_id: str) -> str:
        with self._lock:
            info = self._by_id.get(key_id)
            if info is not None and not info.enabled:
                return "disabled"
            blocked_until = self._unavailable_until.get(key_id, 0)
            if blocked_until == math.inf:
                return "disabled"
            if blocked_until > time.monotonic():
                return "cooldown"
            return "ready"

    def mark_unavailable(self, info: KeyInfo, category: str) -> None:
        if category == "auth":
            duration = math.inf
        elif category == "balance":
            duration = 300
        elif category == "rate_limit":
            duration = 30
        else:
            duration = 60
        with self._lock:
            self._unavailable_until[info.key_id] = math.inf if duration == math.inf else time.monotonic() + duration

    def label_for(self, key_id: str, fallback: str | None = None) -> str:
        info = self.get(key_id)
        return info.label if info else (fallback or f"key-{key_id}")

    def status(self) -> dict[str, int]:
        with self._lock:
            now = time.monotonic()
            available = sum(
                1
                for info in self._infos
                if info.enabled
                and self._unavailable_until.get(info.key_id, 0) != math.inf
                and self._unavailable_until.get(info.key_id, 0) <= now
            )
            return {"configured_keys": len(self._infos), "available_keys": available}


class TaskStore:
    def __init__(self, path: Path = TASKS_PATH):
        self.path = path
        self._lock = threading.Lock()
        self._tasks: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            tasks = data.get("tasks", {})
            if not isinstance(tasks, dict):
                raise ValueError("tasks must be an object")
            self._tasks = {str(task_id): dict(record) for task_id, record in tasks.items()}
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            raise AppError(f"任务绑定文件无法读取：{exc}", 500, "TASK_STORE_INVALID") from exc

    def get(self, task_id: str) -> dict[str, Any] | None:
        with self._lock:
            value = self._tasks.get(task_id)
            return dict(value) if value else None

    def upsert(self, record: dict[str, Any]) -> None:
        task_id = str(record["task_id"])
        with self._lock:
            self._tasks[task_id] = dict(record)
            self._save_locked()

    def update(self, task_id: str, **changes: Any) -> dict[str, Any] | None:
        with self._lock:
            if task_id not in self._tasks:
                return None
            self._tasks[task_id].update(changes)
            self._save_locked()
            return dict(self._tasks[task_id])

    def list(self, limit: int = 50) -> list[dict[str, Any]]:
        with self._lock:
            records = [dict(record) for record in self._tasks.values()]
        records.sort(key=lambda record: str(record.get("created_at", "")), reverse=True)
        return records[:limit]

    def count(self) -> int:
        with self._lock:
            return len(self._tasks)

    def references_key(self, key_id: str) -> bool:
        with self._lock:
            return any(record.get("key_id") == key_id for record in self._tasks.values())

    def daily_usage(self, day: str) -> dict[str, int]:
        usage: dict[str, int] = {}
        with self._lock:
            records = list(self._tasks.values())
        for record in records:
            record_day = record.get("quota_day")
            if not record_day:
                try:
                    created_at = datetime.fromisoformat(str(record.get("created_at")))
                    record_day = quota_day_token(created_at)
                except (TypeError, ValueError):
                    record_day = None
            if record_day == day:
                key_id = record.get("key_id")
                if key_id:
                    usage[key_id] = usage.get(key_id, 0) + 1
        return usage

    def _save_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=self.path.parent,
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                delete=False,
            ) as handle:
                temp_path = handle.name
                json.dump({"tasks": self._tasks}, handle, ensure_ascii=False, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
            temp_path = None
        finally:
            if temp_path:
                try:
                    os.unlink(temp_path)
                except FileNotFoundError:
                    pass


def _require_media_url(value: Any, field_name: str, *, image: bool = False) -> str:
    if not isinstance(value, str) or not value.strip():
        raise AppError(f"{field_name} 不能为空", 400, "MEDIA_URL_REQUIRED")
    value = value.strip()
    parsed = urlparse(value)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return value
    if image and value.startswith(("data:image/jpeg;base64,", "data:image/png;base64,", "data:image/webp;base64,")):
        return value
    raise AppError(f"{field_name} 必须是公网 URL" + (" 或图片 Data URL" if image else ""), 400, "INVALID_MEDIA_URL")


def validate_video_request(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise AppError("请求体必须是 JSON 对象", 400, "INVALID_JSON_BODY")

    model = payload.get("model")
    prompt = payload.get("prompt")
    duration = payload.get("duration")
    resolution = payload.get("resolution")
    mode = payload.get("mode", "text")

    capability = VIDEO_CAPABILITIES.get(model) if model in ACTIVE_VIDEO_MODELS else None
    if capability is None:
        raise AppError("不支持的 MiniMax 视频模型", 400, "INVALID_MODEL")
    if not isinstance(prompt, str) or not prompt.strip():
        raise AppError("Prompt 不能为空", 400, "PROMPT_REQUIRED")
    prompt = prompt.strip()
    prompt_limit = 7000 if capability["api_version"] == "v2" else 2000
    if len(prompt) > prompt_limit:
        raise AppError(f"Prompt 不能超过 {prompt_limit} 个字符", 400, "PROMPT_TOO_LONG")
    if mode not in capability["modes"]:
        mode_name = "首尾帧" if mode == "first_last" else str(mode)
        raise AppError(f"当前模型不支持{mode_name}生成", 400, "INVALID_MODE")
    if isinstance(duration, bool) or not isinstance(duration, int):
        raise AppError("duration 必须是整数", 400, "INVALID_DURATION")
    if not isinstance(resolution, str):
        raise AppError("resolution 必须是字符串", 400, "INVALID_RESOLUTION")

    boolean_fields = ("prompt_optimizer", "fast_pretreatment", "aigc_watermark")
    for field_name in boolean_fields:
        if field_name in payload and not isinstance(payload[field_name], bool):
            raise AppError(f"{field_name} 必须是布尔值", 400, "INVALID_BOOLEAN")

    if capability["api_version"] == "v2":
        if not 4 <= duration <= 15 or resolution not in capability["resolutions"]:
            raise AppError("MiniMax-H3 仅支持 4-15 秒和 768P/2K", 400, "INVALID_MODEL_OPTIONS")
        ratio = payload.get("ratio") or ("16:9" if mode == "text" else "adaptive")
        if ratio not in H3_RATIOS or (mode == "text" and ratio == "adaptive"):
            raise AppError("H3 文生视频必须选择明确画幅，参考素材模式使用 adaptive", 400, "INVALID_RATIO")
        if mode != "text" and ratio != "adaptive":
            raise AppError("H3 图片或参考素材模式的 ratio 必须为 adaptive", 400, "INVALID_RATIO")

        content: list[dict[str, Any]] = [{"type": "text", "text": prompt}]
        if mode in {"first_frame", "first_last"}:
            first = _require_media_url(payload.get("first_frame_image"), "first_frame_image", image=True)
            content.append({"type": "image_url", "image_url": {"url": first}, "role": "first_frame"})
        if mode == "first_last":
            last = _require_media_url(payload.get("last_frame_image"), "last_frame_image", image=True)
            content.append({"type": "image_url", "image_url": {"url": last}, "role": "last_frame"})
        if mode == "reference":
            references = payload.get("references")
            if not isinstance(references, list) or not references or len(references) > 9:
                raise AppError("H3 参考素材数量必须为 1-9 个", 400, "INVALID_REFERENCES")
            type_counts = {"image": 0, "video": 0, "audio": 0}
            for index, reference in enumerate(references, 1):
                if not isinstance(reference, dict) or reference.get("type") not in type_counts:
                    raise AppError("H3 参考素材类型必须是 image、video 或 audio", 400, "INVALID_REFERENCE_TYPE")
                media_type = str(reference["type"])
                type_counts[media_type] += 1
                if media_type in {"video", "audio"} and type_counts[media_type] > 3:
                    raise AppError(f"H3 最多支持 3 个参考{media_type}", 400, "TOO_MANY_REFERENCES")
                media_url = _require_media_url(reference.get("url"), f"references[{index}].url", image=media_type == "image")
                content.append({
                    "type": f"{media_type}_url",
                    f"{media_type}_url": {"url": media_url},
                    "role": f"reference_{media_type}",
                })
        return {
            "api_version": "v2",
            "mode": mode,
            "model": model,
            "content": content,
            "resolution": resolution,
            "duration": duration,
            "ratio": ratio,
            "aigc_watermark": payload.get("aigc_watermark", False),
        }

    supported_combinations = capability.get("mode_combinations", {}).get(mode, capability["combinations"])
    if (duration, resolution) not in supported_combinations:
        raise AppError("当前模型不支持该时长和分辨率组合", 400, "INVALID_MODEL_OPTIONS")

    request: dict[str, Any] = {
        "api_version": "v1",
        "mode": mode,
        "model": model,
        "prompt": prompt,
        "duration": duration,
        "resolution": resolution,
        "prompt_optimizer": payload.get("prompt_optimizer", True),
        "aigc_watermark": payload.get("aigc_watermark", False),
    }
    if model in {"MiniMax-Hailuo-2.3", "MiniMax-Hailuo-2.3-Fast", "MiniMax-Hailuo-02"}:
        request["fast_pretreatment"] = payload.get("fast_pretreatment", False)
    if mode in {"first_frame", "first_last"}:
        request["first_frame_image"] = _require_media_url(payload.get("first_frame_image"), "first_frame_image", image=True)
    if mode == "first_last":
        request["last_frame_image"] = _require_media_url(payload.get("last_frame_image"), "last_frame_image", image=True)
    if mode == "subject":
        references = payload.get("references")
        if not isinstance(references, list) or len(references) != 1:
            raise AppError("S2V-01 当前仅支持一个人物主体", 400, "INVALID_REFERENCES")
        image_url = _require_media_url(references[0].get("url") if isinstance(references[0], dict) else None, "subject image", image=True)
        request["subject_reference"] = [{"type": "character", "image": [image_url]}]
    return request


def validate_generation_payload(payload: Any) -> dict[str, Any]:
    request = validate_video_request(payload)
    request.pop("api_version", None)
    request.pop("mode", None)
    return request


def _error_code_from_body(body: Any) -> int | None:
    if not isinstance(body, dict):
        return None
    base_resp = body.get("base_resp")
    if isinstance(base_resp, dict):
        value = base_resp.get("status_code")
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None
    error = body.get("error")
    if isinstance(error, dict):
        value = error.get("code")
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                pass
        message = error.get("message")
        if isinstance(message, str):
            match = re.search(r"\((\d{3,6})\)\s*$", message)
            if match:
                return int(match.group(1))
    return None


def _has_task_id(body: Any) -> bool:
    return isinstance(body, dict) and bool(body.get("task_id"))


def _safe_upstream_message(http_status: int | None, minimax_code: int | None) -> tuple[str, str, str]:
    if minimax_code in {1004, 2049} or http_status == 401:
        return "MiniMax API Key 鉴权失败，请检查当前 Key", "UPSTREAM_AUTH", "auth"
    if minimax_code == 1008 or http_status == 402:
        return "MiniMax 当前 Key 余额不足", "UPSTREAM_BALANCE", "balance"
    if minimax_code == 1002 or http_status == 429:
        return "MiniMax 当前 Key 触发限流", "UPSTREAM_RATE_LIMIT", "rate_limit"
    if minimax_code == 1026 or http_status == 422:
        return "Prompt 未通过 MiniMax 内容审核", "UPSTREAM_SAFETY", "safety"
    if minimax_code == 2013 or http_status == 400:
        return "MiniMax 请求参数无效", "UPSTREAM_BAD_REQUEST", "parameter"
    if http_status and http_status >= 500:
        return "MiniMax 服务暂时不可用", "UPSTREAM_SERVER", "server"
    return "MiniMax 上游请求失败", "UPSTREAM_ERROR", "upstream"


def _safe_upstream_override(body: Any) -> str | None:
    if not isinstance(body, dict) or not isinstance(body.get("error"), dict):
        return None
    message = body["error"].get("message")
    if isinstance(message, str) and "暂不支持 MiniMax-H3" in message:
        return "当前上游 Key 类型不支持 MiniMax-H3，请配置 MINIMAX_PAYGO_API_KEYS 按量 Key"
    return None


class MiniMaxTransport:
    def request_json(self, key: str, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        body = None
        headers = {
            "Authorization": f"Bearer {key}",
            "Accept": "application/json",
        }
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            headers["Content-Type"] = "application/json"
        request = Request(f"{API_BASE_URL}{path}", data=body, headers=headers, method=method)

        try:
            with urlopen(request, timeout=60) as response:
                raw = response.read(2_000_000)
                http_status = getattr(response, "status", 200)
        except HTTPError as exc:
            raw = exc.read(2_000_000)
            body_data = self._decode_json(raw, exc.code, allow_error=True)
            message, code, category = _safe_upstream_message(exc.code, _error_code_from_body(body_data))
            message = _safe_upstream_override(body_data) or message
            raise UpstreamError(
                message,
                http_status=exc.code,
                minimax_code=_error_code_from_body(body_data),
                retryable_create=exc.code in RETRYABLE_CREATE_HTTP_STATUSES,
                category=category,
                has_task_id=_has_task_id(body_data),
            ) from exc
        except (URLError, TimeoutError, socket.timeout, ConnectionError) as exc:
            raise UpstreamTransportError() from exc

        data = self._decode_json(raw, http_status)
        status_code = _error_code_from_body(data)
        if status_code not in (None, 0) and not _has_task_id(data):
            message, _, category = _safe_upstream_message(http_status, status_code)
            raise UpstreamError(
                message,
                http_status=http_status,
                minimax_code=status_code,
                retryable_create=status_code in RETRYABLE_CREATE_CODES,
                category=category,
            )
        return data

    @staticmethod
    def _decode_json(raw: bytes, http_status: int | None, allow_error: bool = False) -> dict[str, Any]:
        try:
            data = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            if allow_error:
                return {}
            raise UpstreamError("MiniMax 返回了无法解析的响应", http_status=http_status, category="non_json") from exc
        if not isinstance(data, dict):
            raise UpstreamError("MiniMax 返回了无效响应", http_status=http_status, category="non_json")
        return data


def _extract_task_object(data: dict[str, Any]) -> dict[str, Any]:
    nested = data.get("task")
    return nested if isinstance(nested, dict) else data


def provider_payload(request: dict[str, Any]) -> dict[str, Any]:
    payload = {key: value for key, value in request.items() if key not in {"api_version", "mode"}}
    if request.get("model") == "S2V-01":
        payload.pop("duration", None)
        payload.pop("resolution", None)
        payload.pop("fast_pretreatment", None)
    return payload


def request_for_storage(request: dict[str, Any]) -> dict[str, Any]:
    def scrub(value: Any) -> Any:
        if isinstance(value, str) and value.startswith("data:"):
            media_type = value.partition(";")[0]
            return media_type + ";base64,[omitted]"
        if isinstance(value, list):
            return [scrub(item) for item in value]
        if isinstance(value, dict):
            return {key: scrub(item) for key, item in value.items()}
        return value

    return scrub(request)


def normalize_task_response(data: dict[str, Any], expected_task_id: str, record: dict[str, Any]) -> dict[str, Any]:
    task = _extract_task_object(data)
    task_id = str(task.get("id") or data.get("task_id") or expected_task_id)
    status = str(task.get("status") or data.get("status") or record.get("status") or "Unknown")
    file_id = task.get("file_id") or data.get("file_id")
    content = task.get("content")
    result_url = None
    if isinstance(content, dict):
        file_id = file_id or content.get("file_id")
        result_url = content.get("url")
    if file_id is not None:
        file_id = str(file_id)
    base_resp = data.get("base_resp")
    error = None
    if status.lower() in FAIL_STATUSES:
        status_code = _error_code_from_body(data)
        error, _, _ = _safe_upstream_message(None, status_code)
    return {
        "task_id": task_id,
        "status": status,
        "file_id": file_id,
        "result_url": result_url,
        "usage": task.get("usage") if isinstance(task.get("usage"), dict) else None,
        "video_width": task.get("video_width") or data.get("video_width"),
        "video_height": task.get("video_height") or data.get("video_height"),
        "key_id": record["key_id"],
        "key_label": record.get("key_label", ""),
        "error": error,
        "base_resp": base_resp if isinstance(base_resp, dict) else None,
    }


class MiniMaxService:
    def __init__(
        self,
        key_pool: KeyPool,
        task_store: TaskStore,
        transport: MiniMaxTransport | Any | None = None,
        h3_key_pool: KeyPool | None = None,
    ):
        self.key_pool = key_pool
        self.h3_key_pool = h3_key_pool or key_pool
        self.task_store = task_store
        self.transport = transport or MiniMaxTransport()
        self._create_lock = threading.Lock()

    def replace_key_pool(self, key_pool: KeyPool) -> None:
        with self._create_lock:
            with self.key_pool._lock:
                key_pool._unavailable_until = {
                    key_id: until
                    for key_id, until in self.key_pool._unavailable_until.items()
                    if key_pool.get(key_id) is not None
                }
            self.key_pool = key_pool

    def status(self) -> dict[str, Any]:
        day = quota_day_token()
        limit = daily_key_quota()
        usage = self.task_store.daily_usage(day)
        key_states: list[dict[str, Any]] = []
        for info in self.key_pool.infos():
            used = usage.get(info.key_id, 0)
            availability = self.key_pool.availability(info.key_id)
            state = "quota_exhausted" if used >= limit else availability
            key_states.append({
                "key_id": info.key_id,
                "key_label": info.label,
                "used_today": used,
                "remaining_today": max(0, limit - used),
                "state": state,
            })
        return {
            "configured_keys": len(key_states),
            "available_keys": sum(1 for item in key_states if item["state"] == "ready"),
            "daily_quota": limit,
            "quota_day": day,
            "keys": key_states,
        }

    def create_task(self, payload: Any) -> dict[str, Any]:
        # ponytail: one local create lock prevents quota oversubscription; use per-key locks if concurrency matters.
        with self._create_lock:
            return self._create_task_locked(payload)

    def _create_task_locked(self, payload: Any) -> dict[str, Any]:
        request_payload = validate_video_request(payload)
        api_version = request_payload["api_version"]
        upstream_payload = provider_payload(request_payload)
        selected_pool = self.h3_key_pool if api_version == "v2" else self.key_pool
        quota_day = quota_day_token()
        quota_limit = daily_key_quota()
        usage = self.task_store.daily_usage(quota_day)
        candidates = selected_pool.candidates()
        if selected_pool is self.key_pool:
            candidates = [info for info in candidates if usage.get(info.key_id, 0) < quota_limit]
        if not candidates:
            raise AppError(
                f"当前没有可用的 MiniMax Key，今日每 Key 配额为 {quota_limit} 条",
                429,
                "DAILY_QUOTA_EXHAUSTED",
            )

        failures: list[str] = []
        for info in candidates:
            try:
                data = self.transport.request_json(
                    info.value,
                    "POST",
                    f"/{api_version}/video_generation",
                    upstream_payload,
                )
                task_id = str(data.get("task_id") or "")
                if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", task_id):
                    raise UpstreamError("MiniMax 未返回任务 ID", http_status=502, category="invalid_response")
                record = {
                    "task_id": task_id,
                    "key_id": info.key_id,
                    "key_label": info.label,
                    "status": "Preparing",
                    "file_id": None,
                    "created_at": utc_now(),
                    "updated_at": utc_now(),
                    "quota_day": quota_day,
                    "api_version": api_version,
                    "mode": request_payload["mode"],
                    "model": request_payload["model"],
                    "prompt": request_payload.get("prompt") or request_payload["content"][0]["text"],
                    "duration": request_payload["duration"],
                    "resolution": request_payload["resolution"],
                    "ratio": request_payload.get("ratio"),
                    "result_url": None,
                    "usage": None,
                }
                self.task_store.upsert(record)
                return {
                    "task_id": task_id,
                    "key_id": info.key_id,
                    "key_label": info.label,
                    "api_version": api_version,
                }
            except UpstreamError as exc:
                if exc.retryable_create and not exc.has_task_id:
                    selected_pool.mark_unavailable(info, exc.category)
                    failures.append(f"{info.label}: {exc.message}")
                    continue
                code = "UPSTREAM_SUBMIT_UNKNOWN" if exc.category in {"transport", "server", "non_json", "invalid_response"} else "UPSTREAM_REJECTED"
                raise AppError(exc.message, exc.http_status, code) from exc

        summary = "；".join(failures) if failures else "没有可用的 Key"
        raise AppError(f"所有 MiniMax Key 均未能创建任务：{summary}", 502, "ALL_KEYS_FAILED")

    def history(self, limit: int = 50) -> dict[str, Any]:
        records = self.task_store.list(limit)
        items: list[dict[str, Any]] = []
        for record in records:
            file_id = record.get("file_id")
            result_url = record.get("result_url")
            success = bool(str(record.get("status", "")).lower() in SUCCESS_STATUSES and (file_id or result_url))
            preview_url = None
            if success:
                params = {"task_id": record.get("task_id")}
                if file_id:
                    params["file_id"] = file_id
                preview_url = "/api/download?" + urlencode(params)
            item = {
                "task_id": record.get("task_id"),
                "key_id": record.get("key_id"),
                "key_label": record.get("key_label", ""),
                "status": record.get("status", "Unknown"),
                "prompt": record.get("prompt", ""),
                "model": record.get("model", ""),
                "duration": record.get("duration"),
                "resolution": record.get("resolution", ""),
                "file_id": file_id,
                "result_url": result_url,
                "video_width": record.get("video_width"),
                "video_height": record.get("video_height"),
                "created_at": record.get("created_at"),
                "updated_at": record.get("updated_at"),
                "error": record.get("error"),
                "preview_url": preview_url,
            }
            items.append(item)
        return {"items": items, "total": self.task_store.count(), "quota_day": quota_day_token()}

    def query_task(self, task_id: str) -> dict[str, Any]:
        record = self._get_record(task_id)
        selected_pool = self.h3_key_pool if record.get("api_version") == "v2" else self.key_pool
        info = selected_pool.get(record["key_id"])
        if info is None:
            raise AppError("该任务绑定的 Key 当前不可用，无法安全切换账号查询", 409, "BOUND_KEY_UNAVAILABLE")
        try:
            api_version = record.get("api_version", "v1")
            query_path = (
                f"/v2/query/video_generation/{quote(task_id, safe='')}"
                if api_version == "v2"
                else f"/v1/query/video_generation?{urlencode({'task_id': task_id})}"
            )
            data = self.transport.request_json(
                info.value,
                "GET",
                query_path,
            )
        except UpstreamError as exc:
            if exc.retryable_create:
                selected_pool.mark_unavailable(info, exc.category)
            raise AppError(
                "该任务绑定的 Key 查询失败，未切换到其他 Key：" + exc.message,
                exc.http_status,
                "BOUND_KEY_QUERY_FAILED",
            ) from exc

        normalized = normalize_task_response(data, task_id, record)
        changes: dict[str, Any] = {"status": normalized["status"]}
        if normalized["file_id"]:
            changes["file_id"] = normalized["file_id"]
        if normalized["result_url"]:
            changes["result_url"] = normalized["result_url"]
        if normalized["usage"] is not None:
            changes["usage"] = normalized["usage"]
        if normalized["video_width"] is not None:
            changes["video_width"] = normalized["video_width"]
        if normalized["video_height"] is not None:
            changes["video_height"] = normalized["video_height"]
        changes["updated_at"] = utc_now()
        changes["error"] = normalized["error"]
        self.task_store.update(task_id, **changes)
        normalized.pop("base_resp", None)
        return normalized

    def download_url(self, task_id: str, file_id: str = "") -> str:
        record = self._get_record(task_id)
        result_url = record.get("result_url")
        if result_url:
            parsed = urlparse(str(result_url))
            if parsed.scheme not in {"http", "https"} or not parsed.netloc:
                raise AppError("MiniMax 返回的下载地址无效", 502, "INVALID_DOWNLOAD_URL")
            if str(record.get("status", "")).lower() not in SUCCESS_STATUSES:
                raise AppError("任务尚未成功，暂时不能下载", 409, "TASK_NOT_COMPLETE")
            return str(result_url)
        if not file_id or str(record.get("file_id")) != str(file_id):
            raise AppError("file_id 与任务绑定记录不匹配", 400, "FILE_TASK_MISMATCH")
        if str(record.get("status", "")).lower() not in SUCCESS_STATUSES:
            raise AppError("任务尚未成功，暂时不能下载", 409, "TASK_NOT_COMPLETE")
        info = self.key_pool.get(record["key_id"])
        if info is None:
            raise AppError("该任务绑定的 Key 当前不可用，无法安全切换账号下载", 409, "BOUND_KEY_UNAVAILABLE")
        try:
            data = self.transport.request_json(
                info.value,
                "GET",
                f"/v1/files/retrieve?{urlencode({'file_id': file_id})}",
            )
        except UpstreamError as exc:
            raise AppError(
                "该任务绑定的 Key 获取下载地址失败：" + exc.message,
                exc.http_status,
                "BOUND_KEY_DOWNLOAD_FAILED",
            ) from exc

        file_data = data.get("file")
        download_url = file_data.get("download_url") if isinstance(file_data, dict) else None
        parsed = urlparse(download_url or "")
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise AppError("MiniMax 返回的下载地址无效", 502, "INVALID_DOWNLOAD_URL")
        return download_url

    def _get_record(self, task_id: str) -> dict[str, Any]:
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", task_id):
            raise AppError("任务 ID 格式无效", 400, "INVALID_TASK_ID")
        record = self.task_store.get(task_id)
        if not record:
            raise AppError("找不到本地任务绑定记录", 404, "TASK_NOT_FOUND")
        return record


def official_video_price_fen(model: str, duration: int, resolution: str) -> int:
    try:
        return OFFICIAL_VIDEO_PRICES_FEN[(model, duration, resolution)]
    except KeyError as exc:
        raise AppError("当前模型没有对应的价格配置", 500, "INVALID_PRICE_CONFIG") from exc


def quote_video_fen(model: str, duration: int, resolution: str) -> int:
    return math.ceil(official_video_price_fen(model, duration, resolution) / 2)


def yuan_to_fen(value: Any, *, allow_zero: bool = False) -> int:
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError):
        amount = Decimal("NaN")
    fen = amount * 100
    minimum = 0 if allow_zero else 1
    if isinstance(value, bool) or not amount.is_finite() or fen != fen.to_integral_value() or fen < minimum:
        qualifier = "非负" if allow_zero else "正数"
        raise AppError(f"充值金额必须是最多两位小数的{qualifier}人民币元", 400, "INVALID_AMOUNT")
    return int(fen)


def video_model_catalog() -> list[dict[str, Any]]:
    catalog: list[dict[str, Any]] = []
    for model in ACTIVE_VIDEO_MODELS:
        capability = VIDEO_CAPABILITIES[model]
        combinations = [
            {
                "duration": duration,
                "resolution": resolution,
                "price_fen": quote_video_fen(model, duration, resolution),
                "official_price_fen": official_video_price_fen(model, duration, resolution),
            }
            for duration, resolution in sorted(capability["combinations"])
        ]
        catalog.append({
            "id": model,
            "api_version": capability["api_version"],
            "modes": sorted(capability["modes"]),
            "combinations": combinations,
            "mode_combinations": {mode: combinations for mode in sorted(capability["modes"])},
        })
    return catalog


class PlatformStore:
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, future=True)
        self.metadata = MetaData()
        self._lock = threading.Lock()
        self.provider_keys = Table(
            "provider_keys", self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("key_id", String(16), nullable=False, unique=True),
            Column("label", String(200), nullable=False),
            Column("key_hash", String(64), nullable=False, unique=True),
            Column("secret_value", Text, nullable=False),
            Column("masked_value", String(40), nullable=False),
            Column("enabled", Boolean, nullable=False, default=True),
            Column("created_at", String(40), nullable=False),
            Column("updated_at", String(40), nullable=False),
        )
        self.app_settings = Table(
            "app_settings", self.metadata,
            Column("name", String(100), primary_key=True),
            Column("value", Text, nullable=False),
        )
        self.customers = Table(
            "customers", self.metadata,
            Column("id", String(40), primary_key=True),
            Column("name", String(200), nullable=False),
            Column("enabled", Boolean, nullable=False, default=True),
            Column("created_at", String(40), nullable=False),
        )
        self.customer_keys = Table(
            "customer_keys", self.metadata,
            Column("id", String(40), primary_key=True),
            Column("customer_id", String(40), ForeignKey("customers.id"), nullable=False, index=True),
            Column("label", String(200), nullable=False),
            Column("key_hash", String(64), nullable=False, unique=True),
            Column("key_prefix", String(24), nullable=False),
            Column("enabled", Boolean, nullable=False, default=True),
            Column("created_at", String(40), nullable=False),
        )
        self.wallets = Table(
            "wallets", self.metadata,
            Column("customer_id", String(40), ForeignKey("customers.id"), primary_key=True),
            Column("balance_fen", Integer, nullable=False, default=0),
            Column("reserved_fen", Integer, nullable=False, default=0),
        )
        # Legacy account/key tables retain existing balances and task history; this is the only public identity.
        self.tokens = Table(
            "tokens", self.metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("account_id", String(40), ForeignKey("customers.id"), nullable=False, unique=True),
            Column("key_id", String(40), ForeignKey("customer_keys.id"), nullable=False, unique=True),
            Column("created_at", String(40), nullable=False),
            Column("deleted_at", String(40), nullable=True),
        )
        self.tasks = Table(
            "video_tasks", self.metadata,
            Column("id", String(40), primary_key=True),
            Column("customer_id", String(40), ForeignKey("customers.id"), nullable=False, index=True),
            Column("customer_key_id", String(40), ForeignKey("customer_keys.id"), nullable=False),
            Column("idempotency_key", String(200), nullable=False),
            Column("provider_task_id", String(160), nullable=True, index=True),
            Column("provider_key_id", String(16), nullable=True),
            Column("provider_key_label", String(60), nullable=True),
            Column("api_version", String(8), nullable=False),
            Column("model", String(80), nullable=False),
            Column("mode", String(30), nullable=False),
            Column("prompt", Text, nullable=False),
            Column("duration", Integer, nullable=False),
            Column("resolution", String(20), nullable=False),
            Column("ratio", String(20), nullable=True),
            Column("request_json", Text, nullable=False),
            Column("status", String(40), nullable=False),
            Column("quoted_fen", Integer, nullable=False),
            Column("reserved_fen", Integer, nullable=False),
            Column("charged_fen", Integer, nullable=False, default=0),
            Column("file_id", String(160), nullable=True),
            Column("result_url", Text, nullable=True),
            Column("usage_json", Text, nullable=True),
            Column("error", Text, nullable=True),
            Column("created_at", String(40), nullable=False),
            Column("updated_at", String(40), nullable=False),
            UniqueConstraint("customer_id", "idempotency_key", name="uq_video_task_idempotency"),
        )
        self.ledger = Table(
            "wallet_ledger", self.metadata,
            Column("id", String(40), primary_key=True),
            Column("customer_id", String(40), ForeignKey("customers.id"), nullable=False, index=True),
            Column("task_id", String(40), ForeignKey("video_tasks.id"), nullable=True),
            Column("kind", String(30), nullable=False),
            Column("balance_delta_fen", Integer, nullable=False),
            Column("reserved_delta_fen", Integer, nullable=False),
            Column("reference", String(220), nullable=False, unique=True),
            Column("created_at", String(40), nullable=False),
        )

    @staticmethod
    def _dict(row: Any) -> dict[str, Any] | None:
        return dict(row._mapping) if row is not None else None

    def create_schema(self) -> None:
        self.metadata.create_all(self.engine)
        with self.engine.begin() as connection:
            mapped_accounts = set(connection.execute(select(self.tokens.c.account_id)).scalars())
            legacy_keys = connection.execute(
                select(
                    self.customer_keys.c.customer_id,
                    self.customer_keys.c.id,
                    self.customer_keys.c.created_at,
                ).order_by(self.customer_keys.c.created_at, self.customer_keys.c.id)
            )
            for account_id, key_id, created_at in legacy_keys:
                if account_id in mapped_accounts:
                    continue
                connection.execute(insert(self.tokens).values(
                    account_id=account_id,
                    key_id=key_id,
                    created_at=created_at,
                    deleted_at=None,
                ))
                mapped_accounts.add(account_id)

    @staticmethod
    def _validate_provider_key_id(provider_key_id: Any) -> int:
        if isinstance(provider_key_id, bool) or not isinstance(provider_key_id, int) or provider_key_id < 1:
            raise AppError("Key ID 必须是正整数", 400, "INVALID_PROVIDER_KEY_ID")
        return provider_key_id

    @staticmethod
    def _validate_provider_key_label(label: Any) -> str:
        if not isinstance(label, str) or not label.strip() or len(label.strip()) > 200:
            raise AppError("Key 名称不能为空且不能超过 200 字符", 400, "INVALID_PROVIDER_KEY_LABEL")
        return label.strip()

    @staticmethod
    def _validate_provider_secret(secret_value: Any) -> str:
        if (
            not isinstance(secret_value, str)
            or not secret_value.strip()
            or len(secret_value.strip()) > 2048
            or any(character.isspace() for character in secret_value.strip())
        ):
            raise AppError("MiniMax Key 不能为空、不能含空白且不能超过 2048 字符", 400, "INVALID_PROVIDER_KEY")
        return secret_value.strip()

    @staticmethod
    def _mask_provider_secret(secret_value: str) -> str:
        return secret_value[:8] + "…" + secret_value[-4:] if len(secret_value) > 12 else secret_value[:4] + "…"

    @staticmethod
    def _public_provider_key(row: Any) -> dict[str, Any]:
        item = dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
        return {
            key: item[key]
            for key in ("id", "key_id", "label", "masked_value", "enabled", "created_at", "updated_at")
        }

    def get_provider_key(self, provider_key_id: int) -> dict[str, Any]:
        provider_key_id = self._validate_provider_key_id(provider_key_id)
        with self.engine.connect() as connection:
            row = connection.execute(select(self.provider_keys).where(self.provider_keys.c.id == provider_key_id)).first()
        if not row:
            raise AppError("上游 Key 不存在", 404, "PROVIDER_KEY_NOT_FOUND")
        return self._public_provider_key(row)

    def list_provider_keys(self) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(select(self.provider_keys).order_by(self.provider_keys.c.id))
            return [self._public_provider_key(row) for row in rows]

    def load_provider_keys(self) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(
                select(self.provider_keys).order_by(self.provider_keys.c.id)
            )]

    def create_provider_key(self, label: Any, secret_value: Any) -> dict[str, Any]:
        label = self._validate_provider_key_label(label)
        secret_value = self._validate_provider_secret(secret_value)
        key_hash = hashlib.sha256(secret_value.encode("utf-8")).hexdigest()
        with self._lock, self.engine.begin() as connection:
            if connection.execute(select(self.provider_keys.c.id).where(
                self.provider_keys.c.key_hash == key_hash
            )).first():
                raise AppError("该 MiniMax Key 已存在", 409, "PROVIDER_KEY_EXISTS")
            now = utc_now()
            provider_key_id = connection.execute(insert(self.provider_keys).values(
                key_id=key_id_for(secret_value),
                label=label,
                key_hash=key_hash,
                secret_value=secret_value,
                masked_value=self._mask_provider_secret(secret_value),
                enabled=True,
                created_at=now,
                updated_at=now,
            )).inserted_primary_key[0]
        return self.get_provider_key(int(provider_key_id))

    def seed_provider_keys(self, values: list[str]) -> int:
        marker = "provider_keys_seeded"
        validated = [self._validate_provider_secret(value) for value in values]
        with self._lock, self.engine.begin() as connection:
            if connection.execute(select(self.app_settings.c.name).where(
                self.app_settings.c.name == marker
            )).first():
                return 0
            imported = 0
            if not connection.execute(select(self.provider_keys.c.id).limit(1)).first():
                now = utc_now()
                for index, secret_value in enumerate(validated):
                    connection.execute(insert(self.provider_keys).values(
                        key_id=key_id_for(secret_value),
                        label=f"Key #{index + 1}",
                        key_hash=hashlib.sha256(secret_value.encode("utf-8")).hexdigest(),
                        secret_value=secret_value,
                        masked_value=self._mask_provider_secret(secret_value),
                        enabled=True,
                        created_at=now,
                        updated_at=now,
                    ))
                imported = len(validated)
            connection.execute(insert(self.app_settings).values(name=marker, value=utc_now()))
        return imported

    def update_provider_key(
        self,
        provider_key_id: int,
        *,
        label: Any = None,
        enabled: Any = None,
        secret_value: Any = None,
    ) -> dict[str, Any]:
        provider_key_id = self._validate_provider_key_id(provider_key_id)
        if label is None and enabled is None and secret_value is None:
            raise AppError("至少提供 Key 名称、启用状态或新的 Key", 400, "PROVIDER_KEY_UPDATE_REQUIRED")
        values: dict[str, Any] = {"updated_at": utc_now()}
        if label is not None:
            values["label"] = self._validate_provider_key_label(label)
        if enabled is not None:
            if not isinstance(enabled, bool):
                raise AppError("enabled 必须是布尔值", 400, "INVALID_PROVIDER_KEY_STATUS")
            values["enabled"] = enabled
        if secret_value is not None:
            secret_value = self._validate_provider_secret(secret_value)
            values.update(
                key_hash=hashlib.sha256(secret_value.encode("utf-8")).hexdigest(),
                secret_value=secret_value,
                masked_value=self._mask_provider_secret(secret_value),
            )
        with self._lock, self.engine.begin() as connection:
            row = connection.execute(select(self.provider_keys.c.id).where(
                self.provider_keys.c.id == provider_key_id
            ).with_for_update()).first()
            if not row:
                raise AppError("上游 Key 不存在", 404, "PROVIDER_KEY_NOT_FOUND")
            if "key_hash" in values and connection.execute(select(self.provider_keys.c.id).where(
                self.provider_keys.c.key_hash == values["key_hash"],
                self.provider_keys.c.id != provider_key_id,
            )).first():
                raise AppError("该 MiniMax Key 已存在", 409, "PROVIDER_KEY_EXISTS")
            connection.execute(update(self.provider_keys).where(
                self.provider_keys.c.id == provider_key_id
            ).values(**values))
        return self.get_provider_key(provider_key_id)

    def delete_provider_key(self, provider_key_id: int) -> dict[str, Any]:
        provider_key = self.get_provider_key(provider_key_id)
        with self._lock, self.engine.begin() as connection:
            connection.execute(delete(self.provider_keys).where(self.provider_keys.c.id == provider_key_id))
        return {"id": provider_key_id, "deleted": True, "key_id": provider_key["key_id"]}

    def provider_key_in_use(self, key_id: str) -> bool:
        with self.engine.connect() as connection:
            return connection.execute(select(self.tasks.c.id).where(
                self.tasks.c.provider_key_id == key_id
            ).limit(1)).first() is not None

    @staticmethod
    def _validate_token_name(name: Any) -> str:
        if not isinstance(name, str) or not name.strip() or len(name.strip()) > 200:
            raise AppError("令牌名称不能为空且不能超过 200 字符", 400, "INVALID_TOKEN_NAME")
        return name.strip()

    @staticmethod
    def _validate_token_id(token_id: Any) -> int:
        if isinstance(token_id, bool) or not isinstance(token_id, int) or token_id < 1:
            raise AppError("令牌 ID 必须是正整数", 400, "INVALID_TOKEN_ID")
        return token_id

    def _token_statement(self):
        return (
            select(
                self.tokens.c.id,
                self.tokens.c.account_id,
                self.tokens.c.key_id,
                self.tokens.c.created_at,
                self.customer_keys.c.label,
                self.customer_keys.c.key_prefix.label("prefix"),
                self.customer_keys.c.enabled.label("key_enabled"),
                self.customers.c.enabled.label("account_enabled"),
                self.wallets.c.balance_fen,
                self.wallets.c.reserved_fen,
            )
            .join(self.customers, self.customers.c.id == self.tokens.c.account_id)
            .join(self.customer_keys, self.customer_keys.c.id == self.tokens.c.key_id)
            .join(self.wallets, self.wallets.c.customer_id == self.tokens.c.account_id)
        )

    @staticmethod
    def _public_token(row: Any) -> dict[str, Any]:
        item = dict(row._mapping)
        return {
            "id": item["id"],
            "name": item["label"],
            "prefix": item["prefix"],
            "enabled": bool(item["key_enabled"] and item["account_enabled"]),
            "balance_fen": item["balance_fen"],
            "reserved_fen": item["reserved_fen"],
            "available_fen": item["balance_fen"] - item["reserved_fen"],
            "created_at": item["created_at"],
        }

    def create_token(self, name: Any, initial_balance_fen: int = 0) -> dict[str, Any]:
        name = self._validate_token_name(name)
        if isinstance(initial_balance_fen, bool) or not isinstance(initial_balance_fen, int) or initial_balance_fen < 0:
            raise AppError("初始余额必须是非负整数分", 400, "INVALID_AMOUNT")
        raw_key = "mmx_live_" + secrets.token_urlsafe(32)
        key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        key_id = "key_" + uuid.uuid4().hex
        account_id = "cus_" + uuid.uuid4().hex
        created_at = utc_now()
        with self.engine.begin() as connection:
            connection.execute(insert(self.customers).values(
                id=account_id, name=name, enabled=True, created_at=created_at,
            ))
            connection.execute(insert(self.wallets).values(
                customer_id=account_id, balance_fen=initial_balance_fen, reserved_fen=0,
            ))
            connection.execute(insert(self.customer_keys).values(
                id=key_id,
                customer_id=account_id,
                label=name,
                key_hash=key_hash,
                key_prefix=raw_key[:17],
                enabled=True,
                created_at=created_at,
            ))
            token_id = connection.execute(insert(self.tokens).values(
                account_id=account_id,
                key_id=key_id,
                created_at=created_at,
                deleted_at=None,
            )).inserted_primary_key[0]
            if initial_balance_fen:
                connection.execute(insert(self.ledger).values(
                    id="led_" + uuid.uuid4().hex,
                    customer_id=account_id,
                    task_id=None,
                    kind="recharge",
                    balance_delta_fen=initial_balance_fen,
                    reserved_delta_fen=0,
                    reference=f"token:{token_id}:initial",
                    created_at=created_at,
                ))
        return {**self.get_token(token_id), "api_key": raw_key}

    def get_token(self, token_id: int) -> dict[str, Any]:
        token_id = self._validate_token_id(token_id)
        with self.engine.connect() as connection:
            row = connection.execute(self._token_statement().where(
                self.tokens.c.id == token_id,
                self.tokens.c.deleted_at.is_(None),
            )).first()
        if not row:
            raise AppError("令牌不存在", 404, "TOKEN_NOT_FOUND")
        return self._public_token(row)

    def list_tokens(self) -> list[dict[str, Any]]:
        with self.engine.connect() as connection:
            rows = connection.execute(self._token_statement().where(
                self.tokens.c.deleted_at.is_(None),
            ).order_by(self.tokens.c.id))
            return [self._public_token(row) for row in rows]

    def update_token(self, token_id: int, *, name: Any = None, enabled: Any = None) -> dict[str, Any]:
        token_id = self._validate_token_id(token_id)
        if name is None and enabled is None:
            raise AppError("至少提供令牌名称或启用状态", 400, "TOKEN_UPDATE_REQUIRED")
        if name is not None:
            name = self._validate_token_name(name)
        if enabled is not None and not isinstance(enabled, bool):
            raise AppError("enabled 必须是布尔值", 400, "INVALID_TOKEN_STATUS")
        with self._lock, self.engine.begin() as connection:
            token = self._dict(connection.execute(select(self.tokens).where(
                self.tokens.c.id == token_id,
                self.tokens.c.deleted_at.is_(None),
            ).with_for_update()).first())
            if not token:
                raise AppError("令牌不存在", 404, "TOKEN_NOT_FOUND")
            if name is not None:
                connection.execute(update(self.customers).where(
                    self.customers.c.id == token["account_id"]
                ).values(name=name))
                connection.execute(update(self.customer_keys).where(
                    self.customer_keys.c.id == token["key_id"]
                ).values(label=name))
            if enabled is not None:
                connection.execute(update(self.customers).where(
                    self.customers.c.id == token["account_id"]
                ).values(enabled=enabled))
                connection.execute(update(self.customer_keys).where(
                    self.customer_keys.c.id == token["key_id"]
                ).values(enabled=enabled))
        return self.get_token(token_id)

    def delete_token(self, token_id: int) -> dict[str, Any]:
        token_id = self._validate_token_id(token_id)
        with self._lock, self.engine.begin() as connection:
            token = self._dict(connection.execute(select(self.tokens).where(
                self.tokens.c.id == token_id,
                self.tokens.c.deleted_at.is_(None),
            ).with_for_update()).first())
            if not token:
                raise AppError("令牌不存在", 404, "TOKEN_NOT_FOUND")
            wallet = self._dict(connection.execute(select(self.wallets).where(
                self.wallets.c.customer_id == token["account_id"]
            ).with_for_update()).first())
            if wallet and wallet["reserved_fen"]:
                raise AppError("令牌仍有冻结额度，结算后才能删除", 409, "TOKEN_HAS_RESERVED_BALANCE")
            connection.execute(update(self.tokens).where(self.tokens.c.id == token_id).values(deleted_at=utc_now()))
            connection.execute(update(self.customers).where(
                self.customers.c.id == token["account_id"]
            ).values(enabled=False))
            connection.execute(update(self.customer_keys).where(
                self.customer_keys.c.id == token["key_id"]
            ).values(enabled=False))
        return {"id": token_id, "deleted": True}

    def authenticate_token(self, raw_key: str) -> dict[str, Any]:
        if not isinstance(raw_key, str) or not raw_key.startswith("mmx_live_"):
            raise AppError("访问令牌无效", 401, "INVALID_TOKEN")
        key_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        statement = (
            select(
                self.tokens.c.id.label("token_id"),
                self.customer_keys.c.id.label("key_id"),
                self.customer_keys.c.customer_id.label("account_id"),
                self.customer_keys.c.label,
            )
            .join(self.tokens, self.tokens.c.key_id == self.customer_keys.c.id)
            .join(self.customers, self.customers.c.id == self.customer_keys.c.customer_id)
            .where(
                self.customer_keys.c.key_hash == key_hash,
                self.customer_keys.c.enabled.is_(True),
                self.customers.c.enabled.is_(True),
                self.tokens.c.deleted_at.is_(None),
            )
        )
        with self.engine.connect() as connection:
            principal = self._dict(connection.execute(statement).first())
        if not principal:
            raise AppError("访问令牌无效", 401, "INVALID_TOKEN")
        return principal

    def recharge_token(self, token_id: int, amount_fen: int, reference: str) -> dict[str, Any]:
        token_id = self._validate_token_id(token_id)
        if isinstance(amount_fen, bool) or not isinstance(amount_fen, int) or amount_fen <= 0:
            raise AppError("充值金额必须是正整数分", 400, "INVALID_AMOUNT")
        if not isinstance(reference, str) or not reference.strip() or len(reference.strip()) > 220:
            raise AppError("充值 reference 不能为空且不能超过 220 字符", 400, "INVALID_REFERENCE")
        with self._lock, self.engine.begin() as connection:
            existing = connection.execute(select(self.ledger.c.id).where(self.ledger.c.reference == reference.strip())).first()
            if not existing:
                token = self._dict(connection.execute(select(self.tokens).where(
                    self.tokens.c.id == token_id,
                    self.tokens.c.deleted_at.is_(None),
                ).with_for_update()).first())
                if not token:
                    raise AppError("令牌不存在", 404, "TOKEN_NOT_FOUND")
                wallet = self._dict(connection.execute(
                    select(self.wallets).where(self.wallets.c.customer_id == token["account_id"]).with_for_update()
                ).first())
                if not wallet:
                    raise AppError("令牌额度不存在", 404, "TOKEN_WALLET_NOT_FOUND")
                connection.execute(update(self.wallets).where(self.wallets.c.customer_id == token["account_id"]).values(
                    balance_fen=wallet["balance_fen"] + amount_fen,
                ))
                connection.execute(insert(self.ledger).values(
                    id="led_" + uuid.uuid4().hex,
                    customer_id=token["account_id"],
                    task_id=None,
                    kind="recharge",
                    balance_delta_fen=amount_fen,
                    reserved_delta_fen=0,
                    reference=reference.strip(),
                    created_at=utc_now(),
                ))
        return self.get_token(token_id)

    def _wallet_for_account(self, account_id: str) -> dict[str, int]:
        with self.engine.connect() as connection:
            wallet = self._dict(connection.execute(select(self.wallets).where(
                self.wallets.c.customer_id == account_id
            )).first())
        if not wallet:
            raise AppError("令牌额度不存在", 404, "TOKEN_WALLET_NOT_FOUND")
        available = wallet["balance_fen"] - wallet["reserved_fen"]
        return {
            "balance_fen": wallet["balance_fen"],
            "reserved_fen": wallet["reserved_fen"],
            "available_fen": available,
        }

    def reserve_task(
        self,
        principal: dict[str, Any],
        request: dict[str, Any],
        idempotency_key: str,
        quoted_fen: int,
    ) -> tuple[dict[str, Any], bool]:
        if not isinstance(idempotency_key, str) or not idempotency_key.strip() or len(idempotency_key.strip()) > 200:
            raise AppError("Idempotency-Key 必填且不能超过 200 字符", 400, "IDEMPOTENCY_KEY_REQUIRED")
        account_id = principal["account_id"]
        idempotency_key = idempotency_key.strip()
        # ponytail: one process lock keeps SQLite and PostgreSQL behavior aligned; use DB advisory locks for multi-process workers.
        with self._lock, self.engine.begin() as connection:
            existing = self._dict(connection.execute(select(self.tasks).where(
                self.tasks.c.customer_id == account_id,
                self.tasks.c.idempotency_key == idempotency_key,
            )).first())
            if existing:
                return existing, True
            wallet = self._dict(connection.execute(select(self.wallets).where(
                self.wallets.c.customer_id == account_id
            ).with_for_update()).first())
            if not wallet or wallet["balance_fen"] - wallet["reserved_fen"] < quoted_fen:
                raise AppError("余额不足", 402, "INSUFFICIENT_BALANCE")
            task_id = "vid_" + uuid.uuid4().hex
            now = utc_now()
            prompt = request.get("prompt") or request["content"][0]["text"]
            task_values = {
                "id": task_id,
                "customer_id": account_id,
                "customer_key_id": principal["key_id"],
                "idempotency_key": idempotency_key,
                "provider_task_id": None,
                "provider_key_id": None,
                "provider_key_label": None,
                "api_version": request["api_version"],
                "model": request["model"],
                "mode": request["mode"],
                "prompt": prompt,
                "duration": request["duration"],
                "resolution": request["resolution"],
                "ratio": request.get("ratio"),
                "request_json": json.dumps(request_for_storage(request), ensure_ascii=False),
                "status": "submitting",
                "quoted_fen": quoted_fen,
                "reserved_fen": quoted_fen,
                "charged_fen": 0,
                "file_id": None,
                "result_url": None,
                "usage_json": None,
                "error": None,
                "created_at": now,
                "updated_at": now,
            }
            connection.execute(insert(self.tasks).values(**task_values))
            connection.execute(update(self.wallets).where(self.wallets.c.customer_id == account_id).values(
                reserved_fen=wallet["reserved_fen"] + quoted_fen,
            ))
            connection.execute(insert(self.ledger).values(
                id="led_" + uuid.uuid4().hex,
                customer_id=account_id,
                task_id=task_id,
                kind="reserve",
                balance_delta_fen=0,
                reserved_delta_fen=quoted_fen,
                reference=f"reserve:{task_id}",
                created_at=now,
            ))
            return task_values, False

    def attach_upstream(self, task_id: str, provider: dict[str, Any]) -> dict[str, Any]:
        values = {
            "provider_task_id": provider["task_id"],
            "provider_key_id": provider.get("key_id"),
            "provider_key_label": provider.get("key_label"),
            "status": "submitted",
            "updated_at": utc_now(),
        }
        with self.engine.begin() as connection:
            connection.execute(update(self.tasks).where(self.tasks.c.id == task_id).values(**values))
        return self.get_task(task_id)

    def update_from_provider(self, task_id: str, result: dict[str, Any]) -> dict[str, Any]:
        values: dict[str, Any] = {
            "status": str(result.get("status") or "unknown").lower(),
            "updated_at": utc_now(),
            "error": result.get("error"),
        }
        for field_name in ("file_id", "result_url"):
            if result.get(field_name):
                values[field_name] = str(result[field_name])
        if isinstance(result.get("usage"), dict):
            values["usage_json"] = json.dumps(result["usage"], ensure_ascii=False)
        with self.engine.begin() as connection:
            connection.execute(update(self.tasks).where(self.tasks.c.id == task_id).values(**values))
        return self.get_task(task_id)

    def release_task(self, task_id: str, status: str, error: str | None) -> dict[str, Any]:
        with self._lock, self.engine.begin() as connection:
            task = self._dict(connection.execute(select(self.tasks).where(
                self.tasks.c.id == task_id
            ).with_for_update()).first())
            if not task:
                raise AppError("任务不存在", 404, "TASK_NOT_FOUND")
            held = task["reserved_fen"]
            if held:
                wallet = self._dict(connection.execute(select(self.wallets).where(
                    self.wallets.c.customer_id == task["customer_id"]
                ).with_for_update()).first())
                connection.execute(update(self.wallets).where(
                    self.wallets.c.customer_id == task["customer_id"]
                ).values(reserved_fen=wallet["reserved_fen"] - held))
                connection.execute(insert(self.ledger).values(
                    id="led_" + uuid.uuid4().hex,
                    customer_id=task["customer_id"],
                    task_id=task_id,
                    kind="release",
                    balance_delta_fen=0,
                    reserved_delta_fen=-held,
                    reference=f"release:{task_id}",
                    created_at=utc_now(),
                ))
            connection.execute(update(self.tasks).where(self.tasks.c.id == task_id).values(
                status=status, error=error, reserved_fen=0, updated_at=utc_now(),
            ))
        return self.get_task(task_id)

    def mark_submission_unknown(self, task_id: str, error: str) -> dict[str, Any]:
        with self.engine.begin() as connection:
            connection.execute(update(self.tasks).where(self.tasks.c.id == task_id).values(
                status="submitting_unknown", error=error, updated_at=utc_now(),
            ))
        return self.get_task(task_id)

    def settle_task(self, task_id: str, charged_fen: int) -> dict[str, Any]:
        with self._lock, self.engine.begin() as connection:
            task = self._dict(connection.execute(select(self.tasks).where(
                self.tasks.c.id == task_id
            ).with_for_update()).first())
            if not task:
                raise AppError("任务不存在", 404, "TASK_NOT_FOUND")
            held = task["reserved_fen"]
            if held == 0:
                return task
            charge = min(max(0, int(charged_fen)), held)
            wallet = self._dict(connection.execute(select(self.wallets).where(
                self.wallets.c.customer_id == task["customer_id"]
            ).with_for_update()).first())
            connection.execute(update(self.wallets).where(
                self.wallets.c.customer_id == task["customer_id"]
            ).values(
                balance_fen=wallet["balance_fen"] - charge,
                reserved_fen=wallet["reserved_fen"] - held,
            ))
            connection.execute(update(self.tasks).where(self.tasks.c.id == task_id).values(
                status="succeeded",
                charged_fen=charge,
                reserved_fen=0,
                updated_at=utc_now(),
            ))
            connection.execute(insert(self.ledger).values(
                id="led_" + uuid.uuid4().hex,
                customer_id=task["customer_id"],
                task_id=task_id,
                kind="capture",
                balance_delta_fen=-charge,
                reserved_delta_fen=-held,
                reference=f"capture:{task_id}",
                created_at=utc_now(),
            ))
        return self.get_task(task_id)

    def get_task(self, task_id: str) -> dict[str, Any]:
        with self.engine.connect() as connection:
            task = self._dict(connection.execute(select(self.tasks).where(self.tasks.c.id == task_id)).first())
        if not task:
            raise AppError("任务不存在", 404, "TASK_NOT_FOUND")
        return task

    def get_token_task(self, account_id: str, task_id: str) -> dict[str, Any]:
        with self.engine.connect() as connection:
            task = self._dict(connection.execute(select(self.tasks).where(
                self.tasks.c.id == task_id,
                self.tasks.c.customer_id == account_id,
            )).first())
        if not task:
            raise AppError("任务不存在", 404, "TASK_NOT_FOUND")
        return task

    def list_token_tasks(self, account_id: str, limit: int = 50) -> list[dict[str, Any]]:
        statement = select(self.tasks).where(
            self.tasks.c.customer_id == account_id
        ).order_by(self.tasks.c.created_at.desc()).limit(min(100, max(1, limit)))
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def list_pending_tasks(self, limit: int = 20) -> list[dict[str, Any]]:
        statement = select(self.tasks).where(
            self.tasks.c.status.in_(sorted(POLLABLE_STATUSES)),
            self.tasks.c.provider_task_id.is_not(None),
        ).order_by(self.tasks.c.updated_at.asc()).limit(min(100, max(1, limit)))
        with self.engine.connect() as connection:
            return [dict(row._mapping) for row in connection.execute(statement)]

    def debug_dump(self) -> str:
        data: dict[str, Any] = {}
        with self.engine.connect() as connection:
            for table in (self.customers, self.customer_keys, self.wallets, self.tokens, self.tasks, self.ledger):
                data[table.name] = [dict(row._mapping) for row in connection.execute(select(table))]
            data[self.provider_keys.name] = [
                {key: value for key, value in row._mapping.items() if key != "secret_value"}
                for row in connection.execute(select(self.provider_keys))
            ]
        return json.dumps(data, ensure_ascii=False, default=str, sort_keys=True)

    def close(self) -> None:
        self.engine.dispose()


class VideoPlatform:
    def __init__(self, store: PlatformStore, provider: MiniMaxService | Any):
        self.store = store
        self.provider = provider

    @staticmethod
    def _public_task(task: dict[str, Any]) -> dict[str, Any]:
        result = {
            key: task.get(key)
            for key in (
                "id", "status", "model", "mode", "prompt", "duration", "resolution", "ratio",
                "quoted_fen", "reserved_fen", "charged_fen", "error", "created_at", "updated_at",
            )
        }
        result["download_url"] = (
            f"/v1/videos/{task['id']}/download"
            if str(task.get("status", "")).lower() in SUCCESS_STATUSES and (task.get("file_id") or task.get("result_url"))
            else None
        )
        if task.get("usage_json"):
            try:
                result["usage"] = json.loads(task["usage_json"])
            except json.JSONDecodeError:
                result["usage"] = None
        else:
            result["usage"] = None
        return result

    def create_video(self, raw_key: str, payload: Any, idempotency_key: str) -> dict[str, Any]:
        principal = self.store.authenticate_token(raw_key)
        request = validate_video_request(payload)
        quoted_fen = quote_video_fen(request["model"], request["duration"], request["resolution"])
        task, existing = self.store.reserve_task(principal, request, idempotency_key, quoted_fen)
        if existing:
            return self._public_task(task)
        try:
            upstream = self.provider.create_task(payload)
            task = self.store.attach_upstream(task["id"], upstream)
        except AppError as exc:
            if exc.code == "UPSTREAM_SUBMIT_UNKNOWN":
                self.store.mark_submission_unknown(task["id"], exc.message)
            else:
                self.store.release_task(task["id"], "failed", exc.message)
            raise
        except Exception as exc:
            self.store.mark_submission_unknown(task["id"], "上游提交结果未知")
            raise AppError("上游提交结果未知，未自动重试", 502, "UPSTREAM_SUBMIT_UNKNOWN") from exc
        return self._public_task(task)

    def account(self, raw_key: str) -> dict[str, Any]:
        principal = self.store.authenticate_token(raw_key)
        return {"token_id": principal["token_id"], **self.store._wallet_for_account(principal["account_id"])}

    def get_video(self, raw_key: str, task_id: str, *, refresh: bool = True) -> dict[str, Any]:
        principal = self.store.authenticate_token(raw_key)
        task = self.store.get_token_task(principal["account_id"], task_id)
        if refresh and str(task["status"]).lower() in POLLABLE_STATUSES and task.get("provider_task_id"):
            task = self._refresh_task(task)
        return self._public_task(task)

    def _refresh_task(self, task: dict[str, Any]) -> dict[str, Any]:
        result = self.provider.query_task(task["provider_task_id"])
        task = self.store.update_from_provider(task["id"], result)
        status = str(task["status"]).lower()
        if status in SUCCESS_STATUSES:
            task = self.store.settle_task(task["id"], task["quoted_fen"])
        elif status in FAIL_STATUSES:
            task = self.store.release_task(task["id"], status, result.get("error"))
        return task

    def poll_pending_once(self, limit: int = 20) -> int:
        attempted = 0
        for task in self.store.list_pending_tasks(limit):
            attempted += 1
            try:
                self._refresh_task(task)
            except AppError:
                # Bound-key and transient query errors stay pending for the next pass.
                continue
        return attempted

    def list_videos(self, raw_key: str, limit: int = 50) -> dict[str, Any]:
        principal = self.store.authenticate_token(raw_key)
        items = [self._public_task(task) for task in self.store.list_token_tasks(principal["account_id"], limit)]
        return {"items": items, "total": len(items)}

    def download_url(self, raw_key: str, task_id: str) -> str:
        principal = self.store.authenticate_token(raw_key)
        task = self.store.get_token_task(principal["account_id"], task_id)
        if str(task["status"]).lower() not in SUCCESS_STATUSES:
            raise AppError("任务尚未成功", 409, "TASK_NOT_COMPLETE")
        if task.get("result_url"):
            return str(task["result_url"])
        if task.get("provider_task_id") and task.get("file_id"):
            return self.provider.download_url(task["provider_task_id"], task["file_id"])
        raise AppError("任务没有可下载的视频", 404, "VIDEO_NOT_FOUND")


def json_error_payload(error: AppError) -> dict[str, Any]:
    return {"error": {"code": error.code, "message": error.message}}


class RequestHandler(BaseHTTPRequestHandler):
    app: "Application"
    server_version = "MiniMaxLocalVideo/1.0"

    def log_message(self, format: str, *args: Any) -> None:
        # Keep request logs free of headers and request bodies.
        print(f"[{self.log_date_time_string()}] {self.command} {self.path} - {format % args}")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path.startswith("/api/") and not parsed.path.startswith("/api/admin/"):
                self._require_legacy_local()
            if parsed.path == "/":
                self._serve_index("token")
            elif parsed.path in {"/admin", "/admin/"}:
                self._require_admin_local()
                self._serve_index("admin")
            elif parsed.path == "/api/admin/keys":
                self._require_admin()
                self._send_json(200, self.app.provider_keys())
            elif parsed.path == "/api/admin/tokens":
                self._require_admin()
                items = self._platform().store.list_tokens()
                self._send_json(200, {"items": items, "total": len(items)})
            elif re.fullmatch(r"/api/admin/tokens/\d+", parsed.path):
                self._require_admin()
                self._send_json(200, self._platform().store.get_token(int(parsed.path.split("/")[4])))
            elif parsed.path == "/v1/models":
                self._send_json(200, {"data": video_model_catalog()})
            elif parsed.path == "/v1/account":
                self._send_json(200, self._platform().account(self._bearer_key()))
            elif parsed.path == "/v1/videos":
                raw_limit = parse_qs(parsed.query).get("limit", ["50"])[0]
                try:
                    limit = min(100, max(1, int(raw_limit)))
                except ValueError as exc:
                    raise AppError("limit 必须是整数", 400, "INVALID_HISTORY_LIMIT") from exc
                self._send_json(200, self._platform().list_videos(self._bearer_key(), limit))
            elif re.fullmatch(r"/v1/videos/[A-Za-z0-9_-]+/download", parsed.path):
                task_id = parsed.path.split("/")[3]
                download_url = self._platform().download_url(self._bearer_key(), task_id)
                self._proxy_video(download_url, task_id)
            elif re.fullmatch(r"/v1/videos/[A-Za-z0-9_-]+", parsed.path):
                task_id = parsed.path.split("/")[3]
                self._send_json(200, self._platform().get_video(self._bearer_key(), task_id, refresh=True))
            elif parsed.path == "/api/status":
                self._send_json(200, self.app.service.status())
            elif parsed.path == "/api/history":
                raw_limit = parse_qs(parsed.query).get("limit", ["50"])[0]
                try:
                    limit = min(100, max(1, int(raw_limit)))
                except ValueError as exc:
                    raise AppError("history limit 必须是整数", 400, "INVALID_HISTORY_LIMIT") from exc
                self._send_json(200, self.app.service.history(limit))
            elif parsed.path.startswith("/api/tasks/"):
                task_id = unquote(parsed.path.removeprefix("/api/tasks/"))
                self._send_json(200, self.app.service.query_task(task_id))
            elif parsed.path == "/api/download":
                query = parse_qs(parsed.query)
                task_id = query.get("task_id", [""])[0]
                file_id = query.get("file_id", [""])[0]
                if not task_id:
                    raise AppError("task_id 是必填项", 400, "DOWNLOAD_PARAMS_REQUIRED")
                download_url = self.app.service.download_url(task_id, file_id)
                self.send_response(302)
                self.send_header("Location", download_url)
                self.send_header("Cache-Control", "no-store")
                self.end_headers()
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": "接口不存在"}})
        except AppError as exc:
            self._send_json(exc.http_status, json_error_payload(exc))
        except Exception:
            self._send_json(500, {"error": {"code": "INTERNAL_ERROR", "message": "本地服务内部错误"}})

    def do_POST(self) -> None:
        try:
            path = urlparse(self.path).path
            if path.startswith("/api/") and not path.startswith("/api/admin/"):
                self._require_legacy_local()
            payload = self._read_json()
            if path == "/api/generate":
                self._send_json(200, self.app.service.create_task(payload))
            elif path == "/v1/videos":
                idempotency_key = self.headers.get("Idempotency-Key", "")
                result = self._platform().create_video(self._bearer_key(), payload, idempotency_key)
                self._send_json(202, result)
            elif path == "/api/admin/keys":
                self._require_admin()
                self._send_json(201, self.app.create_provider_key(payload))
            elif path == "/api/admin/tokens":
                self._require_admin()
                initial_balance = (
                    yuan_to_fen(payload.get("initial_balance_yuan"), allow_zero=True)
                    if "initial_balance_yuan" in payload
                    else payload.get("initial_balance_fen", 0)
                )
                result = self._platform().store.create_token(
                    payload.get("name"), initial_balance,
                )
                self._send_json(201, result)
            elif re.fullmatch(r"/api/admin/tokens/\d+/recharge", path):
                self._require_admin()
                token_id = int(path.split("/")[4])
                reference = payload.get("reference") or "manual:" + uuid.uuid4().hex
                amount = (
                    yuan_to_fen(payload.get("amount_yuan"))
                    if "amount_yuan" in payload
                    else payload.get("amount_fen")
                )
                self._send_json(200, self._platform().store.recharge_token(token_id, amount, reference))
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": "接口不存在"}})
        except AppError as exc:
            self._send_json(exc.http_status, json_error_payload(exc))
        except Exception:
            self._send_json(500, {"error": {"code": "INTERNAL_ERROR", "message": "本地服务内部错误"}})

    def do_PATCH(self) -> None:
        try:
            path = urlparse(self.path).path
            payload = self._read_json()
            if re.fullmatch(r"/api/admin/keys/\d+", path):
                self._require_admin()
                self._send_json(200, self.app.update_provider_key(int(path.split("/")[4]), payload))
            elif re.fullmatch(r"/api/admin/tokens/\d+", path):
                self._require_admin()
                token_id = int(path.split("/")[4])
                result = self._platform().store.update_token(
                    token_id,
                    name=payload.get("name"),
                    enabled=payload.get("enabled"),
                )
                self._send_json(200, result)
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": "接口不存在"}})
        except AppError as exc:
            self._send_json(exc.http_status, json_error_payload(exc))
        except Exception:
            self._send_json(500, {"error": {"code": "INTERNAL_ERROR", "message": "本地服务内部错误"}})

    def do_DELETE(self) -> None:
        try:
            path = urlparse(self.path).path
            if re.fullmatch(r"/api/admin/keys/\d+", path):
                self._require_admin()
                self._send_json(200, self.app.delete_provider_key(int(path.split("/")[4])))
            elif re.fullmatch(r"/api/admin/tokens/\d+", path):
                self._require_admin()
                self._send_json(200, self._platform().store.delete_token(int(path.split("/")[4])))
            else:
                self._send_json(404, {"error": {"code": "NOT_FOUND", "message": "接口不存在"}})
        except AppError as exc:
            self._send_json(exc.http_status, json_error_payload(exc))
        except Exception:
            self._send_json(500, {"error": {"code": "INTERNAL_ERROR", "message": "本地服务内部错误"}})

    def _read_json(self) -> dict[str, Any]:
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise AppError("Content-Length 无效", 400, "INVALID_CONTENT_LENGTH") from exc
        if length <= 0 or length > MAX_REQUEST_BYTES:
            raise AppError("请求体大小无效", 413, "REQUEST_TOO_LARGE")
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AppError("请求体不是有效 JSON", 400, "INVALID_JSON_BODY") from exc
        if not isinstance(payload, dict):
            raise AppError("请求体必须是 JSON 对象", 400, "INVALID_JSON_BODY")
        return payload

    def _platform(self) -> VideoPlatform:
        if self.app.platform is None:
            raise AppError("令牌平台未启用", 503, "PLATFORM_DISABLED")
        return self.app.platform

    def _bearer_key(self) -> str:
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer "):
            raise AppError("缺少 Bearer 访问令牌", 401, "TOKEN_REQUIRED")
        return authorization[7:].strip()

    def _require_legacy_local(self) -> None:
        if self.client_address[0] not in {"127.0.0.1", "::1"}:
            raise AppError("旧版接口仅允许本机访问", 403, "LEGACY_LOCAL_ONLY")

    def _require_admin_local(self) -> None:
        if self.client_address[0] not in {"127.0.0.1", "::1"}:
            raise AppError("管理接口仅允许本机访问", 403, "ADMIN_LOCAL_ONLY")

    def _require_admin(self) -> None:
        self._require_admin_local()
        configured = os.environ.get("MINIMAX_ADMIN_TOKEN")
        supplied = self.headers.get("X-Admin-Token", "")
        if configured and not secrets.compare_digest(configured, supplied):
            raise AppError("管理令牌无效", 401, "INVALID_ADMIN_TOKEN")

    def _serve_index(self, view: str) -> None:
        path = Path(__file__).with_name("index.html")
        try:
            content = path.read_text(encoding="utf-8").replace("__VIEW__", view).encode("utf-8")
        except OSError:
            self._send_json(500, {"error": {"code": "INDEX_NOT_FOUND", "message": "找不到网页文件"}})
            return
        self.send_response(200)
        self.send_header("Content-Type", mimetypes.guess_type(path.name)[0] or "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)

    def _proxy_video(self, download_url: str, task_id: str) -> None:
        parsed = urlparse(download_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise AppError("视频下载地址无效", 502, "INVALID_DOWNLOAD_URL")
        try:
            request = Request(download_url, headers={"User-Agent": "MiniMaxVideoPlatform/1.0"})
            with urlopen(request, timeout=60) as response:
                self.send_response(200)
                self.send_header("Content-Type", response.headers.get_content_type() or "video/mp4")
                content_length = response.headers.get("Content-Length")
                if content_length:
                    self.send_header("Content-Length", content_length)
                self.send_header("Content-Disposition", f'attachment; filename="{task_id}.mp4"')
                self.send_header("Cache-Control", "private, no-store")
                self.end_headers()
                while chunk := response.read(64 * 1024):
                    self.wfile.write(chunk)
        except (HTTPError, URLError, TimeoutError, socket.timeout, ConnectionError) as exc:
            raise AppError("视频下载暂时不可用", 502, "VIDEO_DOWNLOAD_FAILED") from exc

    def _send_json(self, status: int, payload: dict[str, Any]) -> None:
        content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(content)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(content)


class Application:
    def __init__(self, service: MiniMaxService, platform: VideoPlatform | None = None):
        self.service = service
        self.platform = platform

    def _store(self) -> PlatformStore:
        return self._platform().store

    def _platform(self) -> VideoPlatform:
        if self.platform is None:
            raise AppError("令牌平台未启用", 503, "PLATFORM_DISABLED")
        return self.platform

    def reload_provider_keys(self) -> None:
        self.service.replace_key_pool(KeyPool.from_records(self._store().load_provider_keys()))

    def provider_keys(self) -> dict[str, Any]:
        status = self.service.status()
        states = {item["key_id"]: item for item in status["keys"]}
        status["keys"] = [
            {**item, **states.get(item["key_id"], {})}
            for item in self._store().list_provider_keys()
        ]
        return status

    def create_provider_key(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = self._store().create_provider_key(payload.get("label"), payload.get("api_key"))
        self.reload_provider_keys()
        return result

    def update_provider_key(self, provider_key_id: int, payload: dict[str, Any]) -> dict[str, Any]:
        result = self._store().update_provider_key(
            provider_key_id,
            label=payload.get("label"),
            enabled=payload.get("enabled"),
            secret_value=payload.get("api_key"),
        )
        self.reload_provider_keys()
        return result

    def delete_provider_key(self, provider_key_id: int) -> dict[str, Any]:
        provider_key = self._store().get_provider_key(provider_key_id)
        key_id = provider_key["key_id"]
        if self.service.task_store.references_key(key_id) or self._store().provider_key_in_use(key_id):
            raise AppError("该 Key 已绑定历史任务，请停用而不是删除", 409, "PROVIDER_KEY_IN_USE")
        result = self._store().delete_provider_key(provider_key_id)
        self.reload_provider_keys()
        return result


def main() -> None:
    database_url = os.environ.get("DATABASE_URL") or f"sqlite+pysqlite:///{PLATFORM_DB_PATH.as_posix()}"
    platform_store = PlatformStore(database_url)
    platform_store.create_schema()
    if "MINIMAX_API_KEYS" in os.environ:
        legacy_values = parse_key_values(os.environ.get("MINIMAX_API_KEYS"))
    elif "MINIMAX_API_KEY" in os.environ:
        legacy_values = parse_key_values(os.environ.get("MINIMAX_API_KEY"))
    else:
        legacy_values = load_dotenv_key_values()
    imported_keys = platform_store.seed_provider_keys(legacy_values)
    pool = KeyPool.from_records(platform_store.load_provider_keys())
    h3_pool = KeyPool.optional_from_environment("MINIMAX_PAYGO_API_KEYS", "MINIMAX_PAYGO_API_KEY")
    store = TaskStore()
    service = MiniMaxService(pool, store, h3_key_pool=h3_pool)
    platform = VideoPlatform(platform_store, service)
    app = Application(service, platform)

    try:
        poll_seconds = max(3, int(os.environ.get("MINIMAX_POLL_SECONDS", "10")))
    except ValueError:
        poll_seconds = 10
    stop_poller = threading.Event()

    def poll_loop() -> None:
        while not stop_poller.wait(poll_seconds):
            try:
                platform.poll_pending_once()
            except Exception:
                print("后台任务轮询发生本地错误，将在下一轮重试")

    poller = threading.Thread(target=poll_loop, name="minimax-task-poller", daemon=True)
    poller.start()

    handler_type = type("MiniMaxRequestHandler", (RequestHandler,), {"app": app})
    host = os.environ.get("MINIMAX_HOST", "127.0.0.1")
    port = int(os.environ.get("MINIMAX_PORT", "8000"))
    server = ThreadingHTTPServer((host, port), handler_type)
    print(f"MiniMax 视频工具已启动：http://{host}:{port}")
    print(f"已从数据库加载 {pool.status()['configured_keys']} 个 Key，首次导入 {imported_keys} 个，任务绑定文件：{TASKS_PATH}")
    print("令牌平台 API 已启用；管理接口仅接受本机请求")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n服务已停止")
    finally:
        stop_poller.set()
        poller.join(timeout=2)
        server.server_close()
        platform_store.close()


if __name__ == "__main__":
    main()
