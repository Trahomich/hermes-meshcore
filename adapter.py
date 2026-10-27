"""meshcore platform adapter: MeshCore (LoRa mesh) companion node over TCP.

Архитектура (v3, рефакторинг после двух недель боевых находок):

- ПРИЁМ: постоянных сессий НЕТ. Каждые CYCLE_SECONDS — лёгкий цикл:
  connect (appstart+self_info, ~2 команды) → выгрузка очереди get_msg до
  NO_MORE_MSGS → disconnect. Нода отдаёт очередь только самому свежему
  клиенту и «зомбит» долгоживущие сессии — короткие циклы гарантируют
  наши сообщения. Отправки (свои короткие соединения) ничего не крадут:
  следующий цикл вернёт очередь нам. Латентность приёма ≤ CYCLE_SECONDS.
- ОТПРАВКА: одноразовое соединение, один plain send_msg (проверенный
  путь), жёсткий бюджет на всё.
- Никаких: ротаций с полным рукопожатием (душили ноду), send_msg_with_retry
  (терял ответы), ожиданий ACK (клиент их не шлёт), is_connected (врёт),
  бесконечных ожиданий (всё ограничено таймаутами).

config.yaml platforms.meshcore.extra: host (required), port (5000),
reply_limit (130), listen_channels ("" = выкл), channel_triggers
(бот,bot,aibot). Env: MESHCORE_HOST, MESHCORE_PORT, MESHCORE_REPLY_LIMIT,
MESHCORE_LISTEN_CHANNELS, MESHCORE_CHANNEL_TRIGGERS, MESHCORE_ALLOWED_USERS,
MESHCORE_ALLOW_ALL_USERS, MESHCORE_HOME_CHANNEL(_NAME).
"""

import asyncio
import hashlib
import logging
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Dict, Optional

try:
    from meshcore import MeshCore
    from meshcore.events import EventType
    MESHCORE_AVAILABLE = True
except ImportError:
    MESHCORE_AVAILABLE = False
    MeshCore = None  # type: ignore[assignment]
    EventType = None  # type: ignore[assignment]

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.platforms.event import MessageEvent, MessageType
from gateway.platforms.helpers import MessageDeduplicator
from gateway.platforms._shared import (
    get_scoped_secret as _get_scoped_secret, send_error,
    extra_or_secret as _extra_or_secret, seed_extra_from_env as _seed_extra_from_env,
)

logger = logging.getLogger(__name__)

DEFAULT_HOST = "192.168.99.23"
DEFAULT_PORT = 5000
DEFAULT_REPLY_LIMIT = 130
DEFAULT_CHANNEL_TRIGGERS = "бот,bot,aibot"

CYCLE_SECONDS = 30.0        # период приёмного цикла (латентность приёма)
CYCLE_BUDGET_SECONDS = 25.0 # жёсткий потолок одного цикла (либа висит без таймаутов)
DRAIN_MAX_MESSAGES = 80     # сообщений за цикл максимум
COMMAND_TIMEOUT = 8.0       # таймаут одной команды ноде
SEND_BUDGET_SECONDS = 90.0  # жёсткий потолок всей отправки
CHANNEL_SLOTS = 8           # слоты каналов (обнаружение разовое)
CHANNEL_QUERY_TIMEOUT = 5.0
DEDUP_WINDOW = 300
DEDUP_SIZE = 500

# Служебные уведомления hermes не уходят в эфир
_SERVICE_MARKERS = ("Фоновая задача", "Recovered reply", "Прерываю текущую задачу")
_SERVICE_PREFIXES = ("[IMPORTANT:", "✅ Фоновая", "❌ Фоновая", "🔄 Фоновая", "♻️")


def _norm_chan(name: str) -> str:
    return (name or "").strip().lower().lstrip("#")


def _is_service_message(text: str) -> bool:
    stripped = text.lstrip()
    return any(stripped.startswith(p) for p in _SERVICE_PREFIXES) or any(
        m in text for m in _SERVICE_MARKERS)


def _strip_recovery_marker(text: str) -> str:
    """Повторная доставка реального ответа: снять ♻️-обёртку, доставить тело."""
    if not text.startswith("♻️"):
        return text
    for sep in (":\n\n", ":\n"):
        idx = text.find(sep)
        if idx != -1:
            return text[idx + len(sep):].strip()
    return ""


def _host_value(extra: Dict[str, Any]) -> str:
    return _extra_or_secret(extra, "host", "MESHCORE_HOST", DEFAULT_HOST).strip()


def check_requirements() -> bool:
    return MESHCORE_AVAILABLE and bool(_get_scoped_secret("MESHCORE_HOST", "").strip())


def validate_config(config) -> bool:
    return bool(_host_value(getattr(config, "extra", {}) or {}))


def is_connected(config) -> bool:
    return bool(
        _get_scoped_secret("MESHCORE_HOST", "").strip()
        or _host_value(getattr(config, "extra", {}) or {}) != DEFAULT_HOST
    )


class MeshcoreAdapter(BasePlatformAdapter):
    """MeshCore adapter: приём циклами выгрузки, отправка одним send_msg."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform("meshcore"))
        extra = config.extra or {}
        self._host = _host_value(extra) or DEFAULT_HOST
        self._port = DEFAULT_PORT
        with suppress((TypeError, ValueError)):
            self._port = int(_extra_or_secret(extra, "port", "MESHCORE_PORT", DEFAULT_PORT))
        self._reply_limit = DEFAULT_REPLY_LIMIT
        with suppress((TypeError, ValueError)):
            self._reply_limit = int(_extra_or_secret(
                extra, "reply_limit", "MESHCORE_REPLY_LIMIT", DEFAULT_REPLY_LIMIT))
        raw_channels = str(_extra_or_secret(
            extra, "listen_channels", "MESHCORE_LISTEN_CHANNELS", "") or "")
        self._listen_channels = {_norm_chan(c) for c in raw_channels.split(",") if _norm_chan(c)}
        raw_triggers = str(_extra_or_secret(
            extra, "channel_triggers", "MESHCORE_CHANNEL_TRIGGERS",
            DEFAULT_CHANNEL_TRIGGERS) or "")
        self._channel_triggers = [t.strip().lower() for t in raw_triggers.split(",") if t.strip()]
        self._chan_names: Dict[int, str] = {}
        self._dedup = MessageDeduplicator(max_size=DEDUP_SIZE, ttl_seconds=DEDUP_WINDOW)
        self._rx_task: Optional[asyncio.Task] = None

    # -- Lifecycle ----------------------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not MESHCORE_AVAILABLE:
            logger.warning("[%s] meshcore library not installed", self.name)
            return False
        if not self._host:
            logger.warning("[%s] MESHCORE_HOST not configured", self.name)
            return False
        self._rx_task = asyncio.create_task(self._rx_loop())
        self._mark_connected()
        self._wire_plugin_handlers(None)
        return True

    async def disconnect(self) -> None:
        self._running = False
        self._mark_disconnected()
        if self._rx_task:
            self._rx_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._rx_task
            self._rx_task = None
        logger.info("[%s] Disconnected", self.name)

    # -- Приём: циклы выгрузки очереди ---------------------------------------

    async def _rx_loop(self) -> None:
        """Каждый цикл — свежее лёгкое подключение и выгрузка очереди до пустоты."""
        backoff = 0
        while self._running:
            try:
                drained = await asyncio.wait_for(
                    self._drain_cycle(), timeout=CYCLE_BUDGET_SECONDS)
                backoff = 0
                if drained:
                    logger.info("[%s] Cycle drained %d message(s)", self.name, drained)
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.warning("[%s] Cycle error: %s", self.name, e)
            delay = CYCLE_SECONDS if backoff == 0 else min(CYCLE_SECONDS * backoff, 120.0)
            with suppress(asyncio.TimeoutError, asyncio.CancelledError):
                await asyncio.wait_for(asyncio.sleep(delay), timeout=delay + 1)
            backoff = min(backoff + 1, 4)

    async def _drain_cycle(self) -> int:
        """Лёгкий цикл: connect → get_msg до NO_MORE → disconnect."""
        mc = await MeshCore.create_tcp(self._host, self._port)
        mc.default_timeout = COMMAND_TIMEOUT
        await mc.connect()
        count = 0
        try:
            if self._listen_channels and not self._chan_names:
                with suppress(Exception):
                    await self._discover_channels_once(mc)
            for _ in range(DRAIN_MAX_MESSAGES):
                ev = await mc.commands.get_msg(timeout=COMMAND_TIMEOUT)
                if ev is None or ev.type in (EventType.NO_MORE_MSGS, EventType.ERROR):
                    break
                count += 1
                await self._dispatch(getattr(ev, "payload", None) or {})
                await asyncio.sleep(0.05)
        finally:
            with suppress(Exception):
                await mc.disconnect()
        return count

    async def _dispatch(self, payload: Dict[str, Any]) -> None:
        """Разобрать выгруженное сообщение: DM всегда, каналы — по триггеру."""
        ptype = payload.get("type")
        if ptype == "PRIV":
            await self._handle_dm(payload)
        elif ptype == "CHAN":
            await self._handle_channel(payload)

    async def _handle_dm(self, payload: Dict[str, Any]) -> None:
        if payload.get("txt_type") not in (None, 0, 2):
            return  # бинарные/файлы — мимо
        text = str(payload.get("text") or "").strip()
        if not text:
            return
        prefix = str(payload.get("pubkey_prefix") or "").strip()
        chat_id = prefix or "unknown"
        msg_id = hashlib.sha256(
            f"{chat_id}|{text}|{payload.get('sender_timestamp', '')}".encode()
        ).hexdigest()[:16]
        if self._dedup.is_duplicate(msg_id):
            return
        name = chat_id[:8]
        source = self.build_source(
            chat_id=chat_id, chat_name=name, chat_type="dm",
            user_id=chat_id, user_name=name, message_id=msg_id)
        timestamp = self._ts(payload)
        # [id:pubkey] — криптографическая метка отправителя (права админа)
        await self.handle_message(MessageEvent(
            text=f"[id:{chat_id}] {text}", message_type=MessageType.TEXT,
            source=source, message_id=msg_id, raw_message=payload, timestamp=timestamp))
        logger.info("[%s] DM from %s: %s", self.name, name, text[:50])

    async def _handle_channel(self, payload: Dict[str, Any]) -> None:
        if not self._listen_channels:
            return
        idx = payload.get("channel_idx")
        name = self._chan_names.get(idx, "")
        if name not in self._listen_channels and str(idx) not in self._listen_channels:
            return
        text = str(payload.get("text") or "").strip()
        if not text:
            return
        low = text.lower()
        if not any(t in low for t in self._channel_triggers):
            return  # бота не звали
        msg_id = hashlib.sha256(
            f"chan|{idx}|{text}|{payload.get('sender_timestamp', '')}".encode()
        ).hexdigest()[:16]
        if self._dedup.is_duplicate(msg_id):
            return
        label = f"#{name}" if name else f"#chan{idx}"
        source = self.build_source(
            chat_id=f"chan:{idx}", chat_name=label, chat_type="group",
            user_id=f"chan:{idx}", user_name=label, message_id=msg_id)
        await self.handle_message(MessageEvent(
            text=text, message_type=MessageType.TEXT, source=source,
            message_id=msg_id, raw_message=payload, timestamp=self._ts(payload)))
        logger.info("[%s] Channel %s trigger: %s", self.name, label, text[:50])

    @staticmethod
    def _ts(payload: Dict[str, Any]) -> datetime:
        ts = payload.get("sender_timestamp")
        with suppress((ValueError, OSError, TypeError, OverflowError)):
            if isinstance(ts, (int, float)) and ts > 0:
                return datetime.fromtimestamp(ts, tz=timezone.utc)
        return datetime.now(tz=timezone.utc)

    async def _discover_channels_once(self, mc) -> None:
        """Разовое обнаружение имён каналов (5с на запрос)."""
        saved = mc.default_timeout
        mc.default_timeout = CHANNEL_QUERY_TIMEOUT
        names: Dict[int, str] = {}
        try:
            for idx in range(CHANNEL_SLOTS):
                with suppress(Exception):
                    ev = await mc.commands.get_channel(idx)
                    name = ((getattr(ev, "payload", None) or {}).get("channel_name"))
                    if name:
                        names[idx] = _norm_chan(name)
        finally:
            mc.default_timeout = saved
        self._chan_names = names

    # -- Отправка ------------------------------------------------------------

    async def send(
        self, chat_id: str, content: str, reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Один plain send_msg по свежему соединению; весь процесс ограничен."""
        try:
            return await asyncio.wait_for(
                self._send_flow(chat_id, content), timeout=SEND_BUDGET_SECONDS)
        except asyncio.TimeoutError:
            logger.error("[%s] Send budget exceeded", self.name)
            return SendResult(success=False, error="meshcore: send budget exceeded")

    async def _send_flow(self, chat_id: str, content: str) -> SendResult:
        text = _strip_recovery_marker((content or "").strip())
        if not text:
            return SendResult(success=False, error="meshcore: empty message")
        if _is_service_message(text):
            logger.info("[%s] Dropping service notification (%d chars)", self.name, len(text))
            return SendResult(success=True, message_id=uuid.uuid4().hex[:12])
        if len(text) > self._reply_limit:
            logger.warning("[%s] Reply truncated %d→%d", self.name, len(text), self._reply_limit)
            text = text[:self._reply_limit].rstrip()

        chan_idx: Optional[int] = None
        if chat_id.startswith("chan:"):
            with suppress(ValueError):
                chan_idx = int(chat_id.split(":", 1)[1])
            if chan_idx is None:
                return SendResult(success=False, error=f"meshcore: bad channel id '{chat_id}'")

        mc = await MeshCore.create_tcp(self._host, self._port)
        mc.default_timeout = 15.0
        try:
            await mc.connect()
            if chan_idx is not None:
                result = await mc.commands.send_chan_msg(chan_idx, text)
            else:
                with suppress(Exception):
                    await mc.ensure_contacts()
                destination = None
                with suppress(Exception):
                    destination = mc.get_contact_by_key_prefix(chat_id)
                if destination is None:
                    with suppress(Exception):
                        destination = mc.get_contact_by_name(chat_id)
                if destination is None:
                    with suppress(ValueError):
                        bytes.fromhex(chat_id)
                        destination = chat_id
                if destination is None:
                    return SendResult(
                        success=False, error=f"meshcore: contact '{chat_id}' not found")
                result = await mc.commands.send_msg(destination, text)
        finally:
            with suppress(Exception):
                await mc.disconnect()
        if getattr(result, "type", None) == EventType.ERROR:
            return SendResult(
                success=False,
                error=f"meshcore node error: {getattr(result, 'payload', None)}")
        logger.info("[%s] Reply sent (%d chars)", self.name, len(text))
        return SendResult(success=True, message_id=uuid.uuid4().hex[:12])

    async def send_typing(self, chat_id: str, **_kwargs) -> None:
        """LoRa не имеет индикатора набора — no-op."""

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        if chat_id.startswith("chan:"):
            with suppress(ValueError):
                name = self._chan_names.get(int(chat_id.split(":", 1)[1]), "")
                return {"name": f"#{name}" if name else chat_id, "type": "group"}
        return {"name": chat_id, "type": "dm"}


# -- Plugin registration -----------------------------------------------------


def _parse_target_ref(target_ref: str):
    ref = (target_ref or "").strip()
    return (ref, None) if ref else None


def _env_enablement() -> dict | None:
    host = _get_scoped_secret("MESHCORE_HOST", "").strip()
    if not host:
        return None
    seed = _seed_extra_from_env((
        ("MESHCORE_PORT", "port", None),
        ("MESHCORE_REPLY_LIMIT", "reply_limit", None),
        ("MESHCORE_LISTEN_CHANNELS", "listen_channels", None),
        ("MESHCORE_CHANNEL_TRIGGERS", "channel_triggers", None),
    ), home_env="MESHCORE_HOME_CHANNEL", home_default=None)
    extra: Dict[str, Any] = {"host": host}
    with suppress((TypeError, ValueError)):
        extra["port"] = int(seed.pop("port", DEFAULT_PORT))
    with suppress((TypeError, ValueError)):
        extra["reply_limit"] = int(seed.pop("reply_limit", DEFAULT_REPLY_LIMIT))
    extra.update(seed)
    return extra


async def _standalone_send(
    pconfig, chat_id: str, message: str, *,
    thread_id: Optional[str] = None, media_files: Optional[list] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    if not MESHCORE_AVAILABLE:
        return send_error("meshcore standalone send: meshcore library not installed")
    extra = getattr(pconfig, "extra", {}) or {}
    host = _host_value(extra)
    if not host:
        return send_error("meshcore standalone send: MESHCORE_HOST not configured")
    with suppress((TypeError, ValueError)):
        port = int(_extra_or_secret(extra, "port", "MESHCORE_PORT", DEFAULT_PORT))
    with suppress((TypeError, ValueError)):
        reply_limit = int(_extra_or_secret(
            extra, "reply_limit", "MESHCORE_REPLY_LIMIT", DEFAULT_REPLY_LIMIT))
    home = extra.get("home_channel") if isinstance(extra.get("home_channel"), dict) else {}
    target = (chat_id or _get_scoped_secret("MESHCORE_HOME_CHANNEL", "").strip()
              or (home or {}).get("id") or "").strip()
    if not target:
        return send_error("meshcore standalone send: no target contact")
    text = _strip_recovery_marker((message or "").strip())[:reply_limit]
    if not text:
        return send_error("meshcore standalone send: empty message")
    mc = None
    try:
        mc = await MeshCore.create_tcp(host, port)
        mc.default_timeout = 15.0
        await mc.connect()
        with suppress(Exception):
            await mc.ensure_contacts()
        destination = None
        with suppress(Exception):
            destination = mc.get_contact_by_name(target)
        if destination is None:
            with suppress(Exception):
                destination = mc.get_contact_by_key_prefix(target)
        if destination is None:
            with suppress(ValueError):
                bytes.fromhex(target)
                destination = target
        if destination is None:
            return send_error(f"meshcore standalone send: contact '{target}' not found")
        await mc.commands.send_msg(destination, text)
        return {"success": True, "platform": "meshcore", "chat_id": target,
                "message_id": uuid.uuid4().hex[:12]}
    except Exception as e:
        return send_error(f"meshcore standalone send failed: {e}")
    finally:
        if mc is not None:
            with suppress(Exception):
                await mc.disconnect()


def register(ctx) -> None:
    ctx.register_platform(
        name="meshcore", label="meshcore",
        adapter_factory=lambda cfg: MeshcoreAdapter(cfg),
        check_fn=check_requirements, validate_config=validate_config,
        is_connected=is_connected,
        required_env=["MESHCORE_HOST"],
        install_hint="pip install meshcore",
        parse_target_ref_fn=_parse_target_ref,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MESHCORE_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="MESHCORE_ALLOWED_USERS", allow_all_env="MESHCORE_ALLOW_ALL_USERS",
        max_message_length=DEFAULT_REPLY_LIMIT, emoji="📡",
        pii_safe=False, allow_update_command=True,
        platform_hint=(
            "You are communicating over MeshCore, a long-range low-bandwidth LoRa mesh "
            "radio network. Keep every reply under 130 characters of plain text — this is "
            "a hard adapter-side limit. No attachments, no typing indicators; delivery can "
            "take tens of seconds. Reply in the user's language; use weather emoji for "
            "weather answers."
        ))
