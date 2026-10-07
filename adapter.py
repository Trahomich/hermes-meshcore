"""meshcore platform adapter: MeshCore (LoRa mesh) companion node over TCP.

Connects to one MeshCore companion radio node via the ``meshcore`` Python
library (TCP transport), answers private messages (``CONTACT_MSG_RECV``) from
mesh contacts, and sends replies back over the air. Outgoing replies are
hard-truncated to ``MESHCORE_REPLY_LIMIT`` characters (default 130) — LoRa
airtime is precious and the bot's persona mandates short answers.

config.yaml ``platforms.meshcore.extra``: ``host`` (required), ``port``
(default 5000), ``reply_limit`` (default 130). Env (read at construct time;
``extra`` wins over env): MESHCORE_HOST, MESHCORE_PORT, MESHCORE_REPLY_LIMIT,
MESHCORE_ALLOWED_USERS (contact names), MESHCORE_ALLOW_ALL_USERS,
MESHCORE_HOME_CHANNEL (contact name for cron delivery),
MESHCORE_HOME_CHANNEL_NAME.

Identity: MeshCore contacts are identified by their public-key prefix (hex).
``chat_id`` == pubkey prefix; cron/home delivery targets a contact by name.
Unknown senders with no stored contact are answered via the raw key prefix
(MeshCore encrypts to the pubkey, a contact-list entry is not required).
"""

import asyncio
import hashlib
import logging
import time
import uuid
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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
from gateway.platforms._shared import (
    get_scoped_secret as _get_scoped_secret, send_error,
    extra_or_secret as _extra_or_secret, seed_extra_from_env as _seed_extra_from_env,
)

logger = logging.getLogger(__name__)

DEFAULT_HOST = "192.168.99.23"
DEFAULT_PORT = 5000
DEFAULT_REPLY_LIMIT = 130  # жесткий лимит ответов бота (символы)
# Публичный бот: служебные уведомления hermes (фоновые задачи, статусы,
# [IMPORTANT:]-вставки, ♻️ Recovered reply от delivery ledger) не должны
# попадать в эфир — фильтруем на входе в send
_SERVICE_MARKERS = ("Фоновая задача", "Recovered reply")
_SERVICE_PREFIXES = ("[IMPORTANT:", "✅ Фоновая", "❌ Фоновая", "🔄 Фоновая", "♻️")


def _is_service_message(text: str) -> bool:
    stripped = text.lstrip()
    return any(stripped.startswith(p) for p in _SERVICE_PREFIXES) or any(
        m in text for m in _SERVICE_MARKERS)
RECONNECT_BACKOFF = [1, 2, 5, 15, 30]
KEEPALIVE_CHECK_SECONDS = 15.0  # период проверки dropped-события
RECONNECT_WAIT_SECONDS = 15.0  # сколько send() ждёт реконнекта при обрыве
SEND_TIMEOUT_SECONDS = 60.0  # LoRa медленный: MSG_SENT может идти десятки секунд
CONTACT_CONNECT_ATTEMPTS = 3  # companion TCP отдаёт контакты через раз — ретраим соединением
# Нода (fw v1.17.1) шлёт уведомление MESSAGES_WAITING только самому свежему
# подключившемуся TCP-клиенту. Любой посторонний клиент (CLI-запрос, standalone
# send) перехватывает слот. Плановая ротация соединения возвращает слот адаптеру.
RESLOT_INTERVAL_SECONDS = 60.0
DRAIN_MAX_MESSAGES = 50  # лимит принудительной выгрузки при подключении


async def _connect_with_contacts(host: str, port: int) -> "MeshCore":
    """Соединение с нодой, пока не получим список контактов (или attempts исчерпаны).

    Замеченная особенность companion TCP: первый запрос после переподключения
    иногда молча теряется (ensure_contacts отрабатывает без ошибки, контактов 0).
    Переподключение решает; сырые hex-цели работают и без контактов.
    """
    mc = None
    for attempt in range(CONTACT_CONNECT_ATTEMPTS):
        mc = await MeshCore.create_tcp(host, port)
        mc.default_timeout = SEND_TIMEOUT_SECONDS
        await mc.connect()
        with suppress(Exception):
            await mc.ensure_contacts()
        if len(getattr(mc, "contacts", {}) or {}):
            return mc
        logger.debug("meshcore: connect attempt %d returned no contacts, retrying", attempt + 1)
        with suppress(Exception):
            await mc.disconnect()
        await asyncio.sleep(1.0)
    return mc


def _host_value(extra: Dict[str, Any]) -> str:
    return _extra_or_secret(extra, "host", "MESHCORE_HOST", DEFAULT_HOST).strip()


def check_requirements() -> bool:
    """Библиотека установлена и хост ноды задан (без полной загрузки конфига)."""
    return MESHCORE_AVAILABLE and bool(_get_scoped_secret("MESHCORE_HOST", "").strip())


def validate_config(config) -> bool:
    """Хост задан (config.yaml ``extra`` или env)."""
    return bool(_host_value(getattr(config, "extra", {}) or {}))


def is_connected(config) -> bool:
    """Платформа сконфигурирована (env или config.yaml)."""
    return bool(
        _get_scoped_secret("MESHCORE_HOST", "").strip()
        or _host_value(getattr(config, "extra", {}) or {}) != DEFAULT_HOST
    )


class MeshcoreAdapter(BasePlatformAdapter):
    """MeshCore adapter: TCP → companion node, private messages in/out."""

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform("meshcore"))
        extra = config.extra or {}
        self._host: str = _host_value(extra) or DEFAULT_HOST
        try:
            self._port: int = int(_extra_or_secret(extra, "port", "MESHCORE_PORT", DEFAULT_PORT))
        except (TypeError, ValueError):
            self._port = DEFAULT_PORT
        try:
            self._reply_limit: int = int(
                _extra_or_secret(extra, "reply_limit", "MESHCORE_REPLY_LIMIT", DEFAULT_REPLY_LIMIT))
        except (TypeError, ValueError):
            self._reply_limit = DEFAULT_REPLY_LIMIT
        self._mc: Optional["MeshCore"] = None
        self._run_task: Optional[asyncio.Task] = None
        # будильник: send() при обрыве будит петлю реконнекта немедленно
        self._wake = asyncio.Event()

    # -- Connection lifecycle -----------------------------------------------

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        """Подключение к ноде: фоновая задача с автоматическим реконнектом."""
        if not MESHCORE_AVAILABLE:
            logger.warning("[%s] meshcore library not installed. Run: pip install meshcore", self.name)
            return False
        if not self._host:
            logger.warning("[%s] MESHCORE_HOST not configured", self.name)
            return False
        self._wake.clear()
        self._run_task = asyncio.create_task(self._run_loop())
        self._mark_connected()
        self._wire_plugin_handlers(None)
        return True

    async def _run_loop(self) -> None:
        """Надзор над сессиями: реконнект с нарастающим backoff."""
        backoff_idx = 0
        session_start = 0.0
        while self._running:
            planned_rotation = False
            try:
                session_start = time.monotonic()
                await self._session()
                planned_rotation = True  # нормальный выход из _session = плановая ротация
            except asyncio.CancelledError:
                return
            except Exception as e:
                if not self._running:
                    return
                logger.warning("[%s] Session error: %s", self.name, e)
            if not self._running:
                return
            if planned_rotation:
                # ротация слота уведомлений — переподключаемся без задержки
                backoff_idx = 0
                continue
            # сессия жила стабильно — сбрасываем backoff
            if time.monotonic() - session_start >= 30.0:
                backoff_idx = 0
            if self._wake.is_set():
                # ждёт ответ доставки — реконнектим немедленно
                self._wake.clear()
                logger.info("[%s] Immediate reconnect (reply pending)", self.name)
            else:
                delay = RECONNECT_BACKOFF[min(backoff_idx, len(RECONNECT_BACKOFF) - 1)]
                logger.info("[%s] Reconnecting to %s:%s in %ds...", self.name, self._host, self._port, delay)
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=delay)
            backoff_idx += 1

    async def _session(self) -> None:
        """Одна TCP-сессия с нодой: подписки + контроль связи."""
        mc = await _connect_with_contacts(self._host, self._port)
        self._mc = mc
        try:
            mc.auto_update_contacts = True
            mc.subscribe(EventType.CONTACT_MSG_RECV, self._on_message)
            dropped = asyncio.Event()
            mc.subscribe(EventType.DISCONNECTED, lambda _ev: dropped.set())
            await mc.start_auto_message_fetching()
            # Полный дрейн: lib при подписке забирает только одно сообщение,
            # а уведомление о накопившихся мог уйти другому/ушедшему клиенту
            with suppress(Exception):
                for _ in range(DRAIN_MAX_MESSAGES):
                    ev = await mc.commands.get_msg(timeout=10)
                    if ev is None or ev.type in (EventType.NO_MORE_MSGS, EventType.ERROR):
                        break
                    await asyncio.sleep(0.1)
            node_name = (mc.self_info or {}).get("name") or "?"
            n_contacts = len(getattr(mc, "contacts", {}) or {})
            logger.info(
                "[%s] Connected to companion '%s' at %s:%s (%d contacts)",
                self.name, node_name, self._host, self._port, n_contacts)
            # ВАЖНО: mc.is_connected у meshcore 2.3.15 на TCP-транспорте всегда
            # False даже на живом соединении — не использовать как признак жизни.
            # Живём до события DISCONNECTED; плановая ротация — по таймеру RESLOT.
            session_deadline = time.monotonic() + RESLOT_INTERVAL_SECONDS
            while self._running:
                sleep_task = asyncio.create_task(asyncio.sleep(KEEPALIVE_CHECK_SECONDS))
                drop_task = asyncio.create_task(dropped.wait())
                try:
                    _done, _pending = await asyncio.wait(
                        {sleep_task, drop_task}, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    for task in (sleep_task, drop_task):
                        task.cancel()
                if not self._running:
                    return
                if drop_task in _done and dropped.is_set():
                    raise ConnectionError("companion TCP connection lost")
                if time.monotonic() >= session_deadline:
                    logger.debug("[%s] Planned reslot reconnect", self.name)
                    return  # плановая ротация: _run_loop переподключится сразу
        finally:
            self._mc = None
            with suppress(Exception):
                await mc.stop_auto_message_fetching()
            with suppress(Exception):
                await mc.disconnect()

    async def disconnect(self) -> None:
        """Отключение от ноды."""
        self._running = False
        self._mark_disconnected()
        if self._run_task:
            self._run_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._run_task
            self._run_task = None
        mc, self._mc = self._mc, None
        if mc is not None:
            with suppress(Exception):
                await mc.stop_auto_message_fetching()
            with suppress(Exception):
                await mc.disconnect()
        logger.info("[%s] Disconnected", self.name)

    # -- Inbound message processing -----------------------------------------

    async def _on_message(self, event) -> None:
        """Входящее личное сообщение из эфира → агент."""
        payload = getattr(event, "payload", None) or {}
        # Только личные текстовые сообщения; CHAN/DATA игнорируем
        if payload.get("type") not in (None, "PRIV"):
            return
        # txt_type: 0 = текст, 1 = бинарные/application-данные (передача
        # файлов, чанки, ACK) — публичному боту вложения запрещены, мимо
        if payload.get("txt_type") not in (None, 0, 2):
            logger.debug("[%s] Skipping binary/application message (txt_type=%s)",
                         self.name, payload.get("txt_type"))
            return
        text = str(payload.get("text") or "").strip()
        if not text:
            return
        mc = self._mc
        if mc is None:
            return
        prefix = str(payload.get("pubkey_prefix") or "").strip()
        contact = None
        with suppress(Exception):
            contact = mc.get_contact_by_key_prefix(prefix)
        name = (contact or {}).get("adv_name") or prefix[:8] or "unknown"
        chat_id = prefix or name
        msg_id = hashlib.sha256(
            f"{chat_id}|{text}|{payload.get('sender_timestamp', '')}".encode()
        ).hexdigest()[:16]
        ts = payload.get("sender_timestamp")
        timestamp = datetime.now(tz=timezone.utc)
        with suppress((ValueError, OSError, TypeError, OverflowError)):
            if isinstance(ts, (int, float)) and ts > 0:
                timestamp = datetime.fromtimestamp(ts, tz=timezone.utc)
        source = self.build_source(
            chat_id=chat_id, chat_name=name, chat_type="dm",
            user_id=chat_id, user_name=name, message_id=msg_id)
        message_event = MessageEvent(
            text=text, message_type=MessageType.TEXT, source=source,
            message_id=msg_id, raw_message=payload, timestamp=timestamp)
        logger.debug("[%s] Message from %s: %s", self.name, name, text[:60])
        await self.handle_message(message_event)

    # -- Outbound messaging -------------------------------------------------

    def _resolve_destination(self, chat_id: str) -> Optional[Any]:
        """Контакт по pubkey-префиксу, затем по имени, затем сырой hex-ключ."""
        mc = self._mc
        if mc is None or not chat_id:
            return None
        with suppress(Exception):
            contact = mc.get_contact_by_key_prefix(chat_id)
            if contact:
                return contact
        with suppress(Exception):
            contact = mc.get_contact_by_name(chat_id)
            if contact:
                return contact
        # неизвестный отправитель: отвечаем прямо на pubkey-префикс
        try:
            bytes.fromhex(chat_id)
            return chat_id
        except ValueError:
            return None

    async def send(
        self, chat_id: str, content: str, reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Отправка личного сообщения контакту, лимит ``reply_limit`` символов.

        is_connected ненадёжен (см. _session) — шлём и решаем по факту:
        при ошибке будим петлю реконнекта и повторяем один раз.
        """
        mc = self._mc
        if mc is None:
            self._wake.set()
            logger.info("[%s] No session — waiting for connect to deliver reply", self.name)
            deadline = time.monotonic() + RECONNECT_WAIT_SECONDS
            while time.monotonic() < deadline:
                await asyncio.sleep(0.5)
                mc = self._mc
                if mc is not None:
                    break
            if mc is None:
                return SendResult(success=False, error="meshcore: no companion session")
        text = (content or "").strip()
        if not text:
            return SendResult(success=False, error="meshcore: empty message")
        if _is_service_message(text):
            logger.info("[%s] Dropping service notification (%d chars)", self.name, len(text))
            return SendResult(success=True, message_id=uuid.uuid4().hex[:12])
        if len(text) > self._reply_limit:
            logger.warning(
                "[%s] Reply truncated from %d to %d chars (LoRa limit)",
                self.name, len(text), self._reply_limit)
            text = text[:self._reply_limit].rstrip()
        destination = self._resolve_destination(chat_id)
        if destination is None:
            return SendResult(success=False, error=f"meshcore: contact '{chat_id}' not found")
        result = None
        for attempt in (1, 2):
            try:
                result = await mc.commands.send_msg(destination, text)
                break
            except Exception as e:
                if attempt == 2 or not self._running:
                    return SendResult(success=False, error=f"meshcore send failed: {e}")
                logger.warning("[%s] Send attempt 1 failed (%s) — waking reconnect", self.name, e)
                self._wake.set()
                deadline = time.monotonic() + RECONNECT_WAIT_SECONDS
                while time.monotonic() < deadline and (self._mc is None or self._mc is mc):
                    await asyncio.sleep(0.5)
                mc = self._mc
                if mc is None:
                    return SendResult(success=False, error="meshcore: companion node unreachable")
        if result is not None and getattr(result, "type", None) == EventType.ERROR:
            return SendResult(success=False, error=f"meshcore node error: {getattr(result, 'payload', None)}")
        return SendResult(success=True, message_id=uuid.uuid4().hex[:12])

    async def send_typing(self, chat_id: str, **_kwargs) -> None:
        """LoRa не имеет индикатора набора — no-op."""

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {"name": chat_id, "type": "dm"}


# -- Plugin registration -----------------------------------------------------


def _env_enablement() -> dict | None:
    """``env_enablement_fn``: сидирует ``PlatformConfig.extra`` из env."""
    host = _get_scoped_secret("MESHCORE_HOST", "").strip()
    if not host:
        return None
    seed = _seed_extra_from_env((
        ("MESHCORE_PORT", "port", None),
        ("MESHCORE_REPLY_LIMIT", "reply_limit", None),
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
    thread_id: Optional[str] = None, media_files: Optional[List[str]] = None,
    force_document: bool = False,
) -> Dict[str, Any]:
    """Доставка вне процесса (cron / send_message_tool): короткое TCP-соединение.

    ``thread_id``/``media_files`` — только совместимость сигнатуры (у MeshCore
    нет тредов и вложений).
    """
    if not MESHCORE_AVAILABLE:
        return send_error("meshcore standalone send: meshcore library not installed")
    extra = getattr(pconfig, "extra", {}) or {}
    host = _host_value(extra)
    if not host:
        return send_error("meshcore standalone send: MESHCORE_HOST not configured")
    port, reply_limit = DEFAULT_PORT, DEFAULT_REPLY_LIMIT
    with suppress((TypeError, ValueError)):
        port = int(_extra_or_secret(extra, "port", "MESHCORE_PORT", DEFAULT_PORT))
    with suppress((TypeError, ValueError)):
        reply_limit = int(_extra_or_secret(extra, "reply_limit", "MESHCORE_REPLY_LIMIT", DEFAULT_REPLY_LIMIT))
    home = extra.get("home_channel") if isinstance(extra.get("home_channel"), dict) else {}
    target = (chat_id or _get_scoped_secret("MESHCORE_HOME_CHANNEL", "").strip()
              or (home or {}).get("id") or "").strip()
    if not target:
        return send_error("meshcore standalone send: no target contact (set MESHCORE_HOME_CHANNEL)")
    text = (message or "").strip()[:reply_limit]
    if not text:
        return send_error("meshcore standalone send: empty message")
    mc = None
    try:
        mc = await _connect_with_contacts(host, port)
        destination = None
        with suppress(Exception):
            destination = mc.get_contact_by_name(target)
        if destination is None:
            with suppress(Exception):
                destination = mc.get_contact_by_key_prefix(target)
        if destination is None:
            try:
                bytes.fromhex(target)
                destination = target
            except ValueError:
                destination = None
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


def _parse_target_ref(target_ref: str):
    """``parse_target_ref_fn``: цель для ``hermes send -t meshcore:<ref>``.

    Принимает как есть имя контакта или hex pubkey-префикс — реальный
    резолвинг делает адаптер в ``send()`` (по префиксу, по имени, по ключу).
    """
    ref = (target_ref or "").strip()
    if not ref:
        return None
    return ref, None


def register(ctx) -> None:
    """Точка входа плагина — вызывается плагиновой системой Hermes при старте."""
    ctx.register_platform(
        name="meshcore", label="meshcore",
        adapter_factory=lambda cfg: MeshcoreAdapter(cfg),
        check_fn=check_requirements, validate_config=validate_config, is_connected=is_connected,
        required_env=["MESHCORE_HOST"],
        install_hint="pip install meshcore",
        parse_target_ref_fn=_parse_target_ref,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="MESHCORE_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="MESHCORE_ALLOWED_USERS", allow_all_env="MESHCORE_ALLOW_ALL_USERS",
        max_message_length=DEFAULT_REPLY_LIMIT, emoji="📡",
        pii_safe=False,  # в контактах возможны координаты
        allow_update_command=True,
        platform_hint=(
            "You are communicating over MeshCore, a long-range low-bandwidth LoRa mesh "
            "radio network. Keep every reply under 130 characters of plain text — this is "
            "a hard adapter-side limit, longer replies get truncated. No attachments, no "
            "typing indicators; delivery can take tens of seconds. Reply in the user's "
            "language; use weather emoji for weather answers."
        ))
