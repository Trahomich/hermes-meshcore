# hermes-meshcore

Платформенный адаптер [Hermes Agent](https://github.com/NousResearch/hermes-agent) для сети **MeshCore** (LoRa-mesh).

Подключается к companion-ноде MeshCore по TCP (библиотека [`meshcore`](https://pypi.org/project/meshcore/)), принимает личные сообщения из эфира, передаёт их агенту и отправляет ответы обратно — с жёстким усечением до 130 символов (настраивается).

## Возможности

- Приём личных сообщений (`CONTACT_MSG_RECV`) → агент → ответ контакту
- Жёсткий лимит длины ответа (по умолчанию **130 символов**) — усечение на уровне адаптера
- Автоматический реконнект к ноде с backoff (2→5→10→30→60 c)
- Ответы неизвестным отправителям по сырому pubkey-префиксу (без записи в контактах)
- Доставка cron-уведомлений (`MESHCORE_HOME_CHANNEL`) через standalone-отправку
- Allowlist контактов (`MESHCORE_ALLOWED_USERS`) или публичный режим (`MESHCORE_ALLOW_ALL_USERS=true`)

## Установка

```bash
pip install meshcore          # библиотека работы с companion-нодой

mkdir -p ~/.hermes/plugins/meshcore
cd ~/.hermes/plugins/meshcore
# положить сюда plugin.yaml, adapter.py, __init__.py из этого репозитория
```

Переменные окружения (или `platforms.meshcore.extra` в `config.yaml`):

| Переменная | По умолчанию | Описание |
|---|---|---|
| `MESHCORE_HOST` | — (обяз.) | TCP-хост companion-ноды, напр. `192.168.99.23` |
| `MESHCORE_PORT` | `5000` | TCP-порт companion-ноды |
| `MESHCORE_REPLY_LIMIT` | `130` | Жёсткий лимит ответа в символах |
| `MESHCORE_ALLOWED_USERS` | — | Имена контактов через запятую |
| `MESHCORE_ALLOW_ALL_USERS` | — | `true` — отвечать всем (публичный бот) |
| `MESHCORE_HOME_CHANNEL` | — | Имя контакта для доставки cron |

Перезапустить шлюз: `hermes gateway restart`. Платформа появится в `hermes gateway status`.

## Ограничения

- Только личные сообщения (каналы `CHANNEL_MSG_RECV` не слушаются)
- LoRa медленный: подтверждение отправки (`MSG_SENT`) может идти десятки секунд; таймаут отправки 60 с
- Эмодзи и кириллица — многобайтовые: 130 символов могут быть заметно длиннее 230 байт; MeshCore фрагментирует большие тексты, но краткость — добродетель эфира
- Индикатор набора текста не поддерживается (no-op)
- Замеченная особенность companion TCP: первый запрос после переподключения иногда молча теряется (список контактов приходит пустым). Адаптер переподключается до 3 раз, пока контакты не загрузятся

## Отправка сообщений / cron

```bash
# из контейнера: целью может быть имя контакта или hex pubkey-префикс
hermes send -t "meshcore:UMR-3-test-2b" "текст"
hermes send -t "meshcore:b5dfff96af6611fa" "текст"
```

Cron-доставка: `MESHCORE_HOME_CHANNEL=<имя контакта>`.

## Troubleshooting

**После `hermes plugins enable meshcore-platform` платформы падают с "aiohttp not installed".**
`plugins enable` создаёт управляемое окружение `~/.hermes/installs/<id>/environments/<id>/venv`
и шлюз запускается с PYTHONPATH в него; в нём нет aiohttp и meshcore. Лечится установкой
недостающих пакетов в это окружение:

```bash
SITE=~/.hermes/installs/<id>/environments/<id>/venv/lib/python3.14/site-packages
/opt/hermes/.venv/bin/python -m pip install --target "$SITE" aiohttp meshcore
```

Если окружение пересоберётся (обновление плагина), повторить.

## Разработка

Два файла: `plugin.yaml` (манифест) и `adapter.py` (~450 строк, шаблон — ntfy-адаптер из поставки Hermes). Ядро Hermes не меняется: адаптер регистрируется через `ctx.register_platform()` при загрузке плагина из `~/.hermes/plugins/`.
