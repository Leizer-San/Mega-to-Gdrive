# MEGA & Pixeldrain → Google Drive Importer

Инструмент для импорта публичных ссылок MEGA и Pixeldrain прямо в Google Drive через Google Colab — без использования вашего локального трафика.

## Возможности

- Публичные ссылки MEGA (`mega.nz/file/...` и `mega.nz/folder/...`)
- Публичные файлы и коллекции Pixeldrain (`pixeldrain.com/u/...` и `pixeldrain.com/l/...`)
- Очередь из нескольких ссылок
- Сохранение структуры папок и выбор отдельных файлов
- Отображение квоты Google Drive
- Предварительная проверка свободного места
- Загрузка в Drive через resumable upload
- Автоматический retry ошибок (до 3 раз)
- Пропуск уже существующего файла (по имени + размеру)
- Прогресс, скорость и логи в реальном времени
- Кнопки запуска/остановки очереди
- Автоматическая ротация пула прокси при исчерпании лимитов трафика
- Три варианта веб-туннелей (Colab, Cloudflare, Localtunnel)
- Сохранение очереди между сессиями (на Google Drive)
- Навигатор по папкам Google Drive прямо в интерфейсе
- ZIP-упаковка: вся папка или каждая подпапка отдельно

## Структура проекта

```
mega-to-gdrive/
├── requirements.txt              # pip-зависимости
├── mega_importer/
│   ├── config.py                 # Константы и пути
│   ├── state.py                  # Глобальное состояние и очередь
│   ├── helpers.py                # Утилиты (логи, форматирование)
│   ├── drive.py                  # Google Drive API
│   ├── mega.py                   # MEGAcmd + ZIP-утилиты
│   ├── mega_api.py               # Прямой MEGA HTTP API клиент
│   ├── native_downloader.py      # Нативный параллельный загрузчик MEGA
│   ├── pixeldrain.py             # Интеграция и загрузчик Pixeldrain
│   ├── worker.py                 # Обработчик задач
│   ├── server.py                 # Flask-сервер и API-маршруты
│   └── tunnels.py                # Cloudflare / Localtunnel / Colab
└── ui/
    └── index.html                # Веб-интерфейс
```

## Запуск в Google Colab

1. Откройте файл [`colab/mega_to_google_drive_colab.ipynb`](colab/mega_to_google_drive_colab.ipynb) в Google Colab (или используйте ноутбук из репозитория).

2. **Ячейка 1** — установка MEGAcmd и Python-зависимостей. Нажмите Play.

3. **Ячейка 2** — укажите ваш репозиторий:
   ```python
   GITHUB_REPO = "YOUR_USERNAME/mega-to-gdrive"
   ```
   Нажмите Play. Код автоматически скачает свежую версию из GitHub и запустит сервер.

4. Перейдите по одной из предложенных ссылок (Colab-прокси, Cloudflare или Localtunnel).

5. Авторизуйте Google Drive (два разрешения: диск + API).

6. Выберите папку назначения, вставьте MEGA-ссылки и запустите импорт.

## Обновление кода

Просто сделайте `git push` в `main` — при следующем запуске ноутбука свежая версия будет подтянута автоматически.

## Зависимости

- [MEGAcmd](https://github.com/meganz/MEGAcmd) (устанавливается автоматически)
- `flask`, `werkzeug`
- `google-api-python-client`, `google-auth-httplib2`, `google-auth-oauthlib`
- `psutil`
- Node.js `localtunnel` (устанавливается автоматически)
- [cloudflared](https://github.com/cloudflare/cloudflared) (скачивается автоматически)
