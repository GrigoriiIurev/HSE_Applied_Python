

# URL Shortener API

## Описание

URL Shortener API — это сервис для сокращения длинных ссылок. Пользователь может создать короткую ссылку, которая будет перенаправлять на оригинальный URL.

Сервис поддерживает:

- создание коротких ссылок
- редирект по короткой ссылке
- обновление и удаление ссылок
- просмотр статистики переходов
- поиск по оригинальному URL
- кастомные alias для ссылок
- срок жизни ссылки (TTL)
- регистрацию и авторизацию пользователей
- управление ссылками владельцем
- кэширование через Redis
- автоматическую очистку истёкших и неиспользуемых ссылок

---

# Запуск проекта

## Требования

- Docker
- Docker Compose

## Запуск

В корне проекта выполнить:

```
docker compose up --build
```

После запуска сервис будет доступен по адресу:

http://localhost:8000

Swagger‑документация API:

http://localhost:8000/docs#/

---

# API

## Регистрация пользователя

POST /auth/register

Пример запроса:

```
{
  "email": "user1@test.com",
  "password": "ABCefg123"
}
```

Пример ответа:

```
{
  "id": "d0abb415-091d-4c10-9d98-eb92aa4dd684",
  "email": "user1@test.com",
  "is_active": true,
  "is_superuser": false,
  "is_verified": false
}
```

---

## Получение JWT токена

POST /auth/jwt/login

Запрос (form-data):
```
username=user1@test.com
password=ABCefg123
```
Ответ:
```
{
  "access_token": "jwt_token",
  "token_type": "bearer"
}
```
---

В верхнем углу необходимо нажать authorize и ввести username и password, после чего нажать authorize.

# Работа со ссылками

## Создание короткой ссылки

POST /links/shorten

Пример запроса:
```
{
  "original_url": "https://example.su"
}
```
Ответ:
```
{
  "status": "success",
  "short_code": "uW5XT1"
}
```
---

## Создание ссылки с кастомным alias

POST /links/shorten
```
{
  "original_url": "https://example.org",
  "custom_alias": "myalias"
}
```

Ответ:
```
{
  "status": "success",
  "short_code": "myalias"
}
```
---

## Редирект по короткой ссылке

GET /links/{short_code}

Пример:

GET /links/myalias

Этот эндпоинт выполняет HTTP‑перенаправление на оригинальный URL.

---

## Получение статистики

GET /links/{short_code}/stats

Пример ответа:
```
{
  "original_url": "https://example.org",
  "created_at": "2026-03-08T07:34:06.860352",
  "click_count": 1,
  "last_used_at": "2026-03-08T07:35:30.972506"
}
```
---

## Обновление ссылки

PUT /links/{short_code}

Требуется авторизация.
```
{
  "original_url": "exemple.ru"
}
```

Ответ:
```
{
  "status": "updated"
}
```
---

## Удаление ссылки

DELETE /links/{short_code}

Требуется авторизация.

Ответ:
```
{
  "status": "deleted"
}
```
---

## Поиск по оригинальному URL

GET /links/search?original_url=https://exemple.ru

Ответ:
```
{
  "status": "success",
  "data": []
}
```
---

# Срок жизни ссылки

При создании ссылки можно указать дату истечения:
```
{
  "original_url": "https://exemple.ru",
  "expires_at": "2026-12-31T23:59"
}
```
После истечения срока ссылка становится недоступной и автоматически удаляется фоновым процессом очистки.

---

# Кэширование

Redis используется для кэширования:

- редиректов
- популярных ссылок

Кэш автоматически сбрасывается при обновлении или удалении ссылки.

---

# База данных

В качестве основной базы данных используется PostgreSQL.

Основная таблица: **links**

| поле | тип | описание |
|-----|-----|----------|
| id | UUID | идентификатор ссылки |
| original_url | TEXT | оригинальный URL |
| short_code | TEXT | короткий код |
| custom_alias | TEXT | пользовательский alias |
| owner_id | UUID | владелец ссылки |
| created_at | TIMESTAMP | дата создания |
| expires_at | TIMESTAMP | срок жизни |
| click_count | INTEGER | количество переходов |
| last_used_at | TIMESTAMP | последний переход |

Пользователи хранятся через библиотеку FastAPI Users.

---

# Пример использования

Создание короткой ссылки:

POST /links/shorten

Ответ:
```
{
  "short_code": "myalias"
}
```
Переход по ссылке в браузере:

http://localhost:8000/links/myalias

---
# Тестирование

В проекте реализованы автоматические тесты для проверки работы API.

Тесты находятся в папке **tests** и включают два файла:

- `conftest.py` — содержит настройку тестовой среды:
  - создание тестовой базы данных SQLite
  - подмену Redis на тестовый mock (FakeRedis)
  - фикстуры для FastAPI приложения и HTTP клиента
  - автоматическую очистку базы перед каждым тестом

- `test_links_api.py` — содержит тесты основных сценариев работы сервиса.

Тесты покрывают следующие случаи:

- создание короткой ссылки
- редирект по короткой ссылке
- получение статистики переходов
- создание ссылки с кастомным alias
- проверку уникальности alias
- обновление ссылки владельцем
- запрет изменения чужой ссылки
- удаление ссылки
- автоматическое удаление просроченных ссылок
- очистку неиспользуемых ссылок

## Запуск тестов

Был реализован подход тестирования кода в папке `test`, но он в данный момент не работает, так как пришлось переделать код для деплоя на Render. Для запуска теста необходимо:

Поменять комметирование в файле database.py
```
DATABASE_URL = (
    f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# DATABASE_URL = os.getenv("DATABASE_URL")

# engine = create_async_engine(
#     DATABASE_URL,
#     echo=False,
# )
```
В файле cache.py поменять код на:
```
from redis import asyncio as aioredis
from config import REDIS_HOST, REDIS_PORT


redis = aioredis.from_url(
    f"redis://{REDIS_HOST}:{REDIS_PORT}",
    decode_responses=True
)
```
В корень добавить файл .env с кодом:
```
DB_USER=postgres
DB_PASS=postgres
DB_HOST=postgres
DB_PORT=5432
DB_NAME=links_db

REDIS_HOST=redis
REDIS_PORT=6379

SECRET=super_secret_key

BASE_URL=http://localhost:8000

CLEANUP_AFTER_DAYS=30
```

---

# Деплой проекта

Проект задеплоен на платформе **Render** с использованием репозитория GitHub.

При каждом обновлении кода в ветке `main` происходит автоматический деплой новой версии сервиса.

Сервис доступен по адресу:
```
https://hse-applied-python-lwqr.onrender.com
```

Swagger-документация API:
```
https://hse-applied-python-lwqr.onrender.com/docs
```
Пример проверки редиректа по короткой ссылке:
```
https://hse-applied-python-lwqr.onrender.com/links/{short_code}
```
Например:
```
https://hse-applied-python-lwqr.onrender.com/links/n37pYv
```
Деплой выполняется с использованием следующих сервисов Render:

- **Web Service** — FastAPI приложение
- **PostgreSQL** — основная база данных
- **Redis (Key Value)** — кэширование
# Используемые технологии

- FastAPI
- PostgreSQL
- Redis
- SQLAlchemy
- FastAPI Users
- Docker
- Uvicorn