# DataQuery Pro Backend - FastAPI Oracle Database API

FastAPI бэкенд для корпоративного интерфейса работы с Oracle базой данных. Предоставляет REST API для безопасного выполнения запросов, управления данными и настройками.

## 🚀 Возможности

### 🔐 Безопасность
- **Контролируемый доступ**: Ограниченный список доступных таблиц
- **SQL Injection защита**: Полная санитизация всех запросов
- **Валидация данных**: Проверка всех входящих параметров
- **CORS поддержка**: Настраиваемые источники запросов

### 📊 Функциональность
- **Визуальный конструктор запросов**: Конвертация фильтров в безопасные SQL запросы
- **Управление данными**: Пагинация, сортировка, поиск, экспорт
- **История запросов**: Отслеживание выполненных операций
- **Статистика**: Мониторинг производительности и использования
- **Настройки**: Управление конфигурацией через API

## 📋 Предварительные требования

- Python 3.8 или выше
- Oracle Database 11g или выше
- Oracle Instant Client
- Доступ к Oracle базе данных DSSB_APP

## 🛠 Установка

### 1. Клонирование и настройка окружения

```bash
# Переход в директорию бэкенда
cd database-backend

# Создание виртуального окружения
python -m venv venv

# Активация виртуального окружения
# Windows:
venv\Scripts\activate
# Linux/MacOS:
source venv/bin/activate

# Установка зависимостей
pip install -r requirements.txt
```

### 2. Настройка Oracle Instant Client

#### Windows:
1. Скачайте Oracle Instant Client с официального сайта Oracle
2. Распакуйте в папку (например, `C:\oracle\instantclient_21_1`)
3. Добавьте путь в системную переменную PATH
4. Установите переменную `ORACLE_HOME`

#### Linux:
```bash
# Ubuntu/Debian
sudo apt-get install oracle-instantclient-basic
sudo apt-get install oracle-instantclient-devel

# CentOS/RHEL
sudo yum install oracle-instantclient-basic
sudo yum install oracle-instantclient-devel
```

### 3. Настройка переменных окружения

Создайте файл `.env` в корневой директории проекта:

```bash
# Скопируйте пример
cp env_example.txt .env
```

Отредактируйте `.env` файл:

```env
# Oracle Database Configuration
ORACLE_HOST=your-oracle-server.company.com
ORACLE_PORT=1521
ORACLE_SID=PROD
ORACLE_USER=your_username
ORACLE_PASSWORD=your_password

# Application Configuration
APP_HOST=0.0.0.0
APP_PORT=8000
DEBUG=True

# Security
SECRET_KEY=your-very-secure-secret-key-change-in-production
ACCESS_TOKEN_EXPIRE_MINUTES=30

# CORS Origins (comma-separated)
ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

## 🚀 Запуск

### Режим разработки:
```bash
python main.py
```

### Производственный режим:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

### С Docker:
```bash
# Сборка образа
docker build -t dataquery-backend .

# Запуск контейнера
docker run -p 8000:8000 --env-file .env dataquery-backend
```

## 📖 API Документация

После запуска сервера документация доступна по адресам:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc
- **OpenAPI JSON**: http://localhost:8000/openapi.json

## 🗄 Структура базы данных

### Доступные таблицы:

#### 1. EMPLOYEES (Сотрудники)
```sql
EMPLOYEE_ID     NUMBER          -- ID сотрудника
FIRST_NAME      VARCHAR2(50)    -- Имя
LAST_NAME       VARCHAR2(50)    -- Фамилия
EMAIL           VARCHAR2(100)   -- Email адрес
PHONE           VARCHAR2(20)    -- Телефон
DEPARTMENT      VARCHAR2(50)    -- Отдел
POSITION        VARCHAR2(50)    -- Должность
SALARY          NUMBER(10,2)    -- Зарплата
HIRE_DATE       DATE            -- Дата найма
STATUS          VARCHAR2(20)    -- Статус
```

#### 2. CUSTOMERS (Клиенты)
```sql
CUSTOMER_ID         NUMBER          -- ID клиента
FIRST_NAME          VARCHAR2(50)    -- Имя
LAST_NAME           VARCHAR2(50)    -- Фамилия
EMAIL               VARCHAR2(100)   -- Email адрес
PHONE               VARCHAR2(20)    -- Телефон
ADDRESS             VARCHAR2(200)   -- Адрес
CITY                VARCHAR2(50)    -- Город
REGISTRATION_DATE   DATE            -- Дата регистрации
STATUS              VARCHAR2(20)    -- Статус аккаунта
```

#### 3. ORDERS (Заказы)
```sql
ORDER_ID            NUMBER          -- ID заказа
CUSTOMER_ID         NUMBER          -- ID клиента
ORDER_DATE          DATE            -- Дата заказа
TOTAL_AMOUNT        NUMBER(10,2)    -- Общая сумма
STATUS              VARCHAR2(20)    -- Статус заказа
SHIPPING_ADDRESS    VARCHAR2(200)   -- Адрес доставки
PAYMENT_METHOD      VARCHAR2(50)    -- Способ оплаты
```

#### 4. PRODUCTS (Продукты)
```sql
PRODUCT_ID          NUMBER          -- ID продукта
PRODUCT_NAME        VARCHAR2(100)   -- Название продукта
DESCRIPTION         VARCHAR2(500)   -- Описание
PRICE               NUMBER(10,2)    -- Цена
CATEGORY            VARCHAR2(50)    -- Категория
STOCK_QUANTITY      NUMBER          -- Количество на складе
CREATED_DATE        DATE            -- Дата создания
```

#### 5. SALES (Продажи)
```sql
SALE_ID             NUMBER          -- ID продажи
ORDER_ID            NUMBER          -- ID заказа
PRODUCT_ID          NUMBER          -- ID продукта
QUANTITY            NUMBER          -- Количество
UNIT_PRICE          NUMBER(10,2)    -- Цена за единицу
TOTAL_PRICE         NUMBER(10,2)    -- Общая цена
SALE_DATE           DATE            -- Дата продажи
```

## 🔌 Основные API Эндпоинты

### Базы данных
```http
GET /databases                              # Список баз данных
GET /databases/{id}/tables                  # Список таблиц
GET /databases/{id}/tables/{table}/columns  # Список столбцов
POST /databases/test-connection             # Тест соединения
```

### Запросы
```http
POST /query/execute     # Выполнение запроса
GET /query/history      # История запросов
POST /query/save        # Сохранение запроса
GET /query/saved        # Сохраненные запросы
```

### Данные
```http
GET /data              # Получение данных с фильтрами
GET /data/export       # Экспорт данных
GET /data/stats/{table} # Статистика таблицы
```

### Настройки
```http
GET /settings          # Получение настроек
PUT /settings          # Обновление настроек
GET /stats             # Статистика панели управления
```

## 💻 Примеры использования

### Выполнение запроса:
```python
import requests

# Запрос к API
response = requests.post('http://localhost:8000/query/execute', json={
    "database_id": "dssb_app",
    "table": "employees",
    "columns": ["first_name", "last_name", "department"],
    "filters": [
        {
            "column": "department",
            "operator": "equals",
            "value": "IT"
        }
    ],
    "sort_by": "last_name",
    "sort_order": "ASC",
    "limit": 50
})

data = response.json()
print(data)
```

### Получение данных с пагинацией:
```python
response = requests.get('http://localhost:8000/data', params={
    "database_id": "dssb_app",
    "table": "customers",
    "page": 1,
    "limit": 25,
    "search": "ivan",
    "sort_by": "registration_date",
    "sort_order": "desc"
})

data = response.json()
```

## 🔧 Конфигурация

### Настройка доступных таблиц

Отредактируйте файл `database.py`, раздел `ALLOWED_TABLES`:

```python
ALLOWED_TABLES = {
    'DSSB_APP': {
        'your_table': {
            'description': 'Описание таблицы',
            'columns': [
                {'name': 'column_name', 'type': 'VARCHAR2', 'description': 'Описание столбца'},
                # ... другие столбцы
            ]
        }
    }
}
```

### Настройка операторов фильтров

В файле `query_builder.py` можно добавить новые операторы:

```python
self.allowed_operators = {
    'equals': '=',
    'not_equals': '!=',
    'contains': 'LIKE',
    'starts_with': 'LIKE',  # Новый оператор
    # ... другие операторы
}
```

## 📊 Мониторинг и логирование

### Логи приложения:
```bash
# Просмотр логов в реальном времени
tail -f logs/app.log
```

### Мониторинг производительности:
- GET `/health` - проверка состояния
- GET `/stats` - статистика использования
- Встроенные метрики FastAPI

## 🚀 Развертывание в продакшене

### 1. Настройка переменных окружения:
```env
DEBUG=False
SECRET_KEY=super-secure-production-key
ORACLE_HOST=prod-oracle-server.company.com
ORACLE_USER=prod_user
ORACLE_PASSWORD=secure_password
```

### 2. Использование Gunicorn:
```bash
pip install gunicorn
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
```

### 3. Настройка Nginx (пример):
```nginx
server {
    listen 80;
    server_name your-api-domain.com;

    location / {
        proxy_pass http://127.0.0.1:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

## 🔒 Безопасность

### Рекомендации:
1. **Используйте HTTPS** в продакшене
2. **Ограничьте CORS origins** только доверенными доменами
3. **Настройте брандмауэр** для ограничения доступа к порту 8000
4. **Регулярно обновляйте** зависимости
5. **Используйте сильные пароли** для базы данных
6. **Настройте мониторинг** и алерты

### Дополнительная аутентификация:
Можно добавить JWT токены или API ключи:

```python
from fastapi.security import HTTPBearer
security = HTTPBearer()

@app.get("/protected")
async def protected_route(token: str = Depends(security)):
    # Валидация токена
    pass
```

## 🐛 Отладка

### Общие проблемы:

#### 1. Ошибка подключения к Oracle:
```bash
# Проверьте переменные окружения
echo $ORACLE_HOST

# Проверьте сетевое соединение
telnet your-oracle-host 1521

# Проверьте Oracle Instant Client
python -c "import cx_Oracle; print(cx_Oracle.version)"
```

#### 2. Ошибки импорта:
```bash
# Переустановите зависимости
pip uninstall cx_Oracle
pip install cx_Oracle

# Проверьте виртуальное окружение
which python
pip list
```

#### 3. CORS ошибки:
Убедитесь, что фронтенд URL указан в `ALLOWED_ORIGINS`

## 📞 Поддержка

По вопросам технической поддержки:
- Проверьте логи: `logs/app.log`
- Документация API: http://localhost:8000/docs
- Обратитесь к администратору базы данных

---

**DataQuery Pro Backend** - Надежный и безопасный API для корпоративного доступа к Oracle базе данных. 🚀 