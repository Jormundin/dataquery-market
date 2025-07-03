# 🏢 DataQuery Pro - Корпоративный интерфейс для Oracle Database

Современный веб-интерфейс для работы с корпоративными базами данных Oracle без необходимости знания SQL.

## 📋 Обзор проекта

DataQuery Pro позволяет пользователям без технических навыков легко взаимодействовать с корпоративными базами данных через интуитивный визуальный интерфейс.

### ✨ Основные возможности

- 🎯 **Визуальный конструктор запросов** - создание запросов без написания SQL
- 📊 **Панель управления** - статистика и мониторинг активности
- 👀 **Просмотр данных** - пагинация, поиск, сортировка, экспорт
- ⚙️ **Настройки системы** - конфигурация подключений и пользовательских предпочтений
- 🔢 **Подсчет строк** - мгновенная оценка размера выборки
- 🌐 **Русский интерфейс** - полная локализация
- 🔗 **Мультиплексные подключения** - поддержка 4 различных Oracle баз данных
- 🚀 **Миграция Market.py** - современная архитектура вместо Streamlit монолита

## 🏗️ Архитектура

```
DataQuery Pro/
├── 🖥️ database-interface/     # React Frontend
│   ├── src/
│   │   ├── components/        # Переиспользуемые компоненты
│   │   ├── pages/            # Страницы приложения
│   │   └── services/         # API клиенты
│   └── package.json
├── 🚀 database-backend/       # FastAPI Backend
│   ├── main.py              # Основное приложение
│   ├── models.py            # Pydantic модели
│   ├── database.py          # Логика работы с БД
│   ├── query_builder.py     # Безопасное построение SQL
│   └── requirements.txt
└── README.md
```

## 🎯 Market.py Migration Progress

Мы осуществляем миграцию функциональности из оригинального market.py (Streamlit монолит) в современную FastAPI + React архитектуру.

### ✅ Завершено

**Phase 1: Database Infrastructure**
- ✅ **Множественные подключения Oracle** - 4 различных баз данных
  - `DSSB_APP` - Основные данные и аналитика
  - `SPSS` - SPSS аналитика + пользователи кампаний  
  - `DSSB_OCDS` - Управление кампаниями
  - `ED_OCDS` - Отслеживание кампаний
- ✅ **API эндпоинты** - Тестирование всех подключений
- ✅ **Конфигурация** - Переменные окружения для всех БД
- ✅ **Тестирование** - Автоматический скрипт проверки подключений

**Phase 2: Parquet Data Service** ✅ **Завершено**
- ✅ **Система управления parquet файлами** - загрузка, кеширование, API доступ
- ✅ **15+ наборов данных** - стоп-листы, устройства, пуш-предпочтения, продукты, аналитика
- ✅ **Фильтрация IIN** - интеграция всех методов фильтрации из market.py
- ✅ **Поддержка среды** - автоматическое создание mock данных для тестирования
- ✅ **API эндпоинты** - полный REST API для работы с данными
- ✅ **Кеширование** - TTL кеш с управлением производительности
- ✅ **Документация** - полная документация и тесты

**Phase 3: Campaign Management Core** ✅ **Завершено**
- ✅ **RB1/RB3 Campaign Models** - полные модели данных для обоих типов кампаний
- ✅ **CAMPAIGNCODE Generation** - автоматическая генерация C000012345 и KKB_XXXX кодов
- ✅ **Multi-table Deployment** - развертывание в 4+ Oracle таблицах одновременно
- ✅ **Metadata Management** - управление потоками, каналами, описаниями кампаний
- ✅ **Parquet Integration** - полная интеграция с системой фильтрации данных
- ✅ **API Endpoints** - RESTful endpoints для создания, управления и удаления кампаний
- ✅ **Error Handling** - comprehensive error management и graceful failures

**Phase 4: Frontend Integration** ✅ **Завершено**
- ✅ **Campaign Manager UI** - полноценный React интерфейс для управления кампаниями
- ✅ **API Integration** - полная интеграция с campaign и parquet service APIs
- ✅ **Interactive Forms** - интуитивные формы создания RB1/RB3 кампаний
- ✅ **Campaign Listing** - просмотр и управление существующими кампаниями  
- ✅ **Real-time Feedback** - сообщения об успехе/ошибках с loading состояниями
- ✅ **Navigation Integration** - бесшовная интеграция в существующую навигацию
- ✅ **User Experience** - современный, отзывчивый интерфейс с валидацией
- ✅ **Complete Filtering UI** - все фильтры из market.py (blacklists, устройства, возраст, пол, MAU, push-потоки)
- ✅ **Advanced Filtering** - филиалы, локальные потоки, RB3 потоки, предыдущие кампании, дедупликация
- ✅ **Filter Testing** - интерфейс тестирования фильтров перед созданием кампании
- ✅ **Visual Filter Summary** - наглядное отображение активных фильтров
- ✅ **Manual Operations** - добавление IIN, статистика кампаний, удаление с паролем
- ✅ **SQL Interface** - выполнение произвольных SQL запросов для администраторов
- ✅ **Data Export** - экспорт в Excel/CSV/Parquet форматы
- ✅ **External Tools** - интеграция с AI helper, стратификацией, Jira

### 🔄 В разработке

**Phase 5: Advanced Features**
- 🔄 Продвинутая фильтрация данных в UI
- 🔄 Code generation интерфейс (RB1/RB3 коды)
- 🔄 Массовая загрузка данных с пропуском дубликатов
- 🔄 Email уведомления о статусе кампаний
- 🔄 Excel/CSV импорт-экспорт IIN списков
- 🔄 Dashboard статистики кампаний

### 📊 Сравнение архитектур

| Aspect | Market.py (Old) | DataQuery Pro (New) |
|--------|-----------------|---------------------|
| **UI Framework** | Streamlit | React + FastAPI |
| **Database Access** | Прямые подключения | Контролируемый API |
| **Authentication** | Нет | LDAP интеграция |
| **Security** | Ограниченная | Полная валидация |
| **Scalability** | Одиночный поток | Многопользовательская |
| **Maintainability** | Монолит | Модульная архитектура |
| **Testing** | Ручное | Автоматизированное |
| **Campaign Management** | Включено | ✅ Полностью мигрировано |
| **Data Filtering** | In-memory processing | ✅ API-based + UI interface |
| **Filial Filtering** | Manual selection | ✅ 35+ филиалов с UI |
| **Stream Management** | RB1/RB3 потоки | ✅ Control/Target с UI |
| **Manual Operations** | Basic interface | ✅ Расширенные инструменты |
| **SQL Interface** | Direct execution | ✅ Безопасный UI с паролем |
| **Data Export** | Streamlit download | ✅ Множественные форматы |

## 🚀 Быстрый старт

### Предварительные требования

- **Python 3.8+** для бэкенда
- **Node.js 16+** для фронтенда
- **Oracle Database** (доступ к серверу)
- **Git** для клонирования репозитория

### 1️⃣ Клонирование репозитория

```bash
git clone <repository-url>
cd DataQuery-Pro
```

### 2️⃣ Настройка бэкенда

```bash
cd database-backend

# Создание виртуального окружения
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Установка зависимостей
pip install -r requirements.txt

# Настройка переменных окружения
cp .env.example .env
# Отредактируйте .env файл с вашими настройками Oracle

# Запуск сервера
python main.py
```

Бэкенд будет доступен по адресу: http://localhost:8000

### 3️⃣ Настройка фронтенда

```bash
cd database-interface

# Установка зависимостей
npm install

# Запуск в режиме разработки
npm start
```

Фронтенд будет доступен по адресу: http://localhost:3000

## ⚙️ Конфигурация

### Переменные окружения бэкенда (.env)

```env
# Primary DSSB_APP Database
ORACLE_HOST=your-dssb-app-host
ORACLE_PORT=1521
ORACLE_SID=your-dssb-app-sid
ORACLE_USER=your-dssb-app-username
ORACLE_PASSWORD=your-dssb-app-password

# SPSS Database
SPSS_ORACLE_HOST=your-spss-host
SPSS_ORACLE_PORT=1521
SPSS_ORACLE_SID=your-spss-sid
SPSS_ORACLE_USER=your-spss-username
SPSS_ORACLE_PASSWORD=your-spss-password

# DSSB_OCDS Database (Campaign Management)
DSSB_OCDS_ORACLE_HOST=your-dssb-ocds-host
DSSB_OCDS_ORACLE_PORT=1521
DSSB_OCDS_ORACLE_SID=your-dssb-ocds-sid
DSSB_OCDS_ORACLE_USER=your-dssb-ocds-username
DSSB_OCDS_ORACLE_PASSWORD=your-dssb-ocds-password

# ED_OCDS Database (Campaign Tracking)
ED_OCDS_ORACLE_HOST=your-ed-ocds-host
ED_OCDS_ORACLE_PORT=1521
ED_OCDS_ORACLE_SID=your-ed-ocds-sid
ED_OCDS_ORACLE_USER=your-ed-ocds-username
ED_OCDS_ORACLE_PASSWORD=your-ed-ocds-password

# Application
APP_HOST=0.0.0.0
APP_PORT=8000
ALLOWED_ORIGINS=http://localhost:3000

# LDAP Authentication
LDAP_SERVER=your-ldap-server.com
LDAP_DOMAIN=YOURDOMAIN

# Security
SECRET_KEY=your-secret-key
ACCESS_TOKEN_EXPIRE_MINUTES=480
```

### 🧪 Тестирование подключений

После настройки всех переменных окружения, протестируйте подключения:

```bash
cd database-backend
python test_connections.py
```

Этот скрипт проверит:
- ✅ Все переменные окружения настроены
- ✅ Подключения к каждой из 4 баз данных
- ✅ Общий статус готовности системы

### Переменные окружения фронтенда

```env
REACT_APP_API_URL=http://localhost:8000
```

## 🛡️ Безопасность

- ✅ SQL-инъекции предотвращены через параметризованные запросы
- ✅ Валидация всех пользовательских входных данных
- ✅ Ограничение доступа к таблицам через whitelist
- ✅ Санитизация идентификаторов SQL
- ✅ CORS настроен для разрешенных доменов

## 📊 API Документация

FastAPI автоматически генерирует интерактивную документацию:

- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Основные эндпоинты

```
# Database Management
GET  /databases                    # Список баз данных (теперь 5 БД)
GET  /databases/{id}/tables        # Таблицы БД
GET  /databases/{id}/tables/{name}/columns  # Столбцы таблицы

# Connection Testing
POST /databases/test-connection              # Тест DSSB_APP
POST /databases/test-spss-connection         # Тест SPSS
POST /databases/test-dssb-ocds-connection    # Тест DSSB_OCDS
POST /databases/test-ed-ocds-connection      # Тест ED_OCDS
POST /databases/test-all-connections         # Тест всех БД

# Query Operations
POST /query/execute               # Выполнение запроса
POST /query/count                 # Подсчет строк
GET  /query/history               # История запросов

# Parquet Data Service (Market.py Migration)
GET  /parquet/datasets            # Список всех parquet наборов данных
GET  /parquet/datasets/{name}     # Информация о конкретном наборе
POST /parquet/filter              # Фильтрация IIN по различным критериям
GET  /parquet/cache/stats         # Статистика кеша
POST /parquet/cache/clear         # Очистка кеша

# Campaign Management (Market.py Migration)
GET  /campaigns/codes/next-rb1    # Генерация RB1 кода
GET  /campaigns/codes/next-rb3    # Генерация RB3 кодов  
POST /campaigns/create            # Создание RB1/RB3 кампании
GET  /campaigns/list              # Список кампаний
GET  /campaigns/{code}            # Детали кампании
DELETE /campaigns/{code}          # Удаление кампании

# Theory Management
GET  /theories/active             # Активные теории
POST /theories/create             # Создание теории
POST /theories/stratify-and-create # Стратификация и создание
```

## 🔧 Разработка

### Структура компонентов React

```
src/
├── components/
│   └── Layout.js              # Основной макет с навигацией
├── pages/
│   ├── Dashboard.js           # Панель управления
│   ├── QueryBuilder.js        # Конструктор запросов
│   ├── DataViewer.js          # Просмотр данных
│   └── Settings.js            # Настройки
└── services/
    └── api.js                 # API клиенты
```

### Добавление новых таблиц

Отредактируйте `database-backend/database.py`:

```python
ALLOWED_TABLES = {
    "YOUR_DB": {
        "YOUR_TABLE": {
            "description": "Описание таблицы",
            "columns": [
                {"name": "ID", "type": "NUMBER", "description": "Идентификатор"},
                # ... другие столбцы
            ]
        }
    }
}
```

## 🐛 Отладка

### Логи бэкенда

```bash
# Включить подробные логи
uvicorn main:app --reload --log-level debug
```

### Логи фронтенда

- Откройте DevTools браузера (F12)
- Проверьте консоль на ошибки JavaScript
- Вкладка Network для отладки API запросов

## 📦 Развертывание

### Production сборка фронтенда

```bash
cd database-interface
npm run build
```

### Production запуск бэкенда

```bash
cd database-backend
pip install gunicorn
gunicorn main:app -w 4 -k uvicorn.workers.UvicornWorker
```

### Docker (опционально)

Создайте Dockerfile для каждого сервиса или используйте docker-compose для оркестрации.

## 🤝 Вклад в проект

1. Fork репозитория
2. Создайте feature ветку (`git checkout -b feature/AmazingFeature`)
3. Commit изменения (`git commit -m 'Add AmazingFeature'`)
4. Push в ветку (`git push origin feature/AmazingFeature`)
5. Откройте Pull Request

## 📄 Лицензия

Этот проект лицензирован под MIT License - см. файл [LICENSE](LICENSE) для деталей.

## 🆘 Поддержка

- 📧 Email: support@dataquery-pro.com
- 📖 Документация: [docs.dataquery-pro.com]
- 🐛 Баг-репорты: [GitHub Issues]

---

**DataQuery Pro** - Делаем работу с корпоративными данными простой и доступной! 🚀 