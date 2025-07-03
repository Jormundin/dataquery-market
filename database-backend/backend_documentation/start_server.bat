@echo off
echo DataQuery Pro Backend - Запуск сервера разработки
echo ================================================

REM Проверяем, активировано ли виртуальное окружение
if not exist "venv\Scripts\activate.bat" (
    echo Создание виртуального окружения...
    python -m venv venv
)

REM Активируем виртуальное окружение
call venv\Scripts\activate.bat

REM Устанавливаем зависимости если requirements.txt новее установленных пакетов
echo Проверка зависимостей...
pip install -r requirements.txt

REM Проверяем наличие .env файла
if not exist ".env" (
    echo ВНИМАНИЕ: Файл .env не найден!
    echo Скопируйте env_example.txt в .env и настройте переменные окружения
    echo.
    pause
    exit /b 1
)

echo.
echo Запуск FastAPI сервера...
echo API документация будет доступна по адресу: http://172.28.80.18:1555/docs
echo Для остановки сервера нажмите Ctrl+C
echo.

python main.py

pause 