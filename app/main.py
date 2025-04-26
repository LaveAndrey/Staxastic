from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from app.config import Config
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from routers.webhookbuy import routerbuy
from routers.webhooksell import routersell
from pathlib import Path
import time

# Константы
BASE_DIR = Path(__file__).parent.parent
GOOGLE_SHEETS_CREDENTIALS = BASE_DIR / "credentials.json"
SPREADSHEET_ID = Config.ID_TABLES
SHEETS_API_DELAY = 1.1  # Задержка между запросами к API

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(BASE_DIR / 'webhooks.log')
    ]
)
logger = logging.getLogger(__name__)

# Заголовки столбцов
COLUMN_HEADERS = [
    "Тикер",
    "Капитализация M$",
    "Объем 24H M$",
    "Коэффициент",
    "Действие",
    "Цена по сигналу",
    "Дата и время сигнала",
    "Закрытие 1h",
    "Рост/падение 1h",
    "Закрытие 2h",
    "Рост/падение 2h",
    "Закрытие 4h",
    "Рост/падение 4h",
    "Закрытие 8h",
    "Рост/падение 8h",
    "Закрытие 12h",
    "Рост/падение 12h",
    "Закрытие 1d",
    "Рост/падение 1d",
    "Закрытие 3d",
    "Рост/падение 3d",
    "Закрытие 7d",
    "Рост/падение 7d",
    "Закрытие 14d",
    "Рост/падение 14d",
    "Закрытие 30d",
    "Рост/падение 30d",
]


def init_google_sheets():
    """Инициализация подключения к Google Sheets с оптимизированными запросами"""
    if not GOOGLE_SHEETS_CREDENTIALS.exists():
        raise FileNotFoundError(f"Credentials file not found at {GOOGLE_SHEETS_CREDENTIALS}")

    try:
        # Авторизация
        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            str(GOOGLE_SHEETS_CREDENTIALS), scope)
        client = gspread.authorize(creds)
        time.sleep(SHEETS_API_DELAY)

        # Открытие таблицы
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.sheet1
        time.sleep(SHEETS_API_DELAY)

        # Проверка и создание заголовков одним запросом
        existing_headers = sheet.row_values(1)
        time.sleep(SHEETS_API_DELAY)

        if not existing_headers or existing_headers != COLUMN_HEADERS:
            if existing_headers:
                sheet.clear()
                time.sleep(SHEETS_API_DELAY)
            sheet.insert_row(COLUMN_HEADERS, index=1)
            time.sleep(SHEETS_API_DELAY)
            logger.info("Created column headers in Google Sheet")

        return client, sheet

    except Exception as e:
        logger.error(f"Failed to initialize sheet: {str(e)}")
        raise


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения с обработкой ошибок"""
    try:
        client, sheet = init_google_sheets()
        app.state.google_sheets = client
        app.state.sheet = sheet

        # Инициализация структур для управления задачами
        app.state.background_tasks = set()
        app.state.update_tasks = {}
        app.state.last_api_call = time.time()

        logger.info("Google Sheets initialized successfully")
        yield

        # Корректное завершение задач
        for task in app.state.background_tasks:
            task.cancel()
        for task in app.state.update_tasks.values():
            task.cancel()

    except Exception as e:
        logger.critical(f"Application startup failed: {str(e)}")
        raise
    finally:
        logger.info("Application shutdown")


app = FastAPI(
    lifespan=lifespan,
    title="TradingView Webhook Processor",
    description="API для обработки вебхуков TradingView с интеграцией Google Sheets",
    version="1.0.0"
)

# Подключение роутеров
app.include_router(routerbuy, prefix="/api/v1")
app.include_router(routersell, prefix="/api/v1")

if __name__ == '__main__':
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_config=None  # Используем нашу конфигурацию логирования
    )