from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
from app.config import Config
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from routers.webhookbuy import routerbuy
from routers.webhooksell import routersell
import os
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(BASE_DIR / 'webhooks.log')
    ]
)
logger = logging.getLogger(__name__)

GOOGLE_SHEETS_CREDENTIALS = BASE_DIR / "credentials.json"
SPREADSHEET_ID = Config.ID_TABLES

COLUMN_HEADERS = [
    "Тикер",
    "Капитализация M",
    "Объем 24H M",
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
    """Инициализация подключения к Google Sheets с созданием заголовков"""
    if not GOOGLE_SHEETS_CREDENTIALS.exists():
        raise FileNotFoundError(f"Credentials file not found at {GOOGLE_SHEETS_CREDENTIALS}")

    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive"
    ]

    creds = ServiceAccountCredentials.from_json_keyfile_name(
        str(GOOGLE_SHEETS_CREDENTIALS), scope)
    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.sheet1

        # Проверяем и создаем заголовки если нужно
        existing_headers = sheet.row_values(1)
        if not existing_headers or existing_headers != COLUMN_HEADERS:
            if existing_headers:
                sheet.clear()
            sheet.insert_row(COLUMN_HEADERS, index=1)
            logger.info("Created column headers in Google Sheet")

        return client, sheet

    except Exception as e:
        logger.error(f"Failed to initialize sheet: {str(e)}")
        raise

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения"""
    try:
        client, sheet = init_google_sheets()
        app.state.google_sheets = client
        app.state.sheet = sheet
        logger.info("Google Sheets initialized successfully")

        app.state.background_tasks = set()
        app.state.update_tasks = {}

        yield

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
    title="TradingView Webhook Processor"
)

app.include_router(routerbuy)
app.include_router(routersell)

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)