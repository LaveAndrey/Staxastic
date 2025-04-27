from fastapi import APIRouter, Request, HTTPException
import asyncio
import logging
from app.services.telegram import TelegramBot
from app.services.cmc import CoinMarketCapService
from app.config import Config
import pytz
from datetime import datetime
from app.services.common import get_bybit_price, update_price_periodically
from time import sleep

routerbuy = APIRouter()
logger = logging.getLogger(__name__)
cmc = CoinMarketCapService(api_key=Config.COINMARKETCAP_API_KEY)

SPREADSHEET_ID = Config.ID_TABLES
update_price_periodically.update_tasks = {}
SHEETS_API_DELAY = 1.1


@routerbuy.post("/webhookbuy")
async def webhook(request: Request):
    try:
        if not hasattr(request.app.state, 'google_sheets'):
            raise HTTPException(status_code=503, detail="Service unavailable")

        data = await request.json()
        await asyncio.sleep(3)

        client = request.app.state.google_sheets
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1
        sleep(SHEETS_API_DELAY)

        ticker = data.get('ticker', 'N/A')
        close = data.get('close', 'N/A')
        symbol = cmc.extract_symbol(ticker.lower())

        market_cap, volume_24h = await cmc.get_market_data(symbol)
        # Защита от None значений
        market_cap = market_cap if market_cap is not None else 0
        volume_24h = volume_24h if volume_24h is not None else 0

        try:
            current_price = await get_bybit_price(symbol)
        except:
            current_price = 0.0  # Значение по умолчанию при ошибке

        coif = cmc.coifecent(market_cap, volume_24h)

        fire_emoji = "🔥 " if coif >= 1 else ""

        message = (
            f"🟢 *BUY* {fire_emoji}\n\n"
            f"*{symbol.upper()}*\n\n"
            f"PRICE - *{close}$*\n"
            f"MARKET CAP - *{cmc.format_number(market_cap)}*\n"
            f"24H VOLUME - *{cmc.format_number(volume_24h)}*\n"
            f"TVMCR - *{coif}*"
        )

        try:
            TelegramBot.send_message(text=message, chat_id=Config.CHAT_ID_TRADES)
            logger.info(message)
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            raise HTTPException(status_code=500, detail="Failed to send notification")


        # Добавляем новую строку с данными
        sheet.append_row([
            symbol.upper(),
            cmc.format_number_m(market_cap) if market_cap else 0,
            cmc.format_number_m(volume_24h) if volume_24h else 0,
            coif if coif is not None else 0,
            'buy',
            close,
            datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d %H:%M:%S"),
            *[""] * 20
        ])
        sleep(SHEETS_API_DELAY)

        # Получаем индекс новой строки
        row_index = len(sheet.get_all_values())
        sleep(SHEETS_API_DELAY)

        # Подготовка форматирования
        format_requests = []

        if coif >= 1:
            format_requests.append({
                'range': f"D{row_index}",
                'format': {
                    'backgroundColor': {
                        'red': 0.5,
                        'green': 0.5,
                        'blue': 1
                    },
                    'textFormat': {
                        'bold': True
                    }
                }
            })

        format_requests.append({
            'range': f"B{row_index}:D{row_index}",
            'format': {
                'numberFormat': {
                    'type': 'NUMBER',
                    'pattern': '#,##0.000'
                }
            }
        })

        # Применяем форматирование
        if format_requests:
            sheet.batch_format(format_requests)
            sleep(SHEETS_API_DELAY)

        task = asyncio.create_task(
            update_price_periodically(sheet, row_index, symbol, float(current_price), "buy")
        )
        update_price_periodically.update_tasks[symbol] = task

        return {"status": "success"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")