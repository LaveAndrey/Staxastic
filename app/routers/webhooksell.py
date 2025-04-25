from fastapi import APIRouter, Request, HTTPException, BackgroundTasks
from datetime import datetime, timedelta
from typing import Dict
import asyncio
import logging
from app.services.telegram import TelegramBot
from app.services.cmc import CoinMarketCapService
from app.config import Config
import pytz
import requests

routersell = APIRouter()
logger = logging.getLogger(__name__)
cmc = CoinMarketCapService(api_key=Config.COINMARKETCAP_API_KEY)

SPREADSHEET_ID = Config.ID_TABLES  # ID вашей Google Таблицы
BYBIT_API_URL = "https://api.bybit.com/v5/market/tickers"

update_tasks: Dict[str, asyncio.Task] = {}

async def get_bybit_price(symbol: str) -> float:
    """Получаем текущую цену с Bybit"""
    try:
        # Очистка и валидация символа
        clean_symbol = symbol.upper().strip()
        if not clean_symbol:
            raise ValueError("Empty symbol provided")

        # Формируем торговую пару для Bybit
        trading_pair = f"{clean_symbol}USDT"

        # Выполняем запрос к Bybit API
        response = requests.get(
            BYBIT_API_URL,
            params={
                "category": "linear",  # Используйте "spot" для спотового рынка
                "symbol": trading_pair
            },
            timeout=10
        )

        response.raise_for_status()
        data = response.json()

        # Проверяем структуру ответа Bybit
        if not isinstance(data, dict) or 'result' not in data or not data['result']:
            raise ValueError(f"Invalid API response structure: {data}")

        # Получаем цену последней сделки
        ticker = data["result"]["list"][0]
        price = float(ticker["lastPrice"])
        logger.info(f"Успешно получена цена для {clean_symbol} с Bybit: {price}")

        return price

    except requests.exceptions.HTTPError as e:
        error_detail = f"{e.response.status_code} - {e.response.text}" if e.response else str(e)
        logger.error(f"Ошибка запроса к Bybit API: {error_detail}")
        raise HTTPException(
            status_code=502,
            detail=f"Bybit API error: {error_detail}"
        )
    except Exception as e:
        logger.error(f"Ошибка при получении цены с Bybit: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Failed to get price from Bybit"
        )

async def update_price_periodically(sheet, row_index: int, symbol: str, entry_price: float, action: str):
    """Обновление цен через фиксированные интервалы после сигнала"""
    moscow_tz = pytz.timezone('Europe/Moscow')

    try:
        entry_time_str = sheet.cell(row_index, 4).value
        entry_time = moscow_tz.localize(datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S"))

        intervals = [
            ('1h', 60 * 60),
            ('2h', 2 * 60 * 60),
            ('4h', 4 * 60 * 60),
            ('8h', 8 * 60 * 60),
            ('12h', 12 * 60 * 60),
            ('1d', 24 * 60 * 60),
            ('3d', 3 * 24 * 60 * 60),
            ('7d', 7 * 24 * 60 * 60),
            ('14d', 14 * 24 * 60 * 60),
            ('30d', 30 * 24 * 60 * 60)
        ]

        for name, delay in intervals:
            try:
                target_time = entry_time + timedelta(seconds=delay)
                sleep_duration = (target_time - datetime.now(moscow_tz)).total_seconds()

                if sleep_duration > 0:
                    logger.info(f"Ожидание {name} обновления для {symbol}")
                    await asyncio.sleep(sleep_duration)

                current_price = await get_bybit_price(symbol)

                change_pct = ((entry_price - current_price) / entry_price) * 100

                col = 5 + intervals.index((name, delay)) * 2
                sheet.update_cell(row_index, col, current_price)
                sheet.update_cell(row_index, col + 1, change_pct / 100)

                # Форматирование ячеек
                col_letter = chr(ord('A') + col)
                percent_cell = f"{col_letter}{row_index}"
                sheet.format(percent_cell, {
                    "numberFormat": {
                        "type": "PERCENT",
                        "pattern": "#,##0.00%"
                    }
                })
                format_cell(sheet, row_index, col + 1, change_pct)

                logger.info(f"Обновлен интервал {name} для {symbol}")

            except Exception as e:
                logger.error(f"Ошибка при обновлении интервала {name}: {e}")
                continue

    except Exception as e:
        logger.error(f"Ошибка в update_price_periodically: {e}")
    finally:
        if symbol in update_tasks:
            update_tasks.pop(symbol)

@routersell.post("/webhooksell")
async def webhook(request: Request):
    try:
        if not hasattr(request.app.state, 'google_sheets'):
            raise HTTPException(status_code=503, detail="Service unavailable")

        client = request.app.state.google_sheets
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1

        data = await request.json()
        logger.info(f"Processing data: {data}")

        ticker = data.get('ticker', 'N/A')
        close = data.get('close', 'N/A')
        symbol = cmc.extract_symbol(ticker.lower())

        # Получаем рыночные данные с CoinMarketCap
        market_cap, volume_24h = await cmc.get_market_data(symbol)
        current_price = await get_bybit_price(symbol)

        # Формируем сообщение для Telegram
        message = (
            f"🔴 *SELL*\n\n"
            f"*{symbol.upper()}*\n\n"
            f"PRICE - *{close}$*\n"
            f"MARKET CAP - *{cmc.format_number(market_cap)}*\n"
            f"24H VOLUME - *{cmc.format_number(volume_24h)}*\n\n"
        )

        try:
            TelegramBot.send_message(text=message, chat_id=Config.CHAT_ID_TRADES)
            logger.info(message)
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            raise HTTPException(status_code=500, detail="Failed to send notification")

        # Запись в Google Sheets
        sheet.append_row([
            symbol.upper(),
            market_cap,
            volume_24h,
            'sell',
            close,
            datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d %H:%M:%S"),
            *[""] * 20  # Пустые колонки для интервалов
        ])

        row_index = len(sheet.get_all_values())
        task = asyncio.create_task(
            update_price_periodically(sheet, row_index, symbol, float(current_price))
        )
        update_tasks[symbol] = task

        return {"status": "success"}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


def format_cell(sheet, row: int, col: int, value: float):
    """Форматирование ячейки в зависимости от значения"""
    try:
        col_letter = chr(ord('A') + col - 1)
        cell_ref = f"{col_letter}{row}"

        if value == 0:
            return

        if value >= 0:
            sheet.format(cell_ref, {"backgroundColor": {"red": 0.5, "green": 1, "blue": 0.5}})
        else:
            sheet.format(cell_ref, {"backgroundColor": {"red": 1, "green": 0.5, "blue": 0.5}})
    except Exception as e:
        logger.error(f"Ошибка форматирования ячейки: {e}")



















