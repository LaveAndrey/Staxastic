import asyncio
import logging
import requests
from fastapi import HTTPException
import pytz
from datetime import datetime, timedelta
import gspread

logger = logging.getLogger(__name__)

BYBIT_API_URL = "https://api.bybit.com/v5/market/tickers"


async def get_bybit_price(symbol: str) -> float:
    """Получаем текущую цену с Bybit с улучшенной обработкой ошибок"""
    try:
        clean_symbol = symbol.upper().strip()
        if not clean_symbol:
            raise ValueError("Empty symbol provided")

        trading_pair = f"{clean_symbol}USDT"

        response = requests.get(
            BYBIT_API_URL,
            params={
                "category": "linear",
                "symbol": trading_pair
            },
            timeout=10
        )
        response.raise_for_status()

        data = response.json()

        # Улучшенная проверка структуры ответа
        if not isinstance(data, dict):
            raise ValueError("Invalid API response: not a dictionary")

        if 'result' not in data or not isinstance(data['result'], dict):
            raise ValueError("Invalid API response: missing or invalid 'result'")

        if 'list' not in data['result'] or not isinstance(data['result']['list'], list):
            raise ValueError("Invalid API response: missing or invalid 'list'")

        if not data['result']['list']:
            raise ValueError(f"No trading data available for {trading_pair}")

        # Получаем первый элемент списка
        ticker = data["result"]["list"][0]

        if 'lastPrice' not in ticker:
            raise ValueError("Ticker data missing 'lastPrice' field")

        price = float(ticker["lastPrice"])
        logger.info(f"Успешно получена цена для {clean_symbol}: {price}")
        return price

    except requests.exceptions.HTTPError as e:
        error_detail = f"{e.response.status_code} - {e.response.text}" if e.response else str(e)
        logger.error(f"Ошибка запроса к Bybit API: {error_detail}")
        raise HTTPException(
            status_code=502,
            detail=f"Bybit API error: {error_detail}"
        )
    except ValueError as e:
        logger.error(f"Ошибка обработки данных Bybit: {str(e)}")
        raise HTTPException(
            status_code=503,
            detail=f"Invalid Bybit data format: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Неожиданная ошибка при получении цены: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=503,
            detail="Failed to get price from Bybit"
        )


async def update_price_periodically(sheet, row_index: int, symbol: str, entry_price: float, action: str):
    """Обновление цен через фиксированные интервалы"""
    moscow_tz = pytz.timezone('Europe/Moscow')
    try:
        # Получаем дату и время из правильной колонки (6-я колонка - индекс 5)
        datetime_str = sheet.cell(row_index, 6).value  # Колонка с датой/временем

        # Проверяем, что значение является строкой с датой
        if not isinstance(datetime_str, str) or len(datetime_str) < 10:
            raise ValueError(f"Invalid datetime format: {datetime_str}")

        entry_time = moscow_tz.localize(datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S"))

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
                    logger.info(f"Ожидание {name} обновления для {symbol} (через {sleep_duration:.0f} сек)")
                    await asyncio.sleep(sleep_duration)

                current_price = await get_bybit_price(symbol)
                if action.lower() == 'buy':
                    change_pct = ((current_price - entry_price) / entry_price) * 100
                else:
                    change_pct = ((entry_price - current_price) / entry_price) * 100

                col = 7 + intervals.index((name, delay)) * 2
                sheet.update_cell(row_index, col, current_price)
                sheet.update_cell(row_index, col + 1, change_pct / 100)

                # Форматирование ячеек
                col_letter = chr(ord('A') + col)
                sheet.format(f"{col_letter}{row_index}", {
                    "numberFormat": {"type": "PERCENT", "pattern": "#,##0.00%"}
                })
                format_cell(sheet, row_index, col + 1, change_pct)

                logger.info(f"Обновлен интервал {name} для {symbol}")
            except Exception as e:
                logger.error(f"Ошибка при обновлении интервала {name}: {e}")
                continue

        logger.info(f"Все интервалы обновлены для {symbol}")

    except Exception as e:
        logger.error(f"Ошибка в update_price_periodically: {e}")
    finally:
        if hasattr(update_price_periodically, 'update_tasks'):
            update_price_periodically.update_tasks.pop(symbol, None)


def format_cell(sheet, row: int, col: int, value: float):
    """Форматирование ячейки в зависимости от значения"""
    try:
        if value == 0:
            return

        col_letter = chr(ord('A') + col - 1)
        color = {"red": 0.5, "green": 1, "blue": 0.5} if value >= 0 else {"red": 1, "green": 0.5, "blue": 0.5}
        sheet.format(f"{col_letter}{row}", {"backgroundColor": color})
    except Exception as e:
        logger.error(f"Ошибка форматирования ячейки: {e}")