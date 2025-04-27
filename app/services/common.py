import asyncio
import logging
import requests
from fastapi import HTTPException
import pytz
from datetime import datetime, timedelta
import gspread
from time import sleep
import gspread.utils

logger = logging.getLogger(__name__)

BYBIT_API_URL = "https://api.bybit.com/v5/market/tickers"
SHEETS_API_DELAY = 1.1  # Задержка между запросами к Google Sheets API (секунды)


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

        if not isinstance(data, dict):
            raise ValueError("Invalid API response: not a dictionary")

        if 'result' not in data or not isinstance(data['result'], dict):
            raise ValueError("Invalid API response: missing or invalid 'result'")

        if 'list' not in data['result'] or not isinstance(data['result']['list'], list):
            raise ValueError("Invalid API response: missing or invalid 'list'")

        if not data['result']['list']:
            raise ValueError(f"No trading data available for {trading_pair}")

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
    """Обновление цен через фиксированные интервалы с гарантированным сохранением"""
    moscow_tz = pytz.timezone('Europe/Moscow')
    if entry_price == 0:
        logger.warning("Цена входа равна 0, устанавливаем минимальную")
        entry_price = 0.000001  # Минимальное значение чтобы избежать деления на 0

    async def safe_cell_update(sheet, range_name, value, format_options=None):
        """Безопасное обновление ячейки с повторными попытками"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if format_options:
                    sheet.format(range_name, format_options)
                    await asyncio.sleep(SHEETS_API_DELAY)

                sheet.update(range_name, [[value]])
                await asyncio.sleep(SHEETS_API_DELAY)
                return True
            except Exception as e:
                logger.warning(f"Попытка {attempt + 1} не удалась: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Не удалось обновить {range_name} после {max_retries} попыток")
                    return False
                await asyncio.sleep(2 ** attempt)  # Экспоненциальная задержка

    try:
        # Получаем дату и время с проверкой существования строки
        try:
            datetime_str = sheet.cell(row_index, 7).value
            await asyncio.sleep(SHEETS_API_DELAY)
        except gspread.exceptions.APIError as e:
            if "exceeds grid limits" in str(e):
                logger.error(f"Строка {row_index} не существует в таблице")
                return
            raise

        if not isinstance(datetime_str, str):
            raise ValueError(f"Некорректный формат даты: {datetime_str}")

        entry_time = moscow_tz.localize(datetime.strptime(datetime_str, "%Y-%m-%d %H:%M:%S"))

        intervals = [
            ('1h', 60 * 60), ('2h', 2 * 60 * 60),
            ('4h', 4 * 60 * 60), ('8h', 8 * 60 * 60),
            ('12h', 12 * 60 * 60), ('1d', 24 * 60 * 60),
            ('3d', 3 * 24 * 60 * 60), ('7d', 7 * 24 * 60 * 60),
            ('14d', 14 * 24 * 60 * 60), ('30d', 30 * 24 * 60 * 60)
        ]

        for name, delay in intervals:
            try:
                target_time = entry_time + timedelta(seconds=delay)
                sleep_duration = (target_time - datetime.now(moscow_tz)).total_seconds()

                if sleep_duration > 0:
                    logger.info(f"Ожидание {name} обновления для {symbol} (через {sleep_duration:.0f} сек)")
                    await asyncio.sleep(sleep_duration)

                current_price = await get_bybit_price(symbol)
                if entry_price == 0:
                    logger.error("Цена входа равна 0, пропускаем расчет")
                    continue

                change_pct = ((current_price - entry_price) / entry_price) * 100 if action.lower() == 'buy' else ((
                                                                                                                              entry_price - current_price) / entry_price) * 100
                change_decimal = round(change_pct / 100, 6)

                interval_idx = intervals.index((name, delay))
                price_col = 8 + interval_idx * 2
                pct_col = price_col + 1

                price_cell = gspread.utils.rowcol_to_a1(row_index, price_col)
                pct_cell = gspread.utils.rowcol_to_a1(row_index, pct_col)

                # Форматирование и запись с обработкой ошибок
                format_success = await safe_cell_update(
                    sheet,
                    pct_cell,
                    change_decimal,
                    {
                        "numberFormat": {"type": "PERCENT", "pattern": "#,##0.00%"},
                        "backgroundColor": {
                            "red": 0.5 if change_pct >= 0 else 1,
                            "green": 1 if change_pct >= 0 else 0.5,
                            "blue": 0.5
                        }
                    }
                )

                price_success = await safe_cell_update(sheet, price_cell, current_price)

                if format_success and price_success:
                    logger.info(
                        f"Успешно обновлен {name} для {symbol}: цена {current_price}, изменение {change_pct:.2f}%")
                else:
                    logger.error(f"Ошибка при обновлении интервала {name}")

            except Exception as e:
                logger.error(f"Ошибка при обновлении интервала {name}: {str(e)}", exc_info=True)
                continue

    except Exception as e:
        logger.error(f"Критическая ошибка: {str(e)}", exc_info=True)
    finally:
        if hasattr(update_price_periodically, 'update_tasks'):
            update_price_periodically.update_tasks.pop(symbol, None)