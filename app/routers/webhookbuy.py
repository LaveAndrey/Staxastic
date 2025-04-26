from fastapi import APIRouter, Request, HTTPException
import asyncio
import logging
from app.services.telegram import TelegramBot
from app.services.cmc import CoinMarketCapService
from app.config import Config
import pytz
from datetime import datetime
from app.services.common import get_bybit_price, update_price_periodically

routerbuy = APIRouter()
logger = logging.getLogger(__name__)
cmc = CoinMarketCapService(api_key=Config.COINMARKETCAP_API_KEY)

SPREADSHEET_ID = Config.ID_TABLES
update_price_periodically.update_tasks = {}

@routerbuy.post("/webhookbuy")
async def webhook(request: Request):
    try:
        await asyncio.sleep(5)

        if not hasattr(request.app.state, 'google_sheets'):
            raise HTTPException(status_code=503, detail="Service unavailable")

        client = request.app.state.google_sheets
        sheet = client.open_by_key(SPREADSHEET_ID).sheet1
        data = await request.json()

        ticker = data.get('ticker', 'N/A')
        close = data.get('close', 'N/A')
        symbol = cmc.extract_symbol(ticker.lower())

        market_cap, volume_24h = await cmc.get_market_data(symbol)
        current_price = await get_bybit_price(symbol)

        message = (
            f"🟢 *BUY*\n\n*{symbol.upper()}*\n\n"
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

        sheet.append_row([
            symbol.upper(),
            cmc.format_number_m(market_cap),
            cmc.format_number_m(volume_24h),
            cmc.coifecent(market_cap, volume_24h),
            'buy',
            close,
            datetime.now(pytz.timezone('Europe/Moscow')).strftime("%Y-%m-%d %H:%M:%S"),
            *[""] * 20
        ])

        row_index = len(sheet.get_all_values())
        sheet.format(f"B{row_index}:C{row_index}", {
            "numberFormat": {
                "type": "NUMBER",
                "pattern": "#,##0.00"
            }
        })
        sheet.format(f"D{row_index}", {
            "numberFormat": {
                "type": "NUMBER",
                "pattern": "#,##0.00"
            }
        })
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