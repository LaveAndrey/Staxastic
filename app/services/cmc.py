import logging
from typing import Tuple, Optional, Dict
import asyncio
import requests
from requests.exceptions import RequestException

logger = logging.getLogger(__name__)


class CoinMarketCapService:
    def __init__(self, api_key: str, retries: int = 3, delay: float = 1.0):
        self.api_key = api_key
        self.retries = retries
        self.delay = delay
        self._coin_cache = {}
        self.base_url = "https://pro-api.coinmarketcap.com/v2"  # Обновлено до v2 API

    async def _get_all_coins(self) -> Dict:
        """Получает и кэширует список всех монет"""
        if not self._coin_cache:
            try:
                url = f"{self.base_url}/cryptocurrency/map"
                headers = {
                    'Accepts': 'application/json',
                    'X-CMC_PRO_API_KEY': self.api_key
                }

                response = requests.get(url, headers=headers)
                response.raise_for_status()

                data = response.json()
                coins = data.get('data', [])
                self._coin_cache = {coin['symbol'].lower(): coin for coin in coins}
                logger.info("Кэш монет успешно обновлен")
            except RequestException as e:
                logger.error(f"Ошибка получения списка монет: {e}")
                raise
        return self._coin_cache

    async def get_market_data(self, symbol: str) -> Tuple[Optional[float], Optional[float]]:
        """Получает рыночные данные (капитализацию и объем)"""
        clean_symbol = self.extract_symbol(symbol).upper()

        for attempt in range(self.retries):
            try:
                url = f"{self.base_url}/cryptocurrency/quotes/latest"
                headers = {
                    'Accepts': 'application/json',
                    'X-CMC_PRO_API_KEY': self.api_key
                }
                params = {
                    'symbol': clean_symbol,
                    'convert': 'USD'
                }

                response = requests.get(url, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()

                # Улучшенная обработка структуры ответа
                if not data.get('data'):
                    logger.error(f"Нет данных для {clean_symbol} в ответе API. Полный ответ: {data}")
                    return None, None

                # Получаем данные первой монеты из ответа
                coin_data = list(data['data'].values())[0] if data['data'] else None
                if not coin_data:
                    logger.error(f"Нет данных о монете {clean_symbol}")
                    return None, None

                # Проверяем структуру данных
                if isinstance(coin_data, list):
                    coin_data = coin_data[0]  # Берем первый элемент если это список

                quote = coin_data.get('quote', {}).get('USD', {})
                if not quote:
                    logger.error(f"Нет котировок USD для {clean_symbol}")
                    return None, None

                market_cap = quote.get('market_cap')
                volume = quote.get('volume_24h')

                if market_cap is None or volume is None:
                    logger.warning(f"Отсутствуют данные для {clean_symbol}, попытка {attempt + 1}")
                    await asyncio.sleep(self.delay)
                    continue

                return market_cap, volume

            except requests.exceptions.RequestException as e:
                logger.error(f"Ошибка запроса (попытка {attempt + 1}): {str(e)}")
                if attempt < self.retries - 1:
                    await asyncio.sleep(self.delay)
                continue
            except Exception as e:
                logger.error(f"Критическая ошибка при обработке данных: {str(e)}", exc_info=True)
                return None, None

        return None, None

    @staticmethod
    def extract_symbol(ticker: str) -> str:
        """Извлекает чистый символ из тикера"""
        ticker = ticker.upper()
        for suffix in ["USDT.P", "USDT", "PERP", "USD.P"]:
            if ticker.endswith(suffix):
                return ticker[:-len(suffix)]
        return ticker

    @staticmethod
    def format_number(value: Optional[float]) -> str:
        """Форматирование чисел с защитой от None"""
        if value is None:
            return "0"  # Возвращаем 0 вместо N/A
        return f"{int(value):,}$"


    @staticmethod
    def format_number_m(value: Optional[float]) -> float:
        """Форматирование в миллионы с защитой от None"""
        if value is None:
            return 0.0
        return round(value / 1_000_000, 2)

    @staticmethod
    def coifecent(market_cap: Optional[float], volume_24h: Optional[float]) -> float:
        """Расчет коэффициента с защитой от нулевых значений"""
        if market_cap is None or volume_24h is None:
            return 0.0  # Возвращаем 0 вместо None

        if market_cap == 0 or volume_24h == 0:
            return 0.0

        try:
            return round(volume_24h / market_cap, 3)
        except Exception as e:
            logger.error(f"Ошибка расчета коэффициента: {e}")
            return 0.0
