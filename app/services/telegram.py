import requests
import time
from app.config import Config
import logging

logger = logging.getLogger(__name__)


class TelegramBot:
    @staticmethod
    def send_message(chat_id: str, text: str) -> bool:
        max_retries = 3
        backoff_time = 1

        for attempt in range(max_retries):
            try:
                response = requests.post(
                    f"https://api.telegram.org/bot{Config.TOKEN}/sendMessage",
                    json={
                        "chat_id": chat_id,
                        "text": text,
                        "parse_mode": "Markdown",
                        "disable_web_page_preview": True
                    },
                    timeout=5
                )

                # Обработка лимитов Telegram
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 5))
                    logger.warning(f"Rate limited. Sleeping for {retry_after} sec")
                    time.sleep(retry_after)
                    continue

                response.raise_for_status()
                return True

            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(backoff_time * (attempt + 1))

        logger.error("All sending attempts failed")
        return False