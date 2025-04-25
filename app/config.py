from dotenv import load_dotenv
import os

load_dotenv()

class Config():
    TOKEN = os.getenv('TOKENTELEGRAM')
    CHAT_ID_TRADES = os.getenv('CHAT_IDTELEGRAM')
    ID_TABLES = os.getenv('ID_TABLES')
    COINMARKETCAP_API_KEY = os.getenv('COINMARKETCAP_API_KEY')