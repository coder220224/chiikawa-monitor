import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = int(os.getenv('CHANNEL_ID'))
MONGODB_URI = os.getenv('MONGODB_URI')
WORK_DIR = os.path.dirname(os.path.abspath(__file__))
DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1376888371284279316/ZcfbUjQNZ1KrsKkxq1xLiWCdWx-yZ361Fzd58wo8nZkDbL3XuoaDgJD99EhWb-EEOhkn"
