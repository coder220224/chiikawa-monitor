import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv('DISCORD_TOKEN')
CHANNEL_ID = int(os.getenv('CHANNEL_ID'))
# Render 的持久化儲存路徑
WORK_DIR = '/opt/render/project/data'

# 如果目錄不存在，建立它
if not os.path.exists(WORK_DIR):
    os.makedirs(WORK_DIR)
