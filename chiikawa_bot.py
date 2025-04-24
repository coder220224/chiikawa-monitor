import discord
from discord.ext import commands, tasks
from datetime import datetime, timedelta
import os
import aiohttp
import asyncio
from chiikawa_monitor import ChiikawaMonitor
import logging
import sys
from config import TOKEN, WORK_DIR, MONGODB_URI
from aiohttp import web
import socket
import ssl
import traceback
import json
import signal
import pytz
from linebot import LineBotApi, WebhookHandler
from linebot.exceptions import InvalidSignatureError
from linebot.models import (
    MessageEvent, TextMessage, TextSendMessage,
    FlexSendMessage, BubbleContainer, BoxComponent,
    TextComponent, ButtonComponent, URIAction, CarouselContainer,
    ImageComponent
)

# 設定台灣時區
TW_TIMEZONE = pytz.timezone('Asia/Taipei')

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 从环境变量获取 LINE Bot 配置
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN', '')
LINE_CHANNEL_SECRET = os.environ.get('LINE_CHANNEL_SECRET', '')

# 進程鎖文件路徑
LOCK_FILE = os.path.join(WORK_DIR, 'bot.lock')

def check_running():
    """檢查是否已有實例在運行"""
    try:
        if os.path.exists(LOCK_FILE):
            with open(LOCK_FILE, 'r') as f:
                data = json.load(f)
                pid = data.get('pid')
                start_time = data.get('start_time')
                
                # 檢查進程是否存在
                try:
                    os.kill(pid, 0)
                    logger.warning(f"檢測到另一個 Bot 實例正在運行 (PID: {pid}, 啟動時間: {start_time})")
                    return True
                except OSError:
                    logger.info("發現過期的鎖文件，將刪除")
                    os.remove(LOCK_FILE)
        return False
    except Exception as e:
        logger.error(f"檢查運行狀態時發生錯誤：{str(e)}")
        return False

def create_lock():
    """創建進程鎖文件"""
    try:
        data = {
            'pid': os.getpid(),
            'start_time': datetime.now().isoformat()
        }
        with open(LOCK_FILE, 'w') as f:
            json.dump(data, f)
        logger.info(f"已創建進程鎖文件 (PID: {os.getpid()})")
    except Exception as e:
        logger.error(f"創建進程鎖文件時發生錯誤：{str(e)}")

def remove_lock():
    """移除進程鎖文件"""
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
            logger.info("已移除進程鎖文件")
    except Exception as e:
        logger.error(f"移除進程鎖文件時發生錯誤：{str(e)}")

def signal_handler(signum, frame):
    """處理進程終止信號"""
    logger.info(f"收到信號 {signum}，準備關閉 Bot...")
    remove_lock()
    sys.exit(0)

# 註冊信號處理器
signal.signal(signal.SIGTERM, signal_handler)
signal.signal(signal.SIGINT, signal_handler)

if not os.path.exists(WORK_DIR):
    os.makedirs(WORK_DIR)
    logger.info(f"創建工作目錄：{WORK_DIR}")

# 設置 Bot
intents = discord.Intents.default()
intents.message_content = True

# 使用代理設置創建 Bot
class ProxyBot(commands.Bot):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session = None
        self.connector = None
        self.web_server_task = None
        self.port = int(os.getenv('PORT', 8080))
        self.last_mongodb_check = None
        self.mongodb_status = False
        self.start_time = datetime.now(TW_TIMEZONE)
        logger.info(f"初始化 Bot，端口：{self.port}")

    async def setup_hook(self):
        try:
            self.connector = aiohttp.TCPConnector(
                ssl=False,
                force_close=True,
                limit=None
            )
            logger.info("已創建 aiohttp 連接器")
            
            self.session = aiohttp.ClientSession(
                connector=self.connector
            )
            logger.info("已創建 aiohttp 會話")
            
            self.web_server_task = self.loop.create_task(setup_webserver())
            logger.info("Web 服務器啟動中...")
            
        except Exception as e:
            logger.error(f"setup_hook 錯誤：{str(e)}")
            logger.error(traceback.format_exc())

    async def start(self, *args, **kwargs):
        try:
            await super().start(*args, **kwargs)
        except Exception as e:
            print(f"啟動時發生錯誤: {e}")
            raise

    async def close(self):
        try:
            if self.session:
                await self.session.close()
            if self.connector:
                await self.connector.close()
            if self.web_server_task:
                self.web_server_task.cancel()
                try:
                    await self.web_server_task
                except asyncio.CancelledError:
                    pass
            
            # 移除進程鎖
            remove_lock()
            
            await super().close()
        except Exception as e:
            logger.error(f"關閉時發生錯誤：{str(e)}")

bot = ProxyBot(command_prefix='!', intents=intents)

# 初始化監控器
monitor = ChiikawaMonitor()

# 初始化 LINE Bot
line_bot_api = LineBotApi(LINE_CHANNEL_ACCESS_TOKEN)
line_handler = WebhookHandler(LINE_CHANNEL_SECRET)

# 添加日誌記錄
logging.basicConfig(
    filename=os.path.join(WORK_DIR, 'bot.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def check_updates(ctx):
    """檢查商品更新"""
    try:
        channel = ctx.channel
        if not channel:
            logger.error(f"無法獲取頻道")
            return
            
        current_time = datetime.now(TW_TIMEZONE).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"\n=== {current_time} 開始檢查更新 ===")
        
        # 獲取舊的商品資料
        try:
            old_products = {p['url']: p for p in monitor.get_all_products()}
            logger.info(f"成功獲取現有商品數據：{len(old_products)} 個")
        except Exception as e:
            error_msg = f"獲取現有商品數據失敗：{str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            await channel.send(f"錯誤：{error_msg}")
            return
        
        # 獲取新的商品資料
        try:
            logger.info("開始獲取新商品數據...")
            new_products_data = await bot.loop.run_in_executor(None, monitor.fetch_products)
            
            if not new_products_data:
                error_msg = "獲取新商品數據失敗：返回空列表"
                logger.error(error_msg)
                logger.error("請檢查 fetch_products 函數的執行情況")
                await channel.send(f"錯誤：{error_msg}")
                return
                
            new_products = {p['url']: p for p in new_products_data}
            logger.info(f"成功獲取新商品數據：{len(new_products)} 個")
            
        except Exception as e:
            error_msg = f"獲取新商品數據時發生錯誤：{str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            await channel.send(f"錯誤：{error_msg}")
            return
            
        # 檢查是否是第一次執行（資料庫為空）
        is_first_run = len(old_products) == 0
        logger.info(f"是否首次執行：{is_first_run}")
        
        # 比對差異
        new_listings = []  # 新上架
        delisted = []      # 下架
        missing_products = []  # 暫時找不到的商品
        
        # 檢查新上架
        for url, new_product in new_products.items():
            if url not in old_products and not is_first_run:  # 如果是第一次執行，不標記為新上架
                new_listings.append((new_product['name'], url))
                await bot.loop.run_in_executor(None, lambda: monitor.record_history(new_product, 'new'))
                logger.info(f"新商品上架: {new_product['name']}")
        
        # 檢查下架（如果不是第一次執行才檢查）
        if not is_first_run:
            for url, old_product in old_products.items():
                if url not in new_products:
                    missing_products.append((old_product['name'], url))
                    logger.info(f"商品不見了，準備檢查 URL: {old_product['name']}")
            
            # 只對不見的商品進行 URL 檢查
            for name, url in missing_products:
                is_available = await bot.loop.run_in_executor(None, lambda u=url: monitor.check_product_url(u))
                if not is_available:
                    delisted.append((name, url))
                    await bot.loop.run_in_executor(None, lambda n=name, u=url: monitor.record_history({'name': n, 'url': u}, 'delisted'))
                    logger.info(f"確認商品已下架: {name}")
                else:
                    logger.info(f"商品 {name} 暫時不在列表中，但 URL 仍可訪問")
        
        # 更新資料庫
        await bot.loop.run_in_executor(None, lambda: monitor.update_products(new_products_data))
        
        # 如果是第一次執行，發送初始化訊息
        if is_first_run:
            embed = discord.Embed(title="🔍 吉伊卡哇商品監控初始化", 
                                description=f"初始化時間: {current_time}\n目前商品總數: {len(new_products)}", 
                                color=0x00ff00)
            embed.add_field(name="初始化完成", value="已完成商品資料庫的初始化，開始監控商品變化。", inline=False)
            await channel.send(embed=embed)
            logger.info("資料庫初始化完成")
            return
        
        # 發送例行監控通知
        embed = discord.Embed(title="🔍 吉伊卡哇商品監控", 
                            description=f"檢查時間: {current_time}\n目前商品總數: {len(new_products)}", 
                            color=0x00ff00)
        
        if new_listings:
            new_products_text = "\n".join([f"🆕 [{name}]({url})" for name, url in new_listings])
            if len(new_products_text) > 1024:
                new_products_text = new_products_text[:1021] + "..."
            embed.add_field(name="新上架商品", value=new_products_text, inline=False)
        else:
            embed.add_field(name="新上架商品", value="無", inline=False)
        
        if delisted:
            delisted_text = "\n".join([f"❌ [{name}]({url})" for name, url in delisted])
            if len(delisted_text) > 1024:
                delisted_text = delisted_text[:1021] + "..."
            embed.add_field(name="下架商品", value=delisted_text, inline=False)
        else:
            embed.add_field(name="下架商品", value="無", inline=False)
        
        # 發送例行通知
        await channel.send(embed=embed)
        
        # 如果有變化，在當前頻道發送通知
        if new_listings or delisted:
            alert_embed = discord.Embed(title="⚠️ 商品更新提醒", 
                                      description=f"檢查時間: {current_time}", 
                                      color=0xFF0000)
            
            if new_listings:
                new_products_text = "\n".join([f"🆕 [{name}]({url})" for name, url in new_listings])
                if len(new_products_text) > 1024:
                    new_products_text = new_products_text[:1021] + "..."
                alert_embed.add_field(name="新上架商品", value=new_products_text, inline=False)
            
            if delisted:
                delisted_text = "\n".join([f"❌ [{name}]({url})" for name, url in delisted])
                if len(delisted_text) > 1024:
                    delisted_text = delisted_text[:1021] + "..."
                alert_embed.add_field(name="下架商品", value=delisted_text, inline=False)
            
            # 在執行指令的頻道發送通知
            await channel.send(embed=alert_embed)
        
        logger.info(f"=== 檢查完成 ===\n")
            
    except Exception as e:
        error_msg = f"檢查更新時發生錯誤: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())

@bot.event
async def on_ready():
    logging.info(f'Bot logged in as {bot.user.name}')
    print(f'Bot logged in as {bot.user.name}')

# 在 bot.py 中添加權限檢查裝飾器
def has_role(role_id):
    async def predicate(ctx):
        # 檢查是否為管理員
        if ctx.author.guild_permissions.administrator:
            return True
        # 檢查是否有特定身分組
        return any(role.id == role_id for role in ctx.author.roles)
    return commands.check(predicate)

# 修改指令權限
ADMIN_ROLE_ID = 1353266568875737128 # 請替換為實際的身分組 ID

@bot.command(name='start')
@has_role(ADMIN_ROLE_ID)
async def start_monitoring(ctx):
    """執行一次商品更新檢查"""
    try:
        await ctx.send("開始檢查商品更新...")
        await check_updates(ctx)
        await ctx.send("檢查完成！")
    except Exception as e:
        await ctx.send(f"執行失敗：{str(e)}")
        logger.error(f"執行失敗：{str(e)}")

@bot.command(name='上架')
async def new_listings(ctx):
    """顯示今日上架的商品"""
    try:
        new_products = monitor.get_today_history('new')
        
        if not new_products:
            embed = discord.Embed(title="今日上架商品", description="今天還沒有新商品上架", color=0xff0000)
            await ctx.send(embed=embed)
            return
            
        # 限制商品数量，防止超出Discord嵌入消息大小限制
        max_products = 25  # 设置一个合理的上限
        if len(new_products) > max_products:
            # 如果商品太多，分批发送
            batches = [new_products[i:i+max_products] for i in range(0, len(new_products), max_products)]
            
            for i, batch in enumerate(batches):
                embed = discord.Embed(
                    title=f"今日上架商品 ({i+1}/{len(batches)})", 
                    description=f"共{len(new_products)}个商品上架", 
                    color=0x00ff00
                )
                
                for product in batch:
                    time_str = product['time'].strftime('%H:%M:%S')
                    # 限制字段内容长度
                    name = product['name']
                    if len(name) > 100:  # 限制标题长度
                        name = name[:97] + "..."
                    
                    field_content = f"🆕 上架時間: {time_str}\n[商品連結]({product['url']})"
                    embed.add_field(name=name, value=field_content, inline=False)
                
                await ctx.send(embed=embed)
            
            return
        
        # 如果商品数量不多，正常处理
        embed = discord.Embed(title="今日上架商品", color=0x00ff00)
        for product in new_products:
            time_str = product['time'].strftime('%H:%M:%S')
            
            # 限制字段内容长度
            name = product['name']
            if len(name) > 100:  # 限制标题长度
                name = name[:97] + "..."
            
            field_content = f"🆕 上架時間: {time_str}\n[商品連結]({product['url']})"
            embed.add_field(name=name, value=field_content, inline=False)
        
        await ctx.send(embed=embed)
            
    except Exception as e:
        await ctx.send(f"讀取上架記錄時發生錯誤：{str(e)}")
        logger.error(f"讀取上架記錄時發生錯誤：{str(e)}")
        logger.error(traceback.format_exc())

@bot.command(name='下架')
async def delisted(ctx):
    """顯示今日下架的商品"""
    try:
        delisted_products = monitor.get_today_history('delisted')
        
        if not delisted_products:
            embed = discord.Embed(title="今日下架商品", description="今天還沒有商品下架", color=0xff0000)
            await ctx.send(embed=embed)
            return
        
        # 限制商品数量，防止超出Discord嵌入消息大小限制
        max_products = 25  # 设置一个合理的上限
        if len(delisted_products) > max_products:
            # 如果商品太多，分批发送
            batches = [delisted_products[i:i+max_products] for i in range(0, len(delisted_products), max_products)]
            
            for i, batch in enumerate(batches):
                embed = discord.Embed(
                    title=f"今日下架商品 ({i+1}/{len(batches)})", 
                    description=f"共{len(delisted_products)}个商品下架", 
                    color=0xff0000
                )
                
                for product in batch:
                    time_str = product['time'].strftime('%H:%M:%S')
                    # 限制字段内容长度
                    name = product['name']
                    if len(name) > 100:  # 限制标题长度
                        name = name[:97] + "..."
                    
                    field_content = f"❌ 下架時間: {time_str}\n[商品連結]({product['url']})"
                    embed.add_field(name=name, value=field_content, inline=False)
                
                await ctx.send(embed=embed)
            
            return
            
        # 如果商品数量不多，正常处理
        embed = discord.Embed(title="今日下架商品", color=0xff0000)
        for product in delisted_products:
            time_str = product['time'].strftime('%H:%M:%S')
            
            # 限制字段内容长度
            name = product['name']
            if len(name) > 100:  # 限制标题长度
                name = name[:97] + "..."
                
            field_content = f"❌ 下架時間: {time_str}\n[商品連結]({product['url']})"
            embed.add_field(name=name, value=field_content, inline=False)
        
        await ctx.send(embed=embed)
            
    except Exception as e:
        await ctx.send(f"讀取下架記錄時發生錯誤：{str(e)}")
        logger.error(f"讀取下架記錄時發生錯誤：{str(e)}")
        logger.error(traceback.format_exc())

@bot.command(name='檢查')
@has_role(ADMIN_ROLE_ID)
async def check_product_count(ctx):
    """檢查商品總數"""
    try:
        await ctx.send("開始檢查商品總數...")
        
        # 獲取資料庫中的商品數量
        db_products = monitor.get_all_products()
        db_count = len(db_products)
        
        # 獲取網站上的商品數量（API方式）
        new_products = await bot.loop.run_in_executor(None, monitor.fetch_products)
        api_count = len(new_products)
        
        # 從網頁直接獲取商品數量
        web_count = None
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                    'Accept-Language': 'zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                }
                
                url = f"{monitor.base_url}/zh-hant/collections/all"
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        html = await response.text()
                        
                        # 嘗試從不同位置獲取商品數量
                        # 方法1：從商品計數器獲取
                        if '"collection-product-count"' in html:
                            start = html.find('"collection-product-count"')
                            if start != -1:
                                end = html.find('</span>', start)
                                if end != -1:
                                    count_text = html[start:end]
                                    import re
                                    if match := re.search(r'\d+', count_text):
                                        web_count = int(match.group())
                        
                        # 方法2：計算商品卡片數量
                        if web_count is None and 'product-card' in html:
                            web_count = html.count('product-card')
        except Exception as e:
            logger.error(f"從網站獲取商品數量失敗：{str(e)}")
        
        # 創建嵌入消息
        embed = discord.Embed(
            title="🔍 商品數量檢查",
            description="比較不同來源的商品數量",
            color=0x00ff00
        )
        
        # 添加各來源的商品數量
        embed.add_field(
            name="資料庫商品數量",
            value=f"📚 {db_count} 個商品",
            inline=True
        )
        
        embed.add_field(
            name="API 獲取數量",
            value=f"📡 {api_count} 個商品",
            inline=True
        )
        
        if web_count is not None:
            embed.add_field(
                name="網站顯示數量",
                value=f"🔖 {web_count} 個商品",
                inline=True
            )
        
        # 檢查差異
        differences = []
        if web_count is not None:
            if web_count != api_count:
                differences.append(f"• 網站與 API 差異：{abs(web_count - api_count)} 個商品")
            if web_count != db_count:
                differences.append(f"• 網站與資料庫差異：{abs(web_count - db_count)} 個商品")
        if api_count != db_count:
            differences.append(f"• API 與資料庫差異：{abs(api_count - db_count)} 個商品")
        
        if differences:
            embed.add_field(
                name="⚠️ 發現差異",
                value="\n".join(differences) + "\n建議執行 !start 更新資料",
                inline=False
            )
        else:
            embed.add_field(
                name="✅ 檢查結果",
                value="所有來源的商品數量一致",
                inline=False
            )
        
        # 添加商品列表連結
        embed.add_field(
            name="🔗 商品列表",
            value=f"[點擊查看網站商品列表]({monitor.base_url}/zh-hant/collections/all)",
            inline=False
        )
        
        await ctx.send(embed=embed)
        
    except Exception as e:
        await ctx.send(f"檢查失敗：{str(e)}")
        logger.error(f"檢查失敗：{str(e)}")
        logger.error(traceback.format_exc())

@bot.command(name='資料庫')
@has_role(ADMIN_ROLE_ID)
async def check_database(ctx):
    """檢查資料庫狀態"""
    try:
        await ctx.send("正在檢查資料庫狀態...")
        
        # 檢查 MongoDB 連接
        try:
            monitor.client.admin.command('ping')
            connection_status = "✅ 已連接"
        except Exception as e:
            connection_status = f"❌ 連接失敗: {str(e)}"
        
        # 獲取資料庫信息
        products_count = len(monitor.get_all_products())
        history_count = monitor.history.count_documents({})
        
        # 獲取最近的歷史記錄
        recent_history = list(monitor.history.find().sort('date', -1).limit(3))
        
        # 創建嵌入消息
        embed = discord.Embed(
            title="📊 MongoDB 資料庫狀態",
            description=f"MongoDB 連接狀態",
            color=0x00ff00
        )
        
        # 添加連接狀態
        embed.add_field(
            name="連接狀態",
            value=connection_status,
            inline=False
        )
        
        # 添加數據統計
        embed.add_field(
            name="商品數據",
            value=f"📦 {products_count} 個商品記錄",
            inline=True
        )
        
        embed.add_field(
            name="歷史記錄",
            value=f"📝 {history_count} 條歷史記錄",
            inline=True
        )
        
        # 添加最近的歷史記錄
        if recent_history:
            history_text = ""
            for record in recent_history:
                date = record['date'].strftime('%Y-%m-%d %H:%M:%S')
                type_text = "🆕 新增" if record['type'] == 'new' else "❌ 下架"
                history_text += f"{type_text} {record['name']} ({date})\n"
            
            embed.add_field(
                name="最近的記錄",
                value=history_text or "無記錄",
                inline=False
            )
        
        # 添加資料庫操作建議
        embed.add_field(
            name="💡 操作建議",
            value="• 使用 `!start` 更新商品資料\n• 使用 `!檢查` 驗證資料同步狀態\n• 使用 `!上架` 和 `!下架` 查看商品變化",
            inline=False
        )
        
        await ctx.send(embed=embed)
        
    except Exception as e:
        error_msg = f"檢查資料庫時發生錯誤：{str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        await ctx.send(error_msg)

@bot.command(name='狀態')
async def check_status(ctx):
    """檢查服務狀態"""
    try:
        # 檢查 MongoDB 連接
        try:
            monitor.client.admin.command('ping')
            mongodb_status = "✅ 正常"
        except Exception as e:
            mongodb_status = f"❌ 異常: {str(e)}"

        # 創建嵌入消息
        embed = discord.Embed(
            title="🔧 服務狀態檢查",
            description="檢查各項服務的運行狀態",
            color=0x00ff00
        )

        # Discord Bot 狀態
        embed.add_field(
            name="Discord Bot",
            value="✅ 正常運行中",
            inline=True
        )

        # MongoDB 狀態
        embed.add_field(
            name="MongoDB",
            value=mongodb_status,
            inline=True
        )

        # 運行時間信息
        current_time = datetime.now(TW_TIMEZONE)
        uptime = current_time - bot.start_time.astimezone(TW_TIMEZONE)
        embed.add_field(
            name="運行時間",
            value=f"⏱️ {uptime.days} 天 {uptime.seconds//3600} 小時 {(uptime.seconds//60)%60} 分鐘",
            inline=False
        )

        await ctx.send(embed=embed)

    except Exception as e:
        error_msg = f"檢查狀態時發生錯誤：{str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        await ctx.send(error_msg)

@bot.command(name='歷史')
async def history(ctx, days: int = 7):
    """顯示指定天數內的商品變更記錄"""
    try:
        if days <= 0 or days > 30:
            await ctx.send("請指定 1-30 天的範圍")
            return
            
        # 計算起始時間
        start_date = datetime.now(TW_TIMEZONE) - timedelta(days=days)
        
        # 獲取歷史記錄
        history_records = list(monitor.history.find({
            'date': {'$gte': start_date}
        }).sort('date', -1))
        
        if not history_records:
            embed = discord.Embed(
                title=f"近 {days} 天的商品變更記錄",
                description="這段期間沒有商品變更記錄",
                color=0xff0000
            )
            await ctx.send(embed=embed)
            return
            
        # 按日期分組
        records_by_date = {}
        for record in history_records:
            date_str = record['date'].strftime('%Y-%m-%d')
            if date_str not in records_by_date:
                records_by_date[date_str] = {'new': [], 'delisted': []}
            records_by_date[date_str][record['type']].append(record)
        
        # 統計信息
        total_new = sum(len(r['new']) for r in records_by_date.values())
        total_del = sum(len(r['delisted']) for r in records_by_date.values())
        
        # 拆分发送，每个嵌入消息最多包含5天的数据
        date_chunks = list(records_by_date.keys())
        max_days_per_embed = 5
        date_batches = [date_chunks[i:i+max_days_per_embed] for i in range(0, len(date_chunks), max_days_per_embed)]
        
        for i, date_batch in enumerate(date_batches):
            # 创建嵌入消息
            embed = discord.Embed(
                title=f"近 {days} 天的商品變更記錄 ({i+1}/{len(date_batches)})",
                description=f"從 {start_date.strftime('%Y-%m-%d')} 到現在",
                color=0x00ff00
            )
            
            # 添加每天的记录
            for date_str in date_batch:
                records = records_by_date[date_str]
                day_text = []
                
                if records['new']:
                    # 限制每天显示的项目数量
                    max_items_per_type = 20
                    new_items = records['new'][:max_items_per_type]
                    new_text = [f"🆕 {r['name']}" for r in new_items]
                    if len(records['new']) > max_items_per_type:
                        new_text.append(f"...还有 {len(records['new']) - max_items_per_type} 个商品")
                    day_text.extend(new_text)
                    
                if records['delisted']:
                    # 限制每天显示的项目数量
                    max_items_per_type = 20
                    del_items = records['delisted'][:max_items_per_type]
                    del_text = [f"❌ {r['name']}" for r in del_items]
                    if len(records['delisted']) > max_items_per_type:
                        del_text.append(f"...还有 {len(records['delisted']) - max_items_per_type} 个商品")
                    day_text.extend(del_text)
                
                if day_text:
                    field_text = "\n".join(day_text)
                    # 检查并截断字段值，Discord限制每个字段值最大为1024字节
                    if len(field_text) > 1024:
                        field_text = field_text[:1021] + "..."
                        
                    embed.add_field(
                        name=f"📅 {date_str}",
                        value=field_text,
                        inline=False
                    )
            
            # 在最后一个嵌入消息中添加统计信息
            if i == len(date_batches) - 1:
                embed.add_field(
                    name="📊 統計信息",
                    value=f"期間內共有：\n🆕 {total_new} 個商品上架\n❌ {total_del} 個商品下架",
                    inline=False
                )
            
            await ctx.send(embed=embed)
            
    except Exception as e:
        error_msg = f"讀取歷史記錄時發生錯誤：{str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())
        await ctx.send(error_msg)

@bot.command(name='commands', aliases=['command', '指令'])
async def show_commands(ctx):
    """顯示可用的指令列表"""
    # 檢查用戶是否為管理員或有特定身分組
    is_admin = ctx.author.guild_permissions.administrator or any(role.id == ADMIN_ROLE_ID for role in ctx.author.roles)
    
    embed = discord.Embed(
        title="吉伊卡哇官網監控 指令列表",
        description="以下是您可以使用的指令：",
        color=discord.Color.blue()
    )
    
    # 基本指令（所有人都可以看到）
    embed.add_field(
        name="基本指令",
        value=(
            "📦 `!上架` - 顯示今日上架的商品\n"
            "❌ `!下架` - 顯示今日下架的商品\n"
            "📅 `!歷史` - 顯示7天內的商品變更記錄\n"
            "🔧 `!狀態` - 檢查服務運行狀態\n"
            "❓ `!指令` - 顯示可用指令"
        ),
        inline=False
    )
    
    # 只有管理員/特定身分組才能看到的指令
    if is_admin:
        embed.add_field(
            name="管理員指令",
            value=(
                "🔄 `!start` - 執行一次商品更新檢查\n"
                "🔍 `!檢查` - 檢查商品數量\n"
                "💾 `!資料庫` - 檢查資料庫狀態"
            ),
            inline=False
        )
    
    await ctx.send(embed=embed)

# 錯誤處理
@bot.event
async def on_command_error(ctx, error):
    if isinstance(error, commands.CheckFailure):
        await ctx.send("❌ 您沒有權限使用此指令！")
    elif isinstance(error, commands.CommandNotFound):
        await ctx.send("❌ 無效的指令！請使用 `!指令` 查看可用的指令列表。")

async def healthcheck(request):
    """健康檢查端點"""
    try:
        # 檢查 MongoDB 連接
        monitor.client.admin.command('ping')
        mongodb_status = True
    except Exception as e:
        mongodb_status = False
        logger.error(f"健康檢查：MongoDB 連接失敗 - {str(e)}")

    status_data = {
        "status": "healthy" if mongodb_status else "degraded",
        "timestamp": datetime.now().isoformat(),
        "mongodb": mongodb_status,
        "bot": bot.is_ready()
    }

    # 記錄健康檢查請求
    logger.info(f"健康檢查請求：{status_data}")
    
    return web.json_response(status_data)

async def setup_webserver():
    app = web.Application()
    app.router.add_get('/', healthcheck)
    app.router.add_get('/health', healthcheck)  # 添加 /health 端點
    
    # 添加 LINE Bot Webhook 处理
    app.router.add_post('/line/webhook', handle_line_webhook)
    
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web 服務器已啟動，端口：{port}")
    logger.info("健康檢查端點已配置：/ 和 /health")
    logger.info("LINE Bot Webhook 端點已配置: /line/webhook")

async def handle_line_webhook(request):
    """处理 LINE Webhook 请求"""
    try:
        signature = request.headers.get('X-Line-Signature', '')
        body = await request.text()
        
        # 处理 webhook
        line_handler.handle(body, signature)
        
        return web.Response(text='OK')
    except InvalidSignatureError:
        logger.error("LINE Webhook 签名无效")
        return web.Response(status=400, text='Invalid signature')
    except Exception as e:
        logger.error(f"处理 LINE Webhook 时发生错误: {str(e)}")
        logger.error(traceback.format_exc())
        return web.Response(status=500, text='Internal Server Error')

@line_handler.add(MessageEvent, message=TextMessage)
def handle_line_message(event):
    """處理 LINE 訊息"""
    try:
        text = event.message.text.lower()
        logger.info(f"收到 LINE 訊息: {text}")
        
        # 定義支援的指令列表
        commands = ['上架', '下架', '狀態', '指令']
        
        # 檢查是否是歷史指令(特殊處理)
        is_history_command = False
        if text.startswith('歷史'):
            is_history_command = True
        
        # 檢查是否是支援的指令
        is_command = False
        for cmd in commands:
            if text == cmd:
                is_command = True
                break
        
        # 只處理支援的指令,忽略其他訊息
        if is_command or is_history_command:
            if text == '上架':
                handle_line_new_products(event.reply_token)
            elif text == '下架':
                handle_line_delisted_products(event.reply_token)
            elif text == '狀態':
                handle_line_status(event.reply_token)
            elif text == '指令':
                handle_line_help(event.reply_token)
            elif is_history_command:
                try:
                    days = int(text[2:]) if len(text) > 2 else 7
                    handle_line_history(event.reply_token, days)
                except ValueError:
                    line_bot_api.reply_message(
                        event.reply_token,
                        TextSendMessage(text="請指定 1-30 天的範圍")
                    )
        # 不處理非指令訊息
            
    except Exception as e:
        logger.error(f"處理 LINE 訊息時發生錯誤: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="處理請求時發生錯誤，請稍後再試。")
            )
        except:
            pass

def handle_line_new_products(reply_token):
    """處理 LINE 上架商品請求"""
    new_products = monitor.get_today_history('new')
    
    if not new_products:
        line_bot_api.reply_message(
            reply_token,
            TextSendMessage(text="今天還沒有新商品上架")
        )
        return
    
    # 计算总页数
    products_per_bubble = 6  # 每个气泡显示6个商品
    bubbles_per_carousel = 12  # LINE限制每个Carousel最多12个气泡
    products_per_carousel = products_per_bubble * bubbles_per_carousel  # 一个Carousel最多显示72个商品
    
    # 分批处理商品
    total_products = len(new_products)
    total_carousels = (total_products + products_per_carousel - 1) // products_per_carousel
    
    messages = []
    for carousel_index in range(total_carousels):
        start_idx = carousel_index * products_per_carousel
        end_idx = min(start_idx + products_per_carousel, total_products)
        current_batch = new_products[start_idx:end_idx]
        
        # 创建当前批次的Flex消息
        carousel = create_product_flex_message(
            f"今日上架商品 ({carousel_index + 1}/{total_carousels})", 
            current_batch, 
            "🆕"
        )
        messages.append(FlexSendMessage(
            alt_text=f"今日上架商品 ({carousel_index + 1}/{total_carousels})",
            contents=carousel
        ))
    
    # 如果只有一条消息，使用reply_message
    if len(messages) == 1:
        line_bot_api.reply_message(reply_token, messages[0])
    else:
        # 如果有多条消息，先回复第一条，然后推送其余消息
        line_bot_api.reply_message(reply_token, messages[0])
        # 获取用户ID以发送后续消息
        try:
            user_id = line_bot_api.get_profile(reply_token).user_id
            for message in messages[1:]:
                line_bot_api.push_message(user_id, message)
        except Exception as e:
            logger.error(f"發送後續消息時發生錯誤：{str(e)}")

def handle_line_delisted_products(reply_token):
    """處理 LINE 下架商品請求"""
    delisted_products = monitor.get_today_history('delisted')
    
    if not delisted_products:
        line_bot_api.reply_message(
            reply_token,
            TextSendMessage(text="今天還沒有商品下架")
        )
        return
    
    # 计算总页数
    products_per_bubble = 6  # 每个气泡显示6个商品
    bubbles_per_carousel = 12  # LINE限制每个Carousel最多12个气泡
    products_per_carousel = products_per_bubble * bubbles_per_carousel  # 一个Carousel最多显示72个商品
    
    # 分批处理商品
    total_products = len(delisted_products)
    total_carousels = (total_products + products_per_carousel - 1) // products_per_carousel
    
    messages = []
    for carousel_index in range(total_carousels):
        start_idx = carousel_index * products_per_carousel
        end_idx = min(start_idx + products_per_carousel, total_products)
        current_batch = delisted_products[start_idx:end_idx]
        
        # 创建当前批次的Flex消息
        carousel = create_product_flex_message(
            f"今日下架商品",  # 移除批次编号，只在底部显示页码
            current_batch, 
            "❌"
        )
        messages.append(FlexSendMessage(
            alt_text=f"今日下架商品 ({carousel_index + 1}/{total_carousels})",
            contents=carousel
        ))
    
    # 如果只有一条消息，使用reply_message
    if len(messages) == 1:
        line_bot_api.reply_message(reply_token, messages[0])
    else:
        # 如果有多条消息，先回复第一条，然后推送其余消息
        line_bot_api.reply_message(reply_token, messages[0])
        # 获取用户ID以发送后续消息
        try:
            user_id = line_bot_api.get_profile(reply_token).user_id
            for message in messages[1:]:
                line_bot_api.push_message(user_id, message)
        except Exception as e:
            logger.error(f"發送後續消息時發生錯誤：{str(e)}")

def handle_line_status(reply_token):
    """處理 LINE 狀態請求"""
    try:
        # 檢查 MongoDB 連接
        monitor.client.admin.command('ping')
        mongodb_status = "✅ 正常"
    except Exception as e:
        mongodb_status = f"❌ 異常: {str(e)}"

    # 創建 Flex 消息
    bubble = BubbleContainer(
        body=BoxComponent(
            layout="vertical",
            contents=[
                TextComponent(text="🔧 服務狀態", weight="bold", size="xl"),
                TextComponent(text=f"MongoDB: {mongodb_status}", margin="md"),
                TextComponent(text="LINE Bot: ✅ 正常運行中", margin="md"),
                TextComponent(text="Discord Bot: ✅ 正常運行中", margin="md")
            ]
        )
    )
    
    line_bot_api.reply_message(
        reply_token,
        FlexSendMessage(alt_text="服務狀態", contents=bubble)
    )

def handle_line_history(reply_token, days):
    """處理 LINE 歷史記錄請求"""
    if days <= 0 or days > 30:
        line_bot_api.reply_message(
            reply_token,
            TextSendMessage(text="請指定 1-30 天的範圍")
        )
        return
    
    # 計算起始時間
    start_date = datetime.now(TW_TIMEZONE) - timedelta(days=days)
    
    # 獲取歷史記錄
    history_records = list(monitor.history.find({
        'date': {'$gte': start_date}
    }).sort('date', -1))
    
    if not history_records:
        line_bot_api.reply_message(
            reply_token,
            TextSendMessage(text=f"近 {days} 天沒有商品變更記錄")
        )
        return
    
    # 按日期分組
    records_by_date = {}
    for record in history_records:
        date_str = record['date'].strftime('%Y-%m-%d')
        if date_str not in records_by_date:
            records_by_date[date_str] = []
        records_by_date[date_str].append(record)
    
    # 创建气泡列表
    bubbles = []
    dates = list(records_by_date.keys())
    
    # 每个气泡显示3天的记录
    days_per_bubble = 3
    total_bubbles = (len(dates) + days_per_bubble - 1) // days_per_bubble
    
    for bubble_index in range(total_bubbles):
        start_idx = bubble_index * days_per_bubble
        end_idx = min(start_idx + days_per_bubble, len(dates))
        current_dates = dates[start_idx:end_idx]
        
        # 创建气泡内容
        contents = [
            TextComponent(
                text=f"近 {days} 天的變更記錄 ({bubble_index + 1}/{total_bubbles})", 
                weight="bold", 
                size="xl"
            )
        ]
        
        # 添加每天的记录
        for date_str in current_dates:
            records = records_by_date[date_str]
            
            # 计算每天的记录统计
            new_count = sum(1 for r in records if r['type'] == 'new')
            del_count = sum(1 for r in records if r['type'] == 'delisted')
            
            day_contents = [
                TextComponent(text=f"📅 {date_str}", weight="bold", margin="md"),
                TextComponent(text=f"上架: {new_count}件 | 下架: {del_count}件", size="sm", color="#999999")
            ]
            
            # 添加商品记录
            for record in records:
                icon = "🆕" if record['type'] == 'new' else "❌"
                time_str = record['time'].strftime('%H:%M')
                
                # 名称可能太长，进行截断
                name = record['name']
                if len(name) > 20:
                    name = name[:17] + "..."
                    
                day_contents.append(
                    TextComponent(
                        text=f"{icon} {name} ({time_str})",
                        size="sm",
                        wrap=True,
                        action=URIAction(uri=record['url'])
                    )
                )
            
            contents.append(
                BoxComponent(
                    layout="vertical",
                    margin="md",
                    contents=day_contents
                )
            )
        
        # 添加页码信息
        contents.append(
            TextComponent(
                text=f"第 {bubble_index + 1} 頁，共 {total_bubbles} 頁",
                size="sm",
                color="#999999",
                align="center",
                margin="md"
            )
        )
        
        # 创建气泡
        bubble = BubbleContainer(
            body=BoxComponent(
                layout="vertical",
                contents=contents
            )
        )
        bubbles.append(bubble)
    
    # 创建轮播容器
    carousel = CarouselContainer(contents=bubbles)
    
    line_bot_api.reply_message(
        reply_token,
        FlexSendMessage(alt_text=f"近 {days} 天的變更記錄", contents=carousel)
    )

def handle_line_help(reply_token):
    """發送 LINE 幫助信息"""
    help_text = (
        "可用指令：\n"
        "📦 上架 - 顯示今日新上架商品\n"
        "❌ 下架 - 顯示今日下架商品\n"
        "🔧 狀態 - 檢查服務運行狀態\n"
        "📅 歷史 - 顯示7天內的變更記錄\n"
        "❓ 指令 - 顯示可用指令"
    )
    
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(text=help_text)
    )

def create_product_flex_message(title, products, icon="🆕"):
    """創建商品 Flex 消息，使用 Carousel 實現分頁"""
    # 每个气泡最多显示6个商品
    products_per_bubble = 6
    bubbles = []
    
    # 计算需要多少个气泡
    total_products = len(products)
    total_bubbles = (total_products + products_per_bubble - 1) // products_per_bubble
    
    for bubble_index in range(total_bubbles):
        start_idx = bubble_index * products_per_bubble
        end_idx = min(start_idx + products_per_bubble, total_products)
        current_products = products[start_idx:end_idx]
        
        # 创建每个气泡的内容
        contents = [
            TextComponent(
                text=f"{icon} {title}",
                weight="bold",
                size="xl"
            )
        ]
        
        for product in current_products:
            time_str = product['time'].strftime('%H:%M:%S')
            
            # 截断可能过长的商品名称
            name = product['name']
            if len(name) > 30:
                name = name[:27] + "..."
            
            # 创建商品容器
            product_box = BoxComponent(
                layout="vertical",
                margin="md",
                spacing="sm",
                contents=[
                    # 商品信息行
                    BoxComponent(
                        layout="horizontal",
                        spacing="sm",
                        contents=[
                            # 图片容器（固定宽度，无margin）
                            BoxComponent(
                                layout="vertical",
                                width="40px",
                                height="40px",
                                flex=0,
                                contents=[
                                    ImageComponent(
                                        url=product.get('image_url', 'https://cdn.shopify.com/s/files/1/0626/7142/1681/files/4905114930365_1.jpg?v=1744782283'),
                                        size="full",
                                        aspect_mode="cover",
                                        aspect_ratio="1:1"
                                    )
                                ]
                            ),
                            # 商品信息
                            BoxComponent(
                                layout="vertical",
                                flex=1,
                                spacing="xs",
                                contents=[
                                    TextComponent(text=name, weight="bold", wrap=True, size="sm"),
                                    TextComponent(text=f"時間: {time_str}", size="xs", color="#999999"),
                                    ButtonComponent(
                                        style="link",
                                        height="sm",
                                        action=URIAction(label="查看商品", uri=product['url'])
                                    )
                                ]
                            )
                        ]
                    )
                ]
            )
            
            # 添加商品容器
            contents.append(product_box)
        
        # 添加页码信息
        contents.append(
            TextComponent(
                text=f"第 {bubble_index + 1} 頁，共 {total_bubbles} 頁",
                size="sm",
                color="#999999",
                align="center",
                margin="md"
            )
        )
        
        # 创建气泡容器
        bubble = BubbleContainer(
            body=BoxComponent(
                layout="vertical",
                contents=contents
            )
        )
        bubbles.append(bubble)
    
    # 如果只有一个气泡，直接返回气泡
    if len(bubbles) == 1:
        return bubbles[0]
    
    # 否则返回轮播容器
    return CarouselContainer(contents=bubbles)

# 運行 Bot
if __name__ == "__main__":
    try:
        # 檢查是否已有實例在運行
        if check_running():
            logger.error("另一個 Bot 實例已在運行，退出程序")
            sys.exit(1)
            
        # 創建進程鎖
        create_lock()
        
        # 運行 Bot
        bot.run(TOKEN)
    except Exception as e:
        logger.error(f"Bot crashed: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # 確保在任何情況下都移除進程鎖
        remove_lock() 
