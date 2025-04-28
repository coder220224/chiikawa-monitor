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
    ImageComponent, ImageCarouselTemplate, ImageCarouselColumn, TemplateSendMessage,
    RichMenu, RichMenuArea, RichMenuBounds, RichMenuSize, PostbackAction, PostbackEvent
)
import time
import requests

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

# 從環境變數獲取 LINE Bot 配置
LINE_CHANNEL_ACCESS_TOKEN = os.environ.get('LINE_CHANNEL_ACCESS_TOKEN', '')
LINE_CHANNEL_SECRET = os.environ.get('LINE_CHANNEL_SECRET', '')

# 進程鎖文件路徑
LOCK_FILE = os.path.join(WORK_DIR, 'bot.lock')

# Rich Menu 配置
RICH_MENU_SIZE = RichMenuSize(width=2500, height=1686)
RICH_MENU_IMAGES = {
    'page1': 'https://raw.githubusercontent.com/coder220224/chiikawa-monitor/main/image/chiikawa.png',
    'page2': 'https://raw.githubusercontent.com/coder220224/chiikawa-monitor/main/image/chiikawa.png'
}

# 全局变量用于存储Rich Menu IDs
RICH_MENU_IDS = {}

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
            start_time = time.time()
            old_products = {p['url']: p for p in monitor.get_all_products()}
            logger.info(f"成功獲取現有商品數據：{len(old_products)} 個，耗時：{time.time() - start_time:.2f}秒")
        except Exception as e:
            error_msg = f"獲取現有商品數據失敗：{str(e)}"
            logger.error(error_msg)
            logger.error(traceback.format_exc())
            await channel.send(f"錯誤：{error_msg}")
            return
        
        # 獲取新的商品資料
        try:
            logger.info("開始獲取新商品數據...")
            start_time = time.time()
            new_products_data = await bot.loop.run_in_executor(None, monitor.fetch_products)
            logger.info(f"獲取新商品數據完成，耗時：{time.time() - start_time:.2f}秒")
            
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
        
        # 比對差異（使用集合操作提高效率）
        start_time = time.time()
        new_urls = set(new_products.keys())
        old_urls = set(old_products.keys())
        
        # 找出新上架的URL
        new_listing_urls = new_urls - old_urls if not is_first_run else set()
        # 找出下架的URL
        delisted_urls = old_urls - new_urls if not is_first_run else set()
        
        new_listings = [(new_products[url]['name'], url) for url in new_listing_urls]
        missing_products = [(old_products[url]['name'], url) for url in delisted_urls]
        
        logger.info(f"差異比對完成，耗時：{time.time() - start_time:.2f}秒")
        logger.info(f"發現 {len(new_listings)} 個新上架商品，{len(missing_products)} 個可能下架商品")
        
        # 批量檢查下架商品
        delisted = []
        if not is_first_run and missing_products:
            start_time = time.time()
            logger.info(f"開始檢查 {len(missing_products)} 個可能下架的商品...")
            
            # 批量檢查，每批20個
            batch_size = 20
            for i in range(0, len(missing_products), batch_size):
                batch = missing_products[i:i + batch_size]
                batch_results = await asyncio.gather(
                    *[bot.loop.run_in_executor(None, lambda u=url: monitor.check_product_url(u)) 
                      for name, url in batch]
                )
                
                # 處理批次結果
                for (name, url), is_available in zip(batch, batch_results):
                    if not is_available:
                        delisted.append((name, url))
                        await bot.loop.run_in_executor(
                            None, 
                            lambda n=name, u=url: monitor.record_history({'name': n, 'url': u}, 'delisted')
                        )
                
                logger.info(f"已檢查 {min(i + batch_size, len(missing_products))} / {len(missing_products)} 個商品")
            
            logger.info(f"下架商品檢查完成，確認 {len(delisted)} 個商品下架，耗時：{time.time() - start_time:.2f}秒")
        
        # 批量記錄新上架商品
        if new_listings:
            start_time = time.time()
            logger.info(f"開始記錄 {len(new_listings)} 個新上架商品...")
            
            # 批量處理，每批50個
            batch_size = 50
            for i in range(0, len(new_listings), batch_size):
                batch = new_listings[i:i + batch_size]
                await asyncio.gather(
                    *[bot.loop.run_in_executor(
                        None,
                        lambda p=new_products[url]: monitor.record_history(p, 'new')
                    ) for name, url in batch]
                )
                
                logger.info(f"已記錄 {min(i + batch_size, len(new_listings))} / {len(new_listings)} 個新商品")
            
            logger.info(f"新商品記錄完成，耗時：{time.time() - start_time:.2f}秒")
        
        # 更新資料庫
        start_time = time.time()
        await bot.loop.run_in_executor(None, lambda: monitor.update_products(new_products_data))
        logger.info(f"資料庫更新完成，耗時：{time.time() - start_time:.2f}秒")
        
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
        await channel.send(f"錯誤：{error_msg}")

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
async def new_listings(ctx, days: int = 0):
    """顯示上架的商品，可指定天數"""
    try:
        if days < 0 or days > 7:
            await ctx.send("請指定 0-7 天的範圍（0 表示今天）")
            return
            
        # 根据天数参数选择不同的函数获取数据
        if days == 0:
            # 使用新的函数获取今日数据
            new_products = monitor.get_today_new_products()
            title = "今日上架商品"
        else:
            # 使用新的函数获取指定天数的数据
            new_products = monitor.get_period_new_products(days)
            title = f"近 {days} 天上架商品"
        
        if not new_products:
            embed = discord.Embed(title=title, description=f"指定時間內沒有新商品上架", color=0xff0000)
            await ctx.send(embed=embed)
            return
            
        # 商品數量，不設限制
        total_products = len(new_products)
        
        # 計算需要分批發送的數量
        # Discord 嵌入消息限制：每個消息最多 25 個字段，每個字段最大 1024 字符
        max_fields_per_embed = 25
        batch_count = (total_products + max_fields_per_embed - 1) // max_fields_per_embed
        
        # 分批發送
        for i in range(batch_count):
            start_idx = i * max_fields_per_embed
            end_idx = min(start_idx + max_fields_per_embed, total_products)
            batch = new_products[start_idx:end_idx]
            
            embed = discord.Embed(
                title=f"{title} ({i+1}/{batch_count})",
                description=f"共 {total_products} 個商品上架",
                color=0x00ff00
            )
            
            for product in batch:
                time_str = product['time'].strftime('%Y-%m-%d %H:%M:%S')
                
                # 限制字段内容长度
                name = product['name']
                if len(name) > 100:  # 限制标题长度
                    name = name[:97] + "..."
                
                # 處理標籤信息
                tags_text = ""
                if 'tags' in product and product['tags']:
                    tags = product['tags']
                    tags_text = f"\n🏷️ {', '.join(tags[:10])}"
                    if len(product['tags']) > 10:
                        tags_text += f" ... 等{len(product['tags'])}個標籤"
                
                # 添加價格信息（如果有）
                price_text = ""
                if 'price' in product and product['price']:
                    price = product['price']
                    price_text = f"\n💰 價格: ¥{price:,}"
                
                availability = "✅ 有貨" if product.get('available', False) else "❌ 缺貨"
                
                field_content = f"🆕 上架時間: {time_str}\n{availability}{price_text}\n[商品連結]({product['url']}){tags_text}"
                
                # 確保字段內容不超過 Discord 限制
                if len(field_content) > 1024:
                    field_content = field_content[:1021] + "..."
                    
                embed.add_field(name=name, value=field_content, inline=False)
            
            await ctx.send(embed=embed)
            
    except Exception as e:
        await ctx.send(f"讀取上架記錄時發生錯誤：{str(e)}")
        logger.error(f"讀取上架記錄時發生錯誤：{str(e)}")
        logger.error(traceback.format_exc())

@bot.command(name='下架')
async def delisted(ctx, days: int = 0):
    """顯示下架的商品，可指定天數"""
    try:
        if days < 0 or days > 7:
            await ctx.send("請指定 0-7 天的範圍（0 表示今天）")
            return
            
        # 根据天数参数选择不同的函数获取数据
        if days == 0:
            # 使用新的函数获取今日数据
            delisted_products = monitor.get_today_delisted_products()
            title = "今日下架商品"
        else:
            # 使用新的函数获取指定天数的数据
            delisted_products = monitor.get_period_delisted_products(days)
            title = f"近 {days} 天下架商品"
        
        if not delisted_products:
            embed = discord.Embed(title=title, description=f"指定時間內沒有商品下架", color=0xff0000)
            await ctx.send(embed=embed)
            return
        
        # 商品數量，不設限制
        total_products = len(delisted_products)
        
        # 計算需要分批發送的數量
        # Discord 嵌入消息限制：每個消息最多 25 個字段，每個字段最大 1024 字符
        max_fields_per_embed = 25
        batch_count = (total_products + max_fields_per_embed - 1) // max_fields_per_embed
        
        # 分批發送
        for i in range(batch_count):
            start_idx = i * max_fields_per_embed
            end_idx = min(start_idx + max_fields_per_embed, total_products)
            batch = delisted_products[start_idx:end_idx]
            
            embed = discord.Embed(
                title=f"{title} ({i+1}/{batch_count})",
                description=f"共 {total_products} 個商品下架",
                color=0xff0000
            )
            
            for product in batch:
                time_str = product['time'].strftime('%Y-%m-%d %H:%M:%S')
                
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
        
        # 拆分發送，每個嵌入消息最多包含5天的數據
        date_chunks = list(records_by_date.keys())
        max_days_per_embed = 5
        date_batches = [date_chunks[i:i+max_days_per_embed] for i in range(0, len(date_chunks), max_days_per_embed)]
        
        for i, date_batch in enumerate(date_batches):
            # 創建嵌入消息
            embed = discord.Embed(
                title=f"近 {days} 天的商品變更記錄 ({i+1}/{len(date_batches)})",
                description=f"從 {start_date.strftime('%Y-%m-%d')} 到現在",
                color=0x00ff00
            )
            
            # 添加每天的記錄
            for date_str in date_batch:
                records = records_by_date[date_str]
                day_text = []
                
                if records['new']:
                    # 限制每天顯示的項目數量
                    max_items_per_type = 20
                    new_items = records['new'][:max_items_per_type]
                    new_text = [f"🆕 {r['name']}" for r in new_items]
                    if len(records['new']) > max_items_per_type:
                        new_text.append(f"...還有 {len(records['new']) - max_items_per_type} 個商品")
                    day_text.extend(new_text)
                    
                if records['delisted']:
                    # 限制每天顯示的項目數量
                    max_items_per_type = 20
                    del_items = records['delisted'][:max_items_per_type]
                    del_text = [f"❌ {r['name']}" for r in del_items]
                    if len(records['delisted']) > max_items_per_type:
                        del_text.append(f"...還有 {len(records['delisted']) - max_items_per_type} 個商品")
                    day_text.extend(del_text)
                
                if day_text:
                    field_text = "\n".join(day_text)
                    # 檢查並截斷字段值，Discord限制每個字段值最大為1024字節
                    if len(field_text) > 1024:
                        field_text = field_text[:1021] + "..."
                        
                    embed.add_field(
                        name=f"📅 {date_str}",
                        value=field_text,
                        inline=False
                    )
            
            # 在最後一個嵌入消息中添加統計信息
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
            "📦 `!上架 [天數]` - 顯示上架的商品，可指定 0-7 天範圍（0表示今天）\n"
            "❌ `!下架 [天數]` - 顯示下架的商品，可指定 0-7 天範圍（0表示今天）\n"
            "🔄 `!補貨` - 查看即將補貨的商品\n"
            "📅 `!歷史 [天數]` - 顯示指定天數內的商品變更記錄（默認7天）\n"
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
    
    # 添加 LINE Bot Webhook 處理
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
    """處理 LINE Webhook 請求"""
    try:
        signature = request.headers.get('X-Line-Signature', '')
        body = await request.text()
        
        # 處理 webhook
        line_handler.handle(body, signature)
        
        return web.Response(text='OK')
    except InvalidSignatureError:
        logger.error("LINE Webhook 簽名無效")
        return web.Response(status=400, text='Invalid signature')
    except Exception as e:
        logger.error(f"處理 LINE Webhook 時發生錯誤: {str(e)}")
        logger.error(traceback.format_exc())
        return web.Response(status=500, text='Internal Server Error')

@line_handler.add(MessageEvent, message=TextMessage)
def handle_line_message(event):
    """處理 LINE 訊息"""
    try:
        text = event.message.text.lower()
        logger.info(f"收到 LINE 訊息: {text}")
        
        # 定義支援的指令列表
        commands = ['狀態', '指令']
        
        # 檢查是否是歷史指令(特殊處理)
        is_history_command = False
        days_history = 7  # 默認7天
        if text.startswith('歷史'):
            is_history_command = True
            parts = text.split()
            if len(parts) > 1:
                try:
                    days_history = int(parts[1])
                    if days_history <= 0 or days_history > 30:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text="請指定 1-30 天的範圍")
                        )
                        return
                except ValueError:
                    pass
        
        # 檢查是否是上架指令(特殊處理)
        is_new_command = False
        days_new = 0  # 默認今天
        if text.startswith('上架'):
            is_new_command = True
            parts = text.split()
            if len(parts) > 1:
                try:
                    days_new = int(parts[1])
                    if days_new < 0 or days_new > 7:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text="請指定 0-7 天的範圍（0表示今天）")
                        )
                        return
                except ValueError:
                    pass
        
        # 檢查是否是下架指令(特殊處理)
        is_delisted_command = False
        days_delisted = 0  # 默認今天
        if text.startswith('下架'):
            is_delisted_command = True
            parts = text.split()
            if len(parts) > 1:
                try:
                    days_delisted = int(parts[1])
                    if days_delisted < 0 or days_delisted > 7:
                        line_bot_api.reply_message(
                            event.reply_token,
                            TextSendMessage(text="請指定 0-7 天的範圍（0表示今天）")
                        )
                        return
                except ValueError:
                    pass
        
        # 檢查是否是補貨指令
        is_restock_command = False
        if text == '補貨' or text == '預購' or text == '重新上架':
            is_restock_command = True
        
        # 檢查是否是支援的指令
        is_command = False
        for cmd in commands:
            if text == cmd:
                is_command = True
                break
        
        # 只處理支援的指令,忽略其他訊息
        if is_command or is_history_command or is_new_command or is_delisted_command or is_restock_command:
            if is_new_command:
                handle_line_new_products(event, days_new)
            elif is_delisted_command:
                handle_line_delisted_products(event, days_delisted)
            elif is_restock_command:
                handle_line_restock(event)  # 傳遞完整event對象
            elif text == '狀態':
                handle_line_status(event.reply_token)
            elif text == '指令':
                handle_line_help(event.reply_token)
            elif is_history_command:
                handle_line_history(event, days_history)  # 傳遞完整event對象和天數
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

def handle_line_new_products(event, days):
    """處理 LINE 上架商品請求 (使用Image Carousel)"""
    try:
        if days == 0:
            new_products = monitor.get_today_new_products()
            title = "今日上架商品"
        else:
            new_products = monitor.get_period_new_products(days)
            title = f"近 {days} 天上架商品"
    
        if not new_products:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="指定天數內沒有新商品上架")
            )
            return
    
        # 按日期分組
        products_by_date = {}
        for product in new_products:
            date_str = product['time'].strftime('%Y-%m-%d')
            if date_str not in products_by_date:
                products_by_date[date_str] = []
            products_by_date[date_str].append(product)
        
        # 按日期排序（最新的在前）
        sorted_dates = sorted(products_by_date.keys(), reverse=True)
        
        # 準備要發送的消息列表
        messages = []
        
        # 處理每個日期的商品
        for date_str in sorted_dates:
            products = products_by_date[date_str]
            total_count = len(products)
            
            # 發送日期標題 (每個日期只發一次)
            date_title = f"{date_str} 上架商品 (共{total_count}件)"
            messages.append(TextSendMessage(text=date_title))
            
            # 每10個商品一組，使用Image Carousel顯示
            items_per_carousel = 10
            carousel_count = (total_count + items_per_carousel - 1) // items_per_carousel
            
            for i in range(carousel_count):
                start_idx = i * items_per_carousel
                end_idx = min(start_idx + items_per_carousel, total_count)
                batch_products = products[start_idx:end_idx]
                
                # 創建Image Carousel
                carousel = create_image_carousel(batch_products)
                if carousel:
                    messages.append(carousel)
        
        # 根據消息數量決定如何發送
        if len(messages) == 1:
            # 只有一條消息，直接回覆
            line_bot_api.reply_message(event.reply_token, messages[0])
        else:
            # 有多條消息，回覆第一條並推送後續消息
            line_bot_api.reply_message(event.reply_token, messages[0])
            
            # 獲取用戶ID並推送剩餘消息
            user_id = event.source.user_id
            for msg in messages[1:]:
                line_bot_api.push_message(user_id, msg)
                # 避免太快發送觸發限制
                time.sleep(0.5)
            
    except Exception as e:
        logger.error(f"處理上架商品請求時發生錯誤: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="獲取上架商品時發生錯誤，請稍後再試。")
            )
        except:
            pass

def handle_line_delisted_products(event, days):
    """處理 LINE 下架商品請求 (使用Image Carousel)"""
    try:
        if days == 0:
            delisted_products = monitor.get_today_delisted_products()
            title = "今日下架商品"
        else:
            delisted_products = monitor.get_period_delisted_products(days)
            title = f"近 {days} 天下架商品"
    
        if not delisted_products:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="指定天數內沒有商品下架")
            )
            return
    
        # 按日期分組
        products_by_date = {}
        for product in delisted_products:
            date_str = product['time'].strftime('%Y-%m-%d')
            if date_str not in products_by_date:
                products_by_date[date_str] = []
            products_by_date[date_str].append(product)
        
        # 按日期排序（最新的在前）
        sorted_dates = sorted(products_by_date.keys(), reverse=True)
        
        # 準備要發送的消息列表
        messages = []
        
        # 處理每個日期的商品
        for date_str in sorted_dates:
            products = products_by_date[date_str]
            total_count = len(products)
            
            # 發送日期標題 (每個日期只發一次)
            date_title = f"{date_str} 下架商品 (共{total_count}件)"
            messages.append(TextSendMessage(text=date_title))
            
            # 每10個商品一組，使用Image Carousel顯示
            items_per_carousel = 10
            carousel_count = (total_count + items_per_carousel - 1) // items_per_carousel
            
            for i in range(carousel_count):
                start_idx = i * items_per_carousel
                end_idx = min(start_idx + items_per_carousel, total_count)
                batch_products = products[start_idx:end_idx]
                
                # 創建Image Carousel
                carousel = create_image_carousel(batch_products)
                if carousel:
                    messages.append(carousel)
        
        # 根據消息數量決定如何發送
        if len(messages) == 1:
            # 只有一條消息，直接回覆
            line_bot_api.reply_message(event.reply_token, messages[0])
        else:
            # 有多條消息，回覆第一條並推送後續消息
            line_bot_api.reply_message(event.reply_token, messages[0])
            
            # 獲取用戶ID並推送剩餘消息
            user_id = event.source.user_id
            for msg in messages[1:]:
                line_bot_api.push_message(user_id, msg)
                # 避免太快發送觸發限制
                time.sleep(0.5)
            
    except Exception as e:
        logger.error(f"處理下架商品請求時發生錯誤: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="獲取下架商品時發生錯誤，請稍後再試。")
            )
        except:
            pass

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

def handle_line_history(event, days):
    """處理 LINE 歷史記錄請求 (使用Image Carousel)"""
    if days <= 0 or days > 30:
        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text="請指定 1-30 天的範圍")
        )
        return
    
    try:
        # 計算起始時間
        start_date = datetime.now(TW_TIMEZONE) - timedelta(days=days)
        
        # 獲取歷史記錄
        history_records = list(monitor.history.find({
            'date': {'$gte': start_date}
        }).sort('date', -1))
        
        if not history_records:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text=f"近 {days} 天沒有商品變更記錄")
            )
            return
        
        # 按日期分組
        records_by_date = {}
        for record in history_records:
            date_str = record['date'].strftime('%Y-%m-%d')
            if date_str not in records_by_date:
                records_by_date[date_str] = {'new': [], 'delisted': []}
            records_by_date[date_str][record['type']].append(record)
        
        # 按日期排序（最新的在前）
        sorted_dates = sorted(records_by_date.keys(), reverse=True)
        
        # 準備要發送的消息列表
        messages = []
        
        # 處理每個日期的記錄
        for date_str in sorted_dates:
            records = records_by_date[date_str]
            
            # 統計每種類型的商品數量
            new_count = len(records['new'])
            del_count = len(records['delisted'])
            
            # 發送日期標題
            date_title = f"{date_str} 商品變更記錄 (上架: {new_count}件 | 下架: {del_count}件)"
            messages.append(TextSendMessage(text=date_title))
            
            # 處理上架商品 (如果有的話)
            if new_count > 0:
                new_products = records['new']
                
                # 每10個商品一組，使用Image Carousel顯示
                items_per_carousel = 10
                carousel_count = (new_count + items_per_carousel - 1) // items_per_carousel
                
                # 如果需要發送多個Image Carousel，先發送一個小標題
                if carousel_count > 0:
                    messages.append(TextSendMessage(text=f"🆕 上架商品 ({new_count}件)"))
                
                for i in range(carousel_count):
                    start_idx = i * items_per_carousel
                    end_idx = min(start_idx + items_per_carousel, new_count)
                    batch_products = new_products[start_idx:end_idx]
                    
                    # 創建Image Carousel
                    carousel = create_image_carousel(batch_products)
                    if carousel:
                        messages.append(carousel)
            
            # 處理下架商品 (如果有的話)
            if del_count > 0:
                del_products = records['delisted']
                
                # 每10個商品一組，使用Image Carousel顯示
                items_per_carousel = 10
                carousel_count = (del_count + items_per_carousel - 1) // items_per_carousel
                
                # 如果需要發送多個Image Carousel，先發送一個小標題
                if carousel_count > 0:
                    messages.append(TextSendMessage(text=f"❌ 下架商品 ({del_count}件)"))
                
                for i in range(carousel_count):
                    start_idx = i * items_per_carousel
                    end_idx = min(start_idx + items_per_carousel, del_count)
                    batch_products = del_products[start_idx:end_idx]
                    
                    # 創建Image Carousel
                    carousel = create_image_carousel(batch_products)
                    if carousel:
                        messages.append(carousel)
        
        # 根據消息數量決定如何發送
        if len(messages) == 1:
            # 只有一條消息，直接回覆
            line_bot_api.reply_message(event.reply_token, messages[0])
        else:
            # 有多條消息，回覆第一條並推送後續消息
            line_bot_api.reply_message(event.reply_token, messages[0])
            
            # 獲取用戶ID並推送剩餘消息
            user_id = event.source.user_id
            for msg in messages[1:]:
                line_bot_api.push_message(user_id, msg)
                # 避免太快發送觸發限制
                time.sleep(0.5)
            
    except Exception as e:
        logger.error(f"處理歷史記錄請求時發生錯誤: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="獲取歷史記錄時發生錯誤，請稍後再試。")
            )
        except:
            pass

def create_image_carousel(products):
    """創建Image Carousel消息"""
    # 確保不超過10個項目(LINE的限制)
    if len(products) > 10:
        products = products[:10]
    
    # 如果沒有商品，返回None
    if not products:
        return None
    
    columns = []
    for product in products:
        # 處理標籤文字，確保不超過Label的12字符限制
        name = product['name']
        if len(name) > 12:
            label = name[:11] + "…"
        else:
            label = name
        
        # 獲取圖片URL，如果沒有則使用默認圖片
        image_url = product.get('image_url', 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png')
        
        # 創建列
        column = ImageCarouselColumn(
            image_url=image_url,
            action=URIAction(
                label=label,
                uri=product['url']
            )
        )
        columns.append(column)
    
    # 創建圖片輪播
    carousel_template = ImageCarouselTemplate(columns=columns)
    message = TemplateSendMessage(
        alt_text="商品列表",
        template=carousel_template
    )
    
    return message

def handle_line_help(reply_token):
    """發送 LINE 幫助信息"""
    help_text = (
        "可用指令：\n"
        "📦 上架 [天數] - 顯示上架商品，可指定 0-7 天範圍（0表示今天）\n"
        "❌ 下架 [天數] - 顯示下架商品，可指定 0-7 天範圍（0表示今天）\n"
        "🔄 補貨 - 查看即將補貨的商品\n"
        "🔧 狀態 - 檢查服務運行狀態\n"
        "📅 歷史 [天數] - 顯示指定天數內的變更記錄（默認7天）\n"
        "❓ 指令 - 顯示可用指令"
    )
    
    line_bot_api.reply_message(
        reply_token,
        TextSendMessage(text=help_text)
    )

def handle_line_restock(event):
    """處理 LINE 補貨商品請求 (使用Image Carousel)"""
    try:
        # 獲取補貨商品
        resale_products = monitor.get_resale_products()
        
        if not resale_products:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="目前沒有即將補貨的商品")
            )
            return
        
        # 按補貨日期排序
        resale_products.sort(key=lambda x: x['next_resale_date'])
        
        # 按日期分組
        products_by_date = {}
        for product in resale_products:
            date_str = product['next_resale_date'].strftime('%Y-%m-%d')
            if date_str not in products_by_date:
                products_by_date[date_str] = []
            products_by_date[date_str].append(product)
        
        # 按日期排序
        sorted_dates = sorted(products_by_date.keys())
        
        # 準備要發送的消息列表
        messages = []
        
        # 處理每個日期的商品
        for date_str in sorted_dates:
            products = products_by_date[date_str]
            total_count = len(products)
            
            # 計算與當前日期的差距
            current_date = datetime.now(TW_TIMEZONE).date()
            restock_date = datetime.strptime(date_str, '%Y-%m-%d').date()
            days_diff = (restock_date - current_date).days
            
            # 生成易讀的日期顯示
            if days_diff == 0:
                date_display = f"今天 ({date_str})"
            elif days_diff == 1:
                date_display = f"明天 ({date_str})"
            elif days_diff > 0:
                date_display = f"{days_diff} 天後 ({date_str})"
            else:
                date_display = date_str
            
            # 發送日期標題 (每個日期只發一次)
            date_title = f"補貨日期: {date_display} (共{total_count}件)"
            messages.append(TextSendMessage(text=date_title))
            
            # 每10個商品一組，使用Image Carousel顯示
            items_per_carousel = 10
            carousel_count = (total_count + items_per_carousel - 1) // items_per_carousel
            
            for i in range(carousel_count):
                start_idx = i * items_per_carousel
                end_idx = min(start_idx + items_per_carousel, total_count)
                batch_products = products[start_idx:end_idx]
                
                # 創建Image Carousel
                carousel = create_image_carousel(batch_products)
                if carousel:
                    messages.append(carousel)
        
        # 根據消息數量決定如何發送
        if len(messages) == 1:
            # 只有一條消息，直接回覆
            line_bot_api.reply_message(event.reply_token, messages[0])
        else:
            # 有多條消息，回覆第一條並推送後續消息
            line_bot_api.reply_message(event.reply_token, messages[0])
            
            # 獲取用戶ID並推送剩餘消息
            user_id = event.source.user_id
            for msg in messages[1:]:
                line_bot_api.push_message(user_id, msg)
                # 避免太快發送觸發限制
                time.sleep(0.5)
            
    except Exception as e:
        logger.error(f"處理補貨商品請求時發生錯誤: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="獲取補貨商品時發生錯誤，請稍後再試。")
            )
        except:
            pass

async def delete_all_rich_menus():
    """删除所有现有的Rich Menu"""
    try:
        # 获取所有Rich Menu
        rich_menu_list = line_bot_api.get_rich_menu_list()
        
        # 删除每个Rich Menu
        for rich_menu in rich_menu_list:
            line_bot_api.delete_rich_menu(rich_menu.rich_menu_id)
            logger.info(f"已删除Rich Menu: {rich_menu.rich_menu_id}")
        
        logger.info("所有Rich Menu已删除")
        return True
    except Exception as e:
        logger.error(f"删除Rich Menu时发生错误: {str(e)}")
        logger.error(traceback.format_exc())
        return False

async def create_rich_menus():
    """创建分页式Rich Menu"""
    try:
        
        logger.info("开始创建新的Rich Menu...")
        
        # 创建第一页Rich Menu
        rich_menu_1 = RichMenu(
            size=RICH_MENU_SIZE,
            selected=True,
            name="Page 1",
            chat_bar_text="主选单 (1/2)",
            areas=[
                RichMenuArea(
                    bounds=RichMenuBounds(x=0, y=0, width=833, height=843),
                    action=PostbackAction(label='上架', data='action=new')
                ),
                RichMenuArea(
                    bounds=RichMenuBounds(x=833, y=0, width=833, height=843),
                    action=PostbackAction(label='下架', data='action=delisted')
                ),
                RichMenuArea(
                    bounds=RichMenuBounds(x=1666, y=0, width=834, height=843),
                    action=PostbackAction(label='补货', data='action=restock')
                ),
                RichMenuArea(
                    bounds=RichMenuBounds(x=0, y=843, width=2500, height=843),
                    action=PostbackAction(label='下一页', data='switch_to_page2')
                )
            ]
        )

        logger.info("正在注册第一页Rich Menu...")
        rich_menu_id_1 = line_bot_api.create_rich_menu(rich_menu_1)
        logger.info(f"第一页Rich Menu创建成功，ID: {rich_menu_id_1}")

        # 创建第二页Rich Menu
        rich_menu_2 = RichMenu(
            size=RICH_MENU_SIZE,
            selected=False,
            name="Page 2",
            chat_bar_text="更多功能 (2/2)",
            areas=[
                RichMenuArea(
                    bounds=RichMenuBounds(x=0, y=0, width=833, height=843),
                    action=PostbackAction(label='历史', data='action=history')
                ),
                RichMenuArea(
                    bounds=RichMenuBounds(x=833, y=0, width=833, height=843),
                    action=PostbackAction(label='状态', data='action=status')
                ),
                RichMenuArea(
                    bounds=RichMenuBounds(x=1666, y=0, width=834, height=843),
                    action=PostbackAction(label='帮助', data='action=help')
                ),
                RichMenuArea(
                    bounds=RichMenuBounds(x=0, y=843, width=2500, height=843),
                    action=PostbackAction(label='上一页', data='switch_to_page1')
                )
            ]
        )

        logger.info("正在注册第二页Rich Menu...")
        rich_menu_id_2 = line_bot_api.create_rich_menu(rich_menu_2)
        logger.info(f"第二页Rich Menu创建成功，ID: {rich_menu_id_2}")

        # 从URL下载并上传Rich Menu图片
        async with aiohttp.ClientSession() as session:
            # 上传第一页图片
            logger.info(f"正在下载第一页图片: {RICH_MENU_IMAGES['page1']}")
            async with session.get(RICH_MENU_IMAGES['page1']) as response:
                if response.status == 200:
                    image_data = await response.read()
                    logger.info("第一页图片下载成功，正在上传到LINE...")
                    line_bot_api.set_rich_menu_image(rich_menu_id_1, 'image/png', image_data)
                    logger.info("第一页图片上传成功")
                else:
                    raise Exception(f"下载图片失败: {RICH_MENU_IMAGES['page1']}, 状态码: {response.status}")

            # 上传第二页图片
            logger.info(f"正在下载第二页图片: {RICH_MENU_IMAGES['page2']}")
            async with session.get(RICH_MENU_IMAGES['page2']) as response:
                if response.status == 200:
                    image_data = await response.read()
                    logger.info("第二页图片下载成功，正在上传到LINE...")
                    line_bot_api.set_rich_menu_image(rich_menu_id_2, 'image/png', image_data)
                    logger.info("第二页图片上传成功")
                else:
                    raise Exception(f"下载图片失败: {RICH_MENU_IMAGES['page2']}, 状态码: {response.status}")

        # 设置默认Rich Menu
        logger.info("正在设置默认Rich Menu...")
        line_bot_api.set_default_rich_menu(rich_menu_id_1)
        logger.info("默认Rich Menu设置成功")

        # 保存Rich Menu ID到全局变量
        global RICH_MENU_IDS
        RICH_MENU_IDS = {
            'page1': rich_menu_id_1,
            'page2': rich_menu_id_2
        }

        logger.info("Rich Menu创建完成")
        return True

    except Exception as e:
        logger.error(f"创建Rich Menu时发生错误: {str(e)}")
        logger.error(traceback.format_exc())
        return False

@bot.command(name='richmenu')
@has_role(ADMIN_ROLE_ID)
async def check_rich_menu(ctx):
    """检查Rich Menu状态"""
    try:
        rich_menu_list = line_bot_api.get_rich_menu_list()
        status = f"当前有 {len(rich_menu_list)} 个Rich Menu:\n"
        for menu in rich_menu_list:
            status += f"ID: {menu.rich_menu_id}\n名称: {menu.name}\n状态: {'默认' if menu.selected else '未选中'}\n\n"
        
        # 获取默认Rich Menu
        try:
            default_menu_id = line_bot_api.get_default_rich_menu()
            status += f"\n默认Rich Menu ID: {default_menu_id}"
        except:
            status += "\n当前没有设置默认Rich Menu"
            
        await ctx.send(status)
    except Exception as e:
        await ctx.send(f"检查失败：{str(e)}")
        logger.error(f"检查Rich Menu状态时发生错误: {str(e)}")
        logger.error(traceback.format_exc())

@line_handler.add(PostbackEvent)
def handle_postback(event):
    """处理Postback事件"""
    try:
        data = event.postback.data
        user_id = event.source.user_id

        if data == 'switch_to_page1':
            line_bot_api.link_rich_menu_to_user(user_id, RICH_MENU_IDS['page1'])
        elif data == 'switch_to_page2':
            line_bot_api.link_rich_menu_to_user(user_id, RICH_MENU_IDS['page2'])
        elif data.startswith('action='):
            action = data.split('=')[1]
            handle_menu_action(event, action)

    except Exception as e:
        logger.error(f"处理Postback事件时发生错误: {str(e)}")
        logger.error(traceback.format_exc())

def handle_menu_action(event, action):
    """处理菜单动作"""
    try:
        if action == 'new':
            handle_line_new_products(event, 0)  # 显示今日上架商品
        elif action == 'delisted':
            handle_line_delisted_products(event, 0)  # 显示今日下架商品
        elif action == 'restock':
            handle_line_restock(event)
        elif action == 'history':
            handle_line_history(event, 7)  # 显示7天历史记录
        elif action == 'status':
            handle_line_status(event.reply_token)
        elif action == 'help':
            handle_line_help(event.reply_token)

    except Exception as e:
        logger.error(f"处理菜单动作时发生错误: {str(e)}")
        logger.error(traceback.format_exc())
        try:
            line_bot_api.reply_message(
                event.reply_token,
                TextSendMessage(text="处理请求时发生错误，请稍后再试。")
            )
        except:
            pass

@bot.command(name='setdefault')
@has_role(ADMIN_ROLE_ID)
async def set_default_menu(ctx, menu_id: str = None):
    """手动设置默认Rich Menu"""
    try:
        if not menu_id:
            # 获取现有的Rich Menu列表
            rich_menu_list = line_bot_api.get_rich_menu_list()
            if not rich_menu_list:
                await ctx.send("没有找到任何Rich Menu")
                return
                
            # 默认使用第一个Rich Menu
            menu_id = rich_menu_list[0].rich_menu_id
            
        # 设置默认Rich Menu
        line_bot_api.set_default_rich_menu(menu_id)
        await ctx.send(f"已将Rich Menu {menu_id} 设置为默认选单")
        logger.info(f"已手动设置默认Rich Menu: {menu_id}")
        
    except Exception as e:
        error_msg = f"设置默认Rich Menu失败：{str(e)}"
        await ctx.send(error_msg)
        logger.error(error_msg)
        logger.error(traceback.format_exc())

@bot.command(name='resetmenu')
@has_role(ADMIN_ROLE_ID)
async def reset_rich_menu(ctx):
    """重置所有Rich Menu"""
    try:
        await ctx.send("开始重置Rich Menu...")
        
        # 重新创建Rich Menu
        success = await create_rich_menus()
        
        if success:
            await ctx.send("Rich Menu重置成功！")
            
            # 显示当前状态
            rich_menu_list = line_bot_api.get_rich_menu_list()
            status = f"当前有 {len(rich_menu_list)} 个Rich Menu:\n"
            for menu in rich_menu_list:
                status += f"ID: {menu.rich_menu_id}\n名称: {menu.name}\n状态: {'默认' if menu.selected else '未选中'}\n\n"
            
            await ctx.send(status)
        else:
            await ctx.send("Rich Menu重置失败，请查看日志获取详细信息")
            
    except Exception as e:
        error_msg = f"重置Rich Menu失败：{str(e)}"
        await ctx.send(error_msg)
        logger.error(error_msg)
        logger.error(traceback.format_exc())

# 運行 Bot
if __name__ == "__main__":
    try:
        # 檢查是否已有實例在運行
        if check_running():
            logger.error("另一個 Bot 實例已在運行，退出程序")
            sys.exit(1)
            
        # 創建進程鎖
        create_lock()
        
        # 初始化Rich Menu
        asyncio.run(create_rich_menus())
        
        # 運行 Bot
        bot.run(TOKEN)
    except Exception as e:
        logger.error(f"Bot crashed: {str(e)}")
        logger.error(traceback.format_exc())
    finally:
        # 確保在任何情況下都移除進程鎖
        remove_lock() 
