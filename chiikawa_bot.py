import discord
from discord.ext import commands, tasks
from datetime import datetime
import os
import aiohttp
import asyncio
from chiikawa_monitor import ChiikawaMonitor
import logging
import sys
from config import TOKEN, CHANNEL_ID, WORK_DIR
from aiohttp import web
import socket
import ssl
import traceback

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

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
            await super().close()
        except Exception as e:
            print(f"關閉時發生錯誤: {e}")

bot = ProxyBot(command_prefix='!', intents=intents)

# 初始化監控器
monitor = ChiikawaMonitor()

# 添加日誌記錄
logging.basicConfig(
    filename=os.path.join(WORK_DIR, 'bot.log'),
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

async def check_updates(channel):
    """檢查商品更新"""
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
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
        
        # 如果有變化，額外發送 @everyone 通知
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
            
            await channel.send("@everyone 檢測到商品變化！", embed=alert_embed)
        
        logger.info(f"=== 檢查完成 ===\n")
            
    except Exception as e:
        error_msg = f"檢查更新時發生錯誤: {str(e)}"
        logger.error(error_msg)
        logger.error(traceback.format_exc())

@bot.event
async def on_ready():
    logging.info(f'Bot logged in as {bot.user.name}')
    print(f'Bot logged in as {bot.user.name}')

@bot.command(name='start')
@commands.has_permissions(administrator=True)
async def start_monitoring(ctx):
    """執行一次商品更新檢查"""
    try:
        channel = bot.get_channel(CHANNEL_ID)
        if not channel:
            await ctx.send(f"錯誤：找不到頻道 {CHANNEL_ID}")
            return
            
        await ctx.send("開始檢查商品更新...")
        await check_updates(channel)
        await ctx.send("檢查完成！")
        
    except Exception as e:
        await ctx.send(f"執行失敗：{str(e)}")
        print(f"執行失敗：{str(e)}")

@bot.command(name='上架')
async def new_listings(ctx):
    """顯示今日上架的商品"""
    try:
        new_products = monitor.get_today_history('new')
        
        if not new_products:
            embed = discord.Embed(title="今日上架商品", description="今天還沒有新商品上架", color=0xff0000)
            await ctx.send(embed=embed)
            return
            
        embed = discord.Embed(title="今日上架商品", color=0x00ff00)
        for product in new_products:
            time_str = product['time'].strftime('%H:%M:%S')
            field_content = f"🆕 上架時間: {time_str}\n[商品連結]({product['url']})"
            embed.add_field(name=product['name'], value=field_content, inline=False)
        
        await ctx.send(embed=embed)
            
    except Exception as e:
        await ctx.send(f"讀取上架記錄時發生錯誤：{str(e)}")

@bot.command(name='下架')
async def delisted(ctx):
    """顯示今日下架的商品"""
    try:
        delisted_products = monitor.get_today_history('delisted')
        
        if not delisted_products:
            embed = discord.Embed(title="今日下架商品", description="今天還沒有商品下架", color=0xff0000)
            await ctx.send(embed=embed)
            return
            
        embed = discord.Embed(title="今日下架商品", color=0xff0000)
        for product in delisted_products:
            time_str = product['time'].strftime('%H:%M:%S')
            field_content = f"❌ 下架時間: {time_str}\n[商品連結]({product['url']})"
            embed.add_field(name=product['name'], value=field_content, inline=False)
        
        await ctx.send(embed=embed)
            
    except Exception as e:
        await ctx.send(f"讀取下架記錄時發生錯誤：{str(e)}")

@bot.command(name='檢查')
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
        web_count = await bot.loop.run_in_executor(None, monitor.get_total_products_from_web)
        
        # 創建嵌入消息
        embed = discord.Embed(
            title="🔍 商品數量檢查",
            description="比較不同來源的商品數量",
            color=0x00ff00
        )
        
        embed.add_field(
            name="資料庫商品數量",
            value=f"📚 {db_count} 個商品",
            inline=True
        )
        
        embed.add_field(
            name="API 獲取數量",
            value=f"🌐 {api_count} 個商品",
            inline=True
        )
        
        if web_count is not None:
            embed.add_field(
                name="網頁顯示數量",
                value=f"🔖 {web_count} 個商品",
                inline=True
            )
        
        # 檢查差異
        has_difference = False
        differences = []
        
        if api_count != db_count:
            diff = abs(api_count - db_count)
            differences.append(f"API 與資料庫差異：{diff} 個商品")
            has_difference = True
            
        if web_count is not None:
            if web_count != api_count:
                diff = abs(web_count - api_count)
                differences.append(f"網頁與 API 差異：{diff} 個商品")
                has_difference = True
            if web_count != db_count:
                diff = abs(web_count - db_count)
                differences.append(f"網頁與資料庫差異：{diff} 個商品")
                has_difference = True
        
        if has_difference:
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
        
        # 添加商品列表頁面連結
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

@bot.command(name='commands', aliases=['command', '指令'])
async def show_commands(ctx):
    """顯示所有可用的指令"""
    embed = discord.Embed(
        title="吉伊卡哇官網監控 指令列表",
        description="以下是所有可用的指令：",
        color=discord.Color.blue()
    )
    
    embed.add_field(
        name="!start",
        value="執行一次商品更新檢查",
        inline=False
    )
    embed.add_field(
        name="!上架",
        value="顯示今日新上架的商品",
        inline=False
    )
    embed.add_field(
        name="!下架",
        value="顯示今日下架的商品",
        inline=False
    )
    embed.add_field(
        name="!檢查",
        value="檢查資料庫和網站的商品數量是否一致",
        inline=False
    )
    embed.add_field(
        name="!commands",
        value="顯示此幫助信息（別名：!command、!指令）",
        inline=False
    )
    
    await ctx.send(embed=embed)

async def healthcheck(request):
    return web.Response(text="Bot is running!")

async def setup_webserver():
    app = web.Application()
    app.router.add_get('/', healthcheck)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    print(f"Web server started on port {port}")

# 運行 Bot
if __name__ == "__main__":
    try:
        bot.run(TOKEN)
    except Exception as e:
        logging.error(f"Bot crashed: {str(e)}")
        print(f"Error: {str(e)}") 
