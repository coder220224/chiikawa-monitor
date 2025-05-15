import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import logging
import os
import time
import random
import sqlite3
import json
import pandas as pd
from pymongo import MongoClient
from config import MONGODB_URI
import urllib3
import requests.packages.urllib3.util.ssl_
import sys
import traceback
import brotli  # 添加 brotli 支持
import pytz
import pymongo

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

# 禁用警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 設置 SSL 上下文
requests.packages.urllib3.util.ssl_.DEFAULT_CIPHERS += ':HIGH:!DH:!aNULL'

class ChiikawaMonitor:
    def __init__(self):
        self.base_url = "https://chiikawamarket.jp"
        self.work_dir = os.path.dirname(os.path.abspath(__file__))
        self.excel_path = os.path.join(self.work_dir, 'chiikawa_products.xlsx')
        
        # MongoDB 設置
        try:
            self.client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                tls=True
            )
            
            # 測試連接
            self.client.admin.command('ping')
            logger.info("MongoDB 連接成功！")
            
            self.db = self.client['chiikawa']
            
            # 确保所有集合存在
            self.ensure_collections_exist()
            
            # 获取集合引用
            self.products = self.db['products']
            self.history = self.db['history']  # 保留原有的 history 集合
            self.resale = self.db['resale']    # 补货集合
            self.new = self.db['new']          # 新上架集合
            self.delisted = self.db['delisted'] # 下架集合
            
            # 建立索引
            self.ensure_indexes()
            
        except Exception as e:
            logger.error(f"MongoDB 連接錯誤: {str(e)}")
            logger.error(traceback.format_exc())
            raise

        # 設置請求頭
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://chiikawamarket.jp/",
            "X-Requested-With": "XMLHttpRequest"
        }
        
        # 創建 Session 並設置 SSL 驗證
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        self.session.verify = False

    def decode_response(self, response):
        """解碼響應內容，處理各種壓縮格式"""
        try:
            if response.headers.get('content-encoding') == 'br':
                return brotli.decompress(response.content).decode('utf-8')
            return response.text
        except Exception as e:
            logger.error(f"解碼響應內容失敗: {str(e)}")
            return None

    def update_excel(self):
        """更新 Excel 文件"""
        try:
            # 從數據庫獲取所有商品
            products = self.get_all_products()
            
            # 格式化時間列
            for product in products:
                product['last_seen'] = product['last_seen'].strftime('%Y-%m-%d %H:%M:%S')
            
            # 保存到 Excel
            df = pd.DataFrame(products)
            df.to_excel(self.excel_path, index=False, engine='openpyxl')
            logger.info(f"已更新 Excel 文件：{self.excel_path}")
            
            return True
        except Exception as e:
            logger.error(f"更新 Excel 時發生錯誤：{str(e)}")
            return False

    def fetch_products(self):
        """獲取所有商品信息"""
        try:
            logger.info("\n=== 開始獲取商品數據 ===")
            logger.info(f"基礎 URL: {self.base_url}")
            
            # 測試基本連接
            try:
                logger.info("\n1. 測試基礎連接...")
                test_response = self.session.get(self.base_url, timeout=30)
                logger.info(f"基礎連接狀態碼: {test_response.status_code}")
                
                if test_response.status_code != 200:
                    logger.error(f"警告：基礎連接返回非 200 狀態碼")
                    return []
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"基礎連接測試失敗: {str(e)}")
                logger.error(traceback.format_exc())
                return []
            
            # 測試 API 端點
            logger.info("\n2. 測試商品 API...")
            api_url = f"{self.base_url}/zh-hant/products.json"
            logger.info(f"API URL: {api_url}")
            
            try:
                logger.info("發送 API 請求...")
                api_response = self.session.get(
                    api_url, 
                    params={'page': 1, 'limit': 1}, 
                    timeout=30
                )
                logger.info(f"API 響應狀態碼: {api_response.status_code}")
                
                if api_response.status_code == 200:
                    try:
                        # requests 會自動處理解壓縮
                        data = api_response.json()
                        logger.info("成功解析 JSON 響應")
                        logger.info(f"響應數據預覽: {str(data)[:200]}")
                        
                        if 'products' not in data:
                            logger.error("錯誤：響應中沒有 products 字段")
                            return []
                            
                    except json.JSONDecodeError as e:
                        logger.error(f"JSON 解析失敗: {str(e)}")
                        logger.error(f"原始響應內容: {api_response.text[:200]}")
                        return []
                else:
                    logger.error(f"API 請求失敗，狀態碼: {api_response.status_code}")
                    return []
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"API 請求失敗: {str(e)}")
                logger.error(traceback.format_exc())
                return []
                
            # 開始獲取所有商品
            logger.info("\n3. 開始獲取完整商品列表...")
            total_products = 0
            page = 1
            new_products_data = []
            seen_handles = set()
            
            while True:
                try:
                    logger.info(f"\n獲取第 {page} 頁...")
                    response = self.session.get(
                        api_url,
                        params={'page': page, 'limit': 250},
                        timeout=30
                    )
                    
                    if response.status_code != 200:
                        logger.error(f"獲取第 {page} 頁失敗，狀態碼: {response.status_code}")
                        break
                        
                    try:
                        data = response.json()
                    except json.JSONDecodeError as e:
                        logger.error(f"解析第 {page} 頁 JSON 失敗: {str(e)}")
                        break
                        
                    if not isinstance(data, dict) or 'products' not in data:
                        logger.error(f"第 {page} 頁數據格式錯誤")
                        break
                        
                    products = data['products']
                    if not products:
                        logger.info("沒有更多商品")
                        break
                        
                    page_count = 0
                    for product in products:
                        try:
                            handle = product.get('handle', '')
                            if not handle or handle in seen_handles:
                                continue
                                
                            seen_handles.add(handle)
                            title = product.get('title', '')
                            variants = product.get('variants', [])
                            
                            price = 0
                            available = False
                            if variants:
                                variant = variants[0]
                                price = int(float(variant.get('price', 0)))
                                available = variant.get('available', False)
                            
                            # 獲取商品圖片URL
                            image_url = None
                            if 'images' in product and product['images'] and len(product['images']) > 0:
                                first_image = product['images'][0]
                                if isinstance(first_image, dict) and 'src' in first_image:
                                    image_url = first_image['src']
                            
                            # 如果沒有圖片，使用默認圖片
                            if not image_url:
                                image_url = 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png'
                                
                            product_url = f"{self.base_url}/zh-hant/products/{handle}"
                            new_products_data.append({
                                'url': product_url,
                                'name': title,
                                'price': price,
                                'available': available,
                                'tags': product.get('tags', []),
                                'image_url': image_url,  # 存儲圖片URL
                                'last_seen': datetime.now(TW_TIMEZONE)
                            })
                            
                            total_products += 1
                            page_count += 1
                            
                        except Exception as e:
                            logger.error(f"處理商品時出錯: {str(e)}")
                            continue
                            
                    logger.info(f"第 {page} 頁處理完成，獲取 {page_count} 個商品")
                    if page_count == 0:
                        break
                        
                    page += 1
                    time.sleep(1)
                    
                except Exception as e:
                    logger.error(f"處理第 {page} 頁時出錯: {str(e)}")
                    break
                
            logger.info(f"\n=== 商品獲取完成 ===")
            logger.info(f"總共獲取: {total_products} 個商品")
            return new_products_data
            
        except Exception as e:
            logger.error(f"商品獲取過程中發生錯誤: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def update_products(self, products_data):
        """更新商品数据到数据库"""
        try:
            if not products_data:
                logger.warning("没有商品数据需要更新")
                return
            
            start_time = time.time()
            logger.info("开始更新商品数据...")
            
            # 1. 获取new集合中的商品URL
            new_products = list(self.new.find({}, {'url': 1, '_id': 0}))
            new_urls = {p['url'] for p in new_products}
            
            # 2. 如果new集合中有商品，检查并清理delisted和resale集合
            if new_urls:
                # 从下架集合中删除已重新上架的商品
                delisted_result = self.delisted.delete_many({
                    'url': {'$in': list(new_urls)}
                })
                if delisted_result.deleted_count > 0:
                    logger.info(f"从下架集合中删除 {delisted_result.deleted_count} 个重新上架的商品")
                
                # 从补货集合中删除已重新上架的商品
                resale_result = self.resale.delete_many({
                    'url': {'$in': list(new_urls)}
                })
                if resale_result.deleted_count > 0:
                    logger.info(f"从补货集合中删除 {resale_result.deleted_count} 个已上架的商品")
            
            # 3. 更新商品数据
            current_time = datetime.now(TW_TIMEZONE)
            operations = []
            for product in products_data:
                if 'url' not in product:
                    continue
                
                product['last_seen'] = current_time
                operations.append(
                    pymongo.UpdateOne(
                        {'url': product['url']},
                        {'$set': product},
                        upsert=True
                    )
                )
            
            # 执行批量更新
            if operations:
                result = self.products.bulk_write(operations, ordered=False)
                logger.info(f"商品数据更新完成：{len(operations)} 个商品")
                logger.info(f"更新结果：matched={result.matched_count}, modified={result.modified_count}, upserted={result.upserted_count}")
            
            # 同步更新history集合中的商品库存状态
            self.sync_product_availability(products_data)
            
            # 处理 RE 标签的商品
            self.process_resale_items(products_data)
            
            # 清理過舊的數據記錄
            self.clean_old_records()
            
            logger.info(f"所有更新操作完成，总耗时：{time.time() - start_time:.2f}秒")
            return True
            
        except Exception as e:
            logger.error(f"更新数据库时发生错误：{str(e)}")
            logger.error(traceback.format_exc())
            return False

    def sync_product_availability(self, products_data):
        """同步更新history和new集合中的商品库存状态"""
        try:
            start_time = time.time()
            logger.info("开始同步商品库存状态...")

            # 准备批量更新操作
            history_operations = []
            new_operations = []
            
            # 将商品数据转换为以URL为键的字典
            products_dict = {p['url']: p['available'] for p in products_data if 'url' in p and 'available' in p}
            
            if not products_dict:
                logger.info("没有需要更新的商品库存状态")
                return True
            
            # 创建批量更新操作
            for url, available in products_dict.items():
                # 更新history集合中type为new的记录
                history_operations.append(
                    pymongo.UpdateMany(
                        {'url': url, 'type': 'new'},
                        {'$set': {'available': available}}
                    )
                )
                
                # 更新new集合中的记录
                new_operations.append(
                    pymongo.UpdateMany(
                        {'url': url},
                        {'$set': {'available': available}}
                    )
                )
            
            # 执行批量更新
            if history_operations:
                history_result = self.history.bulk_write(history_operations, ordered=False)
                logger.info(f"history集合更新完成：matched={history_result.matched_count}, modified={history_result.modified_count}")
                
            if new_operations:
                new_result = self.new.bulk_write(new_operations, ordered=False)
                logger.info(f"new集合更新完成：matched={new_result.matched_count}, modified={new_result.modified_count}")
            
            logger.info(f"库存状态同步完成，耗时：{time.time() - start_time:.2f}秒")
            return True
            
        except Exception as e:
            logger.error(f"同步库存状态时发生错误: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def process_resale_items(self, products_data):
        """處理具有 RE 標籤的商品，並更新 resale 集合"""
        try:
            start_time = time.time()
            logger.info("开始处理补货商品...")
            
            # 计数器
            resale_tags_count = 0
            
            # 获取当前时间
            current_time = datetime.now(TW_TIMEZONE)
            
            # 批量操作列表
            bulk_operations = []
            
            # 遍历所有商品
            logger.info(f"开始处理 {len(products_data)} 个商品的标签")
            for product in products_data:
                if 'tags' not in product or not product['tags']:
                    continue
                    
                # 调试日志：输出商品名称和标签
                logger.info(f"处理商品: {product.get('name', 'Unknown')}")
                logger.info(f"商品标签: {product['tags']}")
                
                # 只查找 RE2025 开头的标签
                resale_tags = [tag for tag in product['tags'] 
                             if tag.startswith('RE2025') and len(tag) >= 10]
                
                if not resale_tags:
                    continue
                
                logger.info(f"发现 RE2025 标签: {resale_tags}")
                
                # 提取补货日期
                valid_resale_dates = []
                for tag in resale_tags:
                    try:
                        date_str = tag[2:]  # 提取日期部分 (YYYYMMDD)
                        year = int(date_str[:4])
                        month = int(date_str[4:6])
                        day = int(date_str[6:8])
                        resale_date = datetime(year, month, day).replace(tzinfo=TW_TIMEZONE)
                        
                        # 只添加比当前时间晚的日期
                        if resale_date > current_time:
                            valid_resale_dates.append(resale_date)
                            logger.info(f"有效的补货日期: {resale_date}")
                            
                    except Exception as e:
                        logger.error(f"解析 RE 标签日期失败: {tag}, 错误: {str(e)}")
                
                if not valid_resale_dates:
                    continue
                
                # 获取最近的补货日期
                next_resale_date = min(valid_resale_dates)
                resale_tags_count += 1
                
                # 准备更新操作
                bulk_operations.append(
                    pymongo.UpdateOne(
                        {'url': product['url']},
                        {'$set': {
                            'name': product['name'],
                            'price': product.get('price', 0),
                            'available': product.get('available', False),
                            'tags': product.get('tags', []),
                            'resale_tags': resale_tags,
                            'next_resale_date': next_resale_date,
                            'last_updated': current_time,
                            'detected_date': current_time,
                            'image_url': product.get('image_url', 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png')
                        }},
                        upsert=True
                    )
                )
                
                if len(bulk_operations) >= 500:  # 每500个操作执行一次批量更新
                    result = self.resale.bulk_write(bulk_operations, ordered=False)
                    logger.info(f"批量更新补货商品：matched={result.matched_count}, modified={result.modified_count}, upserted={result.upserted_count}")
                    bulk_operations = []
            
            # 执行剩余的批量操作
            if bulk_operations:
                result = self.resale.bulk_write(bulk_operations, ordered=False)
                logger.info(f"批量更新补货商品：matched={result.matched_count}, modified={result.modified_count}, upserted={result.upserted_count}")
            
            logger.info(f"RE 标签处理完成：发现 {resale_tags_count} 个补货商品，耗时：{time.time() - start_time:.2f}秒")
            return True
            
        except Exception as e:
            logger.error(f"处理 RE 标签商品时发生错误: {str(e)}")
            logger.error(traceback.format_exc())
            return False
            
    def get_resale_products(self, days=None):
        """獲取即將補貨的商品
        
        Args:
            days: 如果指定，則只返回指定天數內即將補貨的商品
        
        Returns:
            符合條件的補貨商品列表
        """
        try:
            query = {}
            
            # 如果指定了天數，添加日期篩選條件
            if days is not None:
                today = datetime.now(TW_TIMEZONE)
                target_date = today + timedelta(days=days)
                query = {
                    'next_resale_date': {
                        '$gte': today,
                        '$lte': target_date
                    }
                }
                
            # 按補貨日期排序
            products = list(self.resale.find(
                query, 
                {'_id': 0}
            ).sort('next_resale_date', 1))
            
            return products
            
        except Exception as e:
            logger.error(f"獲取補貨商品時發生錯誤: {str(e)}")
            logger.error(traceback.format_exc())
            return []

    def get_all_products(self):
        """獲取所有商品"""
        try:
            return list(self.products.find({}, {'_id': 0}))
        except Exception as e:
            logger.error(f"獲取所有商品時發生錯誤: {str(e)}")
            return []

    def record_history(self, product, type_):
        """記錄商品歷史"""
        try:
            today = datetime.now(TW_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
            exists = self.history.find_one({
                'url': product['url'],
                'type': type_,
                'date': {'$gte': today}
            })
            if exists:
                logger.info(f"已存在同一天同 type 同 url 的歷史紀錄，不重複寫入: {product['name']}")
                return False
            
            current_time = datetime.now(TW_TIMEZONE)
            
            # 創建通用的歷史數據
            history_data = {
                'date': current_time,
                'type': type_,
                'name': product['name'],
                'url': product['url'],
                'time': current_time
            }
            
            # 添加圖片URL（如果有）
            if 'image_url' in product:
                history_data['image_url'] = product['image_url']
            
            # 如果是下架商品，但沒有圖片URL，嘗試從資料庫獲取
            if type_ == 'delisted' and 'image_url' not in history_data:
                try:
                    # 從資料庫中搜尋該商品
                    existing_product = self.products.find_one({'url': product['url']})
                    if existing_product and 'image_url' in existing_product:
                        history_data['image_url'] = existing_product['image_url']
                        logger.info(f"為下架商品 {product['name']} 從資料庫中恢復圖片URL")
                    else:
                        # 如果資料庫中沒有，使用默認圖片
                        history_data['image_url'] = 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png'
                except Exception as e:
                    logger.error(f"嘗試獲取下架商品圖片URL時出錯: {str(e)}")
                    # 使用默認圖片
                    history_data['image_url'] = 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png'
            
            # 向原有的 history 集合寫入數據（保持向後兼容性）
            self.history.insert_one(history_data)
            
            # 根據類型分別寫入到對應的集合
            if type_ == 'new':
                # 如果是新上架商品，先检查并删除下架和补货集合中的记录
                delisted_result = self.delisted.delete_many({'url': product['url']})
                if delisted_result.deleted_count > 0:
                    logger.info(f"商品重新上架，从下架集合中删除: {product['name']}")
                    
                resale_result = self.resale.delete_many({'url': product['url']})
                if resale_result.deleted_count > 0:
                    logger.info(f"商品已上架，从补货集合中删除: {product['name']}")
                
                # 附加更多信息到新上架記錄
                new_data = history_data.copy()
                # 添加額外的字段
                if isinstance(product, dict):
                    new_data.update({
                        'price': product.get('price', 0),
                        'available': product.get('available', False),
                        'tags': product.get('tags', [])
                    })
                # 寫入到新上架集合
                self.new.insert_one(new_data)
                logger.info(f"商品已添加到新上架集合: {product['name']}")
                
            elif type_ == 'delisted':
                # 寫入到下架集合
                self.delisted.insert_one(history_data)
                logger.info(f"商品已添加到下架集合: {product['name']}")
            
            return True
        except Exception as e:
            logger.error(f"記錄歷史時發生錯誤：{str(e)}")
            return False

    def get_today_history(self, type_):
        """獲取今日的歷史記錄（舊方法，保持向後兼容性）"""
        try:
            today = datetime.now(TW_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
            query = {
                'date': {'$gte': today},
                'type': type_
            }
            return list(self.history.find(query, {'_id': 0}))
        except Exception as e:
            logger.error(f"獲取歷史記錄時發生錯誤: {str(e)}")
            return []
        
    def get_today_new_products(self):
        """獲取今日新上架的商品"""
        try:
            today = datetime.now(TW_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
            query = {
                'date': {'$gte': today}
            }
            return list(self.new.find(query, {'_id': 0}))
        except Exception as e:
            logger.error(f"獲取今日新上架商品時發生錯誤: {str(e)}")
            return []
        
    def get_today_delisted_products(self):
        """獲取今日下架的商品"""
        try:
            today = datetime.now(TW_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
            query = {
                'date': {'$gte': today}
            }
            return list(self.delisted.find(query, {'_id': 0}))
        except Exception as e:
            logger.error(f"獲取今日下架商品時發生錯誤: {str(e)}")
            return []
        
    def get_period_new_products(self, days=7):
        """獲取指定天數內新上架的商品"""
        try:
            start_date = datetime.now(TW_TIMEZONE) - timedelta(days=days)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            query = {
                'date': {'$gte': start_date}
            }
            return list(self.new.find(query, {'_id': 0}).sort('date', -1))
        except Exception as e:
            logger.error(f"獲取指定天數內新上架商品時發生錯誤: {str(e)}")
            return []
        
    def get_period_delisted_products(self, days=7):
        """獲取指定天數內下架的商品"""
        try:
            start_date = datetime.now(TW_TIMEZONE) - timedelta(days=days)
            start_date = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
            query = {
                'date': {'$gte': start_date}
            }
            return list(self.delisted.find(query, {'_id': 0}).sort('date', -1))
        except Exception as e:
            logger.error(f"獲取指定天數內下架商品時發生錯誤: {str(e)}")
            return []

    def check_product_url(self, url):
        """檢查商品URL是否可訪問"""
        try:
            response = self.session.head(url, allow_redirects=True, timeout=10)
            return response.status_code == 200
        except:
            return False

    def close(self):
        """關閉數據庫連接（MongoDB 不需要）"""
        pass
            
    def __del__(self):
        """析構函數"""
        self.close()

    def get_total_products_from_web(self):
        """從網頁直接獲取商品總數"""
        try:
            # 訪問商品列表頁面
            url = f"{self.base_url}/zh-hant/collections/all"
            logger.info(f"訪問商品列表頁面: {url}")
            
            response = self.session.get(url, timeout=30)
            if response.status_code != 200:
                logger.error(f"獲取頁面失敗，狀態碼: {response.status_code}")
                return None
                
            # 使用 BeautifulSoup 解析頁面
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # 尋找商品數量信息
            # 通常在類似 "xxx 件商品" 的文字中
            product_count_text = None
            
            # 方法1：從商品計數器中獲取
            count_element = soup.find('div', {'class': 'collection-counter'})
            if count_element:
                product_count_text = count_element.text.strip()
                
            # 方法2：從商品網格中計算
            if not product_count_text:
                product_grid = soup.find('div', {'class': 'product-grid'})
                if product_grid:
                    products = product_grid.find_all('div', {'class': 'grid__item'})
                    return len(products)
            
            # 方法3：從分頁信息中獲取
            if not product_count_text:
                pagination = soup.find('div', {'class': 'pagination'})
                if pagination:
                    last_page = pagination.find_all('a')[-2].text.strip()
                    try:
                        total_pages = int(last_page)
                        # 假設每頁顯示24個商品（這是常見的設置）
                        return total_pages * 24
                    except ValueError:
                        pass
            
            # 如果找到了文字形式的數量
            if product_count_text:
                # 提取數字
                import re
                numbers = re.findall(r'\d+', product_count_text)
                if numbers:
                    return int(numbers[0])
            
            logger.error("無法從網頁獲取商品總數")
            return None
            
        except Exception as e:
            logger.error(f"從網頁獲取商品總數時出錯: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def clean_old_records(self):
        """清理過舊的數據記錄"""
        try:
            start_time = time.time()
            logger.info("開始清理過舊記錄...")
            
            # 獲取集合列表
            collections = self.db.list_collection_names()
            
            # 計算時間點
            now = datetime.now(TW_TIMEZONE)
            seven_days_ago = now - timedelta(days=7)
            thirty_days_ago = now - timedelta(days=30)
            
            total_deleted = 0
            
            # 清理超過7天的 new 記錄
            if 'new' in collections:
                result = self.new.delete_many({'date': {'$lt': seven_days_ago}})
                deleted_count = result.deleted_count
                total_deleted += deleted_count
                logger.info(f"已清理 {deleted_count} 條超過7天的新上架記錄")
            
            # 清理超過7天的 delisted 記錄
            if 'delisted' in collections:
                result = self.delisted.delete_many({'date': {'$lt': seven_days_ago}})
                deleted_count = result.deleted_count
                total_deleted += deleted_count
                logger.info(f"已清理 {deleted_count} 條超過7天的下架記錄")
            
            # 清理超過30天的 history 記錄
            if 'history' in collections:
                result = self.history.delete_many({'date': {'$lt': thirty_days_ago}})
                deleted_count = result.deleted_count
                total_deleted += deleted_count
                logger.info(f"已清理 {deleted_count} 條超過30天的歷史記錄")
            
            logger.info(f"清理完成，共删除 {total_deleted} 條記錄，耗時：{time.time() - start_time:.2f}秒")
            return True
            
        except Exception as e:
            logger.error(f"清理過舊記錄時發生錯誤: {str(e)}")
            logger.error(traceback.format_exc())
            return False

    def ensure_collections_exist(self):
        """確保所有必要的集合存在"""
        collections = self.db.list_collection_names()
        required_collections = ['products', 'history', 'resale', 'new', 'delisted']
        
        for collection in required_collections:
            if collection not in collections:
                # 創建集合（在MongoDB中，寫入第一個文檔時會自動創建集合）
                logger.info(f"集合 '{collection}' 不存在，將自動創建")
                
    def ensure_indexes(self):
        """确保所有必要的索引存在"""
        try:
            # 建立索引
            self.products.create_index('url', unique=True)
            self.history.create_index([('date', 1), ('type', 1)])
            self.resale.create_index('url', unique=True)
            self.new.create_index([('date', 1)])
            self.delisted.create_index([('date', 1)])
        except Exception as e:
            logger.error(f"建立索引時發生錯誤: {str(e)}")
            logger.error(traceback.format_exc())

if __name__ == "__main__":
    # 測試代碼
    monitor = ChiikawaMonitor()
    try:
        logger.info("測試獲取商品...")
        total = monitor.fetch_products()
        logger.info(f"共獲取到 {len(total)} 個商品")
        
        logger.info("\n獲取所有商品...")
        products = monitor.get_all_products()
        for product in products[:5]:  # 只顯示前5個
            logger.info(f"- {product['name']}")
            
    finally:
        monitor.close() 
