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

    def fetch_products(self, max_retries=3, retry_delay=5):
        """獲取所有商品信息，失敗時會重試"""
        for attempt in range(max_retries):
            try:
                logger.info(f"\n=== 開始獲取商品數據 (第 {attempt + 1} 次嘗試) ===")
                logger.info(f"基礎 URL: {self.base_url}")
                
                # 測試基本連接
                try:
                    logger.info("\n1. 測試基礎連接...")
                    test_response = self.session.get(self.base_url, timeout=30)
                    logger.info(f"基礎連接狀態碼: {test_response.status_code}")
                    
                    if test_response.status_code != 200:
                        logger.error(f"警告：基礎連接返回非 200 狀態碼")
                        if attempt < max_retries - 1:
                            logger.info(f"等待 {retry_delay} 秒後重試...")
                            time.sleep(retry_delay)
                            continue
                        return []
                        
                except requests.exceptions.RequestException as e:
                    logger.error(f"基礎連接測試失敗: {str(e)}")
                    logger.error(traceback.format_exc())
                    if attempt < max_retries - 1:
                        logger.info(f"等待 {retry_delay} 秒後重試...")
                        time.sleep(retry_delay)
                        continue
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
                            data = api_response.json()
                            logger.info("成功解析 JSON 響應")
                            logger.info(f"響應數據預覽: {str(data)[:200]}")
                            
                            if 'products' not in data:
                                logger.error("錯誤：響應中沒有 products 字段")
                                if attempt < max_retries - 1:
                                    logger.info(f"等待 {retry_delay} 秒後重試...")
                                    time.sleep(retry_delay)
                                    continue
                                return []
                                
                        except json.JSONDecodeError as e:
                            logger.error(f"JSON 解析失敗: {str(e)}")
                            logger.error(f"原始響應內容: {api_response.text[:200]}")
                            if attempt < max_retries - 1:
                                logger.info(f"等待 {retry_delay} 秒後重試...")
                                time.sleep(retry_delay)
                                continue
                            return []
                    else:
                        logger.error(f"API 請求失敗，狀態碼: {api_response.status_code}")
                        if attempt < max_retries - 1:
                            logger.info(f"等待 {retry_delay} 秒後重試...")
                            time.sleep(retry_delay)
                            continue
                        return []
                        
                except requests.exceptions.RequestException as e:
                    logger.error(f"API 請求失敗: {str(e)}")
                    logger.error(traceback.format_exc())
                    if attempt < max_retries - 1:
                        logger.info(f"等待 {retry_delay} 秒後重試...")
                        time.sleep(retry_delay)
                        continue
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
                if attempt < max_retries - 1:
                    logger.info(f"等待 {retry_delay} 秒後重試...")
                    time.sleep(retry_delay)
                    continue
                return []
        
        logger.error(f"已重試 {max_retries} 次仍然失敗")
        return []

    def update_products(self, products_data):
        """更新商品数据到数据库"""
        try:
            if not products_data:
                logger.warning("没有商品数据需要更新")
                return
            
            start_time = time.time()
            logger.info("开始更新商品数据...")
            
            # 1. 获取现有的所有商品数据（用于比对和保存下架商品信息）
            existing_products = list(self.products.find({}))
            # 确保现有数据中的时间都转换为台湾时区
            for product in existing_products:
                if 'last_seen' in product:
                    product['last_seen'] = self.ensure_timezone(product['last_seen'])
            
            existing_products_dict = {p['url']: p for p in existing_products}
            existing_urls = set(existing_products_dict.keys())
            
            # 2. 整理新获取的商品数据
            new_products_dict = {p['url']: p for p in products_data}
            new_urls = set(new_products_dict.keys())
            
            # 3. 找出新上架、下架的商品
            new_listing_urls = new_urls - existing_urls  # 新上架
            delisted_urls = existing_urls - new_urls     # 下架
            
            current_time = datetime.now(TW_TIMEZONE)
            
            # 4. 如果有新商品，清理相关集合
            if new_listing_urls:
                # 从下架集合中删除已重新上架的商品
                delisted_result = self.delisted.delete_many({
                    'url': {'$in': list(new_listing_urls)}
                })
                if delisted_result.deleted_count > 0:
                    logger.info(f"从下架集合中删除 {delisted_result.deleted_count} 个重新上架的商品")
                
                # 从补货集合中删除已上架的商品
                resale_result = self.resale.delete_many({
                    'url': {'$in': list(new_listing_urls)}
                })
                if resale_result.deleted_count > 0:
                    logger.info(f"从补货集合中删除 {resale_result.deleted_count} 个已上架的商品")
            
            # 5. 处理下架商品（使用原有数据）
            for url in delisted_urls:
                original_product = existing_products_dict[url]
                history_data = {
                    'date': current_time,
                    'type': 'delisted',
                    'name': original_product['name'],
                    'url': original_product['url'],
                    'image_url': original_product.get('image_url', 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png'),
                    'price': original_product.get('price', 0),
                    'time': current_time
                }
                
                # 写入下架集合
                self.delisted.insert_one(history_data)
                # 写入历史记录
                self.history.insert_one(history_data)
                logger.info(f"记录下架商品: {original_product['name']}")
            
            # 6. 处理新上架商品（使用新数据）
            for url in new_listing_urls:
                new_product = new_products_dict[url]
                history_data = {
                    'date': current_time,
                    'type': 'new',
                    'name': new_product['name'],
                    'url': new_product['url'],
                    'image_url': new_product.get('image_url', 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png'),
                    'price': new_product.get('price', 0),
                    'available': new_product.get('available', False),
                    'tags': new_product.get('tags', []),
                    'time': current_time
                }
                
                # 检查是否是重新上架
                was_delisted = self.delisted.find_one({'url': url})
                if was_delisted:
                    history_data['is_restock'] = True
                    logger.info(f"商品重新上架: {new_product['name']}")
                else:
                    history_data['is_restock'] = False
                    logger.info(f"新商品上架: {new_product['name']}")
                
                # 写入新上架集合
                self.new.insert_one(history_data)
                # 写入历史记录
                self.history.insert_one(history_data)
            
            # 7. 处理补货商品（使用新数据）
            self.process_resale_items(products_data)
            
            # 8. 最后，更新 products 集合（完全同步到最新状态）
            # 先清空 products 集合
            self.products.delete_many({})
            
            # 批量插入新数据
            if products_data:
                # 确保所有文档都有带时区的 last_seen 字段
                for product in products_data:
                    product['last_seen'] = current_time  # current_time 已经带有台湾时区信息
                
                self.products.insert_many(products_data)
                logger.info(f"products 集合更新完成：插入 {len(products_data)} 个商品")
            
            # 9. 同步商品库存状态到历史记录
            self.sync_product_availability(products_data)
            
            # 10. 清理过旧的记录
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
            logger.info("開始處理補貨商品...")
            
            # 计数器
            resale_tags_count = 0
            
            # 获取当前时间
            current_time = datetime.now(TW_TIMEZONE)
            current_date = current_time.replace(hour=0, minute=0, second=0, microsecond=0)
            logger.info(f"目前時間: {current_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            logger.info(f"比較用的日期: {current_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
            
            # 批量操作列表
            bulk_operations = []
            
            # 遍历所有商品
            for product in products_data:
                if 'tags' not in product or not product['tags']:
                    continue
                    
                # 只查找 RE2025 开头的标签
                resale_tags = [tag for tag in product['tags'] 
                             if tag.startswith('RE2025') and len(tag) >= 10]
                
                if not resale_tags:
                    continue
                
                # 提取補貨日期
                valid_resale_dates = []
                for tag in resale_tags:
                    try:
                        date_str = tag[2:]  # 提取日期部分 (YYYYMMDD)
                        year = int(date_str[:4])
                        month = int(date_str[4:6])
                        day = int(date_str[6:8])
                        resale_date = datetime(year, month, day, tzinfo=TW_TIMEZONE)
                        
                        logger.info(f"處理商品 '{product['name']}' 的標籤 '{tag}':")
                        logger.info(f"- 解析出的日期: {resale_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                        logger.info(f"- 比較結果: {resale_date >= current_date}")
                        
                        if resale_date >= current_date:
                            valid_resale_dates.append(resale_date)
                            logger.info("  => 此日期有效，已加入清單")
                        else:
                            logger.info("  => 此日期已過期，略過")
                            
                    except Exception as e:
                        logger.error(f"解析 RE 標籤日期失敗: {tag}, 錯誤: {str(e)}")
                
                if not valid_resale_dates:
                    continue
                
                # 获取最近的补货日期
                next_resale_date = min(valid_resale_dates)
                resale_tags_count += 1
                
                # 記錄找到的補貨商品資訊
                logger.info(f"找到補貨商品: {product['name']}")
                logger.info(f"補貨標籤: {resale_tags}")
                logger.info(f"下次補貨日期: {next_resale_date.strftime('%Y-%m-%d')}")
                
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
                    logger.info(f"批量更新補貨商品：matched={result.matched_count}, modified={result.modified_count}, upserted={result.upserted_count}")
                    bulk_operations = []
            
            # 执行剩余的批量操作
            if bulk_operations:
                result = self.resale.bulk_write(bulk_operations, ordered=False)
                logger.info(f"批量更新補貨商品：matched={result.matched_count}, modified={result.modified_count}, upserted={result.upserted_count}")
            
            logger.info(f"RE 標籤處理完成：發現 {resale_tags_count} 個補貨商品，耗時：{time.time() - start_time:.2f}秒")
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
            
            # 檢查今天是否已經記錄過這個商品
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
            
            # 如果是下架商品，先從 products 集合獲取原有的圖片 URL
            if type_ == 'delisted':
                existing_product = self.products.find_one({'url': product['url']})
                if existing_product and 'image_url' in existing_product:
                    history_data['image_url'] = existing_product['image_url']
                    logger.info(f"使用原有商品圖片 URL: {existing_product['image_url']}")
                else:
                    # 如果找不到原有圖片，使用默認圖片
                    history_data['image_url'] = 'https://chiikawamarket.jp/cdn/shop/files/chiikawa_logo_144x.png'
                    logger.info("找不到原有商品圖片，使用默認圖片")
            else:
                # 其他情況（如新上架）使用傳入的圖片 URL
                if 'image_url' in product:
                    history_data['image_url'] = product['image_url']
            
            # 如果是新上架商品
            if type_ == 'new':
                # 檢查商品是否之前存在於資料庫
                existing_product = self.products.find_one({'url': product['url']})
                
                # 檢查商品是否之前下架
                was_delisted = self.delisted.find_one({'url': product['url']})
                
                if was_delisted:
                    logger.info(f"商品重新上架: {product['name']}")
                    # 從下架集合中移除
                    self.delisted.delete_many({'url': product['url']})
                
                # 不管商品是新增還是重新上架，都添加到新上架集合
                new_data = history_data.copy()
                if isinstance(product, dict):
                    new_data.update({
                        'price': product.get('price', 0),
                        'available': product.get('available', False),
                        'tags': product.get('tags', []),
                        'is_restock': bool(was_delisted)  # 標記是否為重新上架
                    })
                
                # 寫入到新上架集合
                self.new.insert_one(new_data)
                logger.info(f"商品已添加到新上架集合: {product['name']} ({'重新上架' if was_delisted else '新商品'})")
                
                # 同時也要寫入到歷史記錄
                self.history.insert_one(history_data)
                
            elif type_ == 'delisted':
                # 寫入到下架集合
                self.delisted.insert_one(history_data)
                # 同時也要寫入到歷史記錄
                self.history.insert_one(history_data)
                logger.info(f"商品已添加到下架集合: {product['name']}")
            
            return True
            
        except Exception as e:
            logger.error(f"記錄歷史時發生錯誤：{str(e)}")
            logger.error(traceback.format_exc())
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

    def check_products_consistency(self):
        """檢查 products 集合中的數據一致性"""
        try:
            # 獲取所有商品
            all_products = list(self.products.find({}, {'url': 1, 'name': 1, 'last_seen': 1, '_id': 0}))
            total_products = len(all_products)
            
            # 檢查是否有重複的 URL
            urls = [p['url'] for p in all_products]
            unique_urls = set(urls)
            duplicate_urls = len(urls) - len(unique_urls)
            
            # 檢查最後更新時間超過7天的商品
            current_time = datetime.now(TW_TIMEZONE)
            seven_days_ago = current_time - timedelta(days=7)
            
            # 確保所有時間都轉換為台灣時區
            old_products = []
            for p in all_products:
                last_seen = self.ensure_timezone(p['last_seen'])
                if last_seen < seven_days_ago:
                    p['last_seen'] = last_seen  # 更新為台灣時區的時間
                    old_products.append(p)
            
            # 輸出檢查結果
            logger.info(f"\n=== Products 集合檢查結果 ===")
            logger.info(f"總商品數: {total_products}")
            logger.info(f"唯一 URL 數: {len(unique_urls)}")
            logger.info(f"重複 URL 數: {duplicate_urls}")
            logger.info(f"超過7天未更新的商品數: {len(old_products)}")
            
            if old_products:
                logger.info("\n超過7天未更新的商品列表:")
                for product in old_products:
                    days_old = (current_time - product['last_seen']).days
                    logger.info(f"- {product['name']} (最後更新: {days_old} 天前，時間: {product['last_seen'].strftime('%Y-%m-%d %H:%M:%S %Z')})")
            
            # 如果發現重複 URL，列出它們
            if duplicate_urls > 0:
                from collections import Counter
                url_counts = Counter(urls)
                duplicate_urls = {url: count for url, count in url_counts.items() if count > 1}
                logger.info("\n重複的 URL:")
                for url, count in duplicate_urls.items():
                    logger.info(f"- {url} (出現 {count} 次)")
            
            return {
                'total': total_products,
                'unique_urls': len(unique_urls),
                'duplicates': duplicate_urls,
                'old_products': len(old_products)
            }
            
        except Exception as e:
            logger.error(f"檢查 products 集合時發生錯誤: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def clean_products_collection(self):
        """清理 products 集合中的問題數據"""
        try:
            # 獲取當前時間
            current_time = datetime.now(TW_TIMEZONE)
            seven_days_ago = current_time - timedelta(days=7)
            
            # 刪除超過7天未更新的商品
            result = self.products.delete_many({
                'last_seen': {'$lt': seven_days_ago}
            })
            deleted_old = result.deleted_count
            
            # 檢查並修復重複的 URL
            pipeline = [
                {'$group': {
                    '_id': '$url',
                    'count': {'$sum': 1},
                    'docs': {'$push': '$_id'}
                }},
                {'$match': {
                    'count': {'$gt': 1}
                }}
            ]
            
            duplicates = list(self.products.aggregate(pipeline))
            deleted_duplicates = 0
            
            for dup in duplicates:
                # 保留最新的一條記錄，刪除其他的
                docs_to_delete = dup['docs'][1:]  # 保留第一個文檔
                result = self.products.delete_many({'_id': {'$in': docs_to_delete}})
                deleted_duplicates += result.deleted_count
            
            logger.info(f"\n=== Products 集合清理結果 ===")
            logger.info(f"刪除超過7天未更新的商品: {deleted_old} 個")
            logger.info(f"刪除重複的商品記錄: {deleted_duplicates} 個")
            
            return {
                'deleted_old': deleted_old,
                'deleted_duplicates': deleted_duplicates
            }
            
        except Exception as e:
            logger.error(f"清理 products 集合時發生錯誤: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    def ensure_timezone(self, dt):
        """确保时间对象带有时区信息并转换为台湾时区"""
        if dt is None:
            return datetime.now(TW_TIMEZONE)
            
        # 如果时间没有时区信息，假设是 UTC
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.UTC)
            
        # 将时间转换为台湾时区
        return dt.astimezone(TW_TIMEZONE)

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
