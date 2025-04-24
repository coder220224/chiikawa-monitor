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
import brotli
import pytz

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
        
        # Reurl.cc API setup
        self.reurl_api_key = "4070ff49d794e73010563b663c974755ecd6bf31959304df8a38b58d6516556389"
        self.reurl_api_url = "https://api.reurl.cc"
        
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
            self.products = self.db['products']
            self.history = self.db['history']
            
            # 建立索引
            self.products.create_index('url', unique=True)
            self.history.create_index([('date', 1), ('type', 1)])
            
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

    def shorten_url_with_reurl(self, long_url):
        """使用 reurl.cc API 縮短 URL"""
        try:
            headers = {
                'Content-Type': 'application/json',
                'reurl-api-key': self.reurl_api_key
            }
            
            # 只包含必要的參數
            payload = {
                'url': long_url
            }
            
            response = requests.post(
                f"{self.reurl_api_url}/shorten",
                headers=headers,
                json=payload,
                timeout=10
            )
            
            logger.info(f"Reurl.cc API response status: {response.status_code}")
            
            if response.status_code == 200:
                try:
                    data = response.json()
                    if 'short_url' in data:
                        logger.info(f"Successfully shortened URL: {data['short_url']}")
                        return data['short_url']
                    else:
                        logger.error(f"No short_url in response: {data}")
                except json.JSONDecodeError as e:
                    logger.error(f"Failed to parse JSON response: {e}")
            else:
                logger.error(f"Reurl.cc API error: {response.status_code}")
            
            return long_url
            
        except Exception as e:
            logger.error(f"Error shortening URL with Reurl.cc: {str(e)}")
            return long_url

    def shorten_image_url(self, original_url):
        """縮短圖片 URL"""
        try:
            # 如果原始URL為空，返回空
            if not original_url:
                return original_url
            
            # 如果是 Shopify URL，先優化它
            if 'cdn.shopify.com' in original_url:
                optimized_url = f"{original_url.split('?')[0]}?width=100&height=100"
                # 使用 reurl.cc 縮短優化後的 URL
                return self.shorten_url_with_reurl(optimized_url)
            
            # 對於其他 URL，直接使用 reurl.cc 縮短
            return self.shorten_url_with_reurl(original_url)
            
        except Exception as e:
            logger.error(f"Error in shorten_image_url: {str(e)}")
            return original_url

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
                            
                            # 获取商品图片URL并上传到Imgur
                            image_url = None
                            if 'images' in product and product['images']:
                                image = product['images'][0]
                                if isinstance(image, dict) and 'src' in image:
                                    original_url = image['src']
                                    image_url = self.shorten_image_url(original_url)
                                    logger.info(f"圖片 URL 已轉換: {original_url} -> {image_url}")
                                elif isinstance(image, str):
                                    image_url = self.shorten_image_url(image)
                            
                            price = 0
                            available = False
                            if variants:
                                variant = variants[0]
                                price = int(float(variant.get('price', 0)))
                                available = variant.get('available', False)
                                
                            product_url = f"{self.base_url}/zh-hant/products/{handle}"
                            new_products_data.append({
                                'url': product_url,
                                'name': title,
                                'price': price,
                                'available': available,
                                'image_url': image_url,
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
        """更新數據庫中的商品資料"""
        try:
            # 清空現有資料
            self.products.delete_many({})
            
            # 插入新資料
            if products_data:
                self.products.insert_many(products_data)
            return True
        except Exception as e:
            logger.error(f"更新數據庫時發生錯誤：{str(e)}")
            return False

    def get_all_products(self):
        """獲取所有商品"""
        return list(self.products.find({}, {'_id': 0}))

    def record_history(self, product, type_):
        """記錄商品歷史"""
        try:
            history_data = {
                'date': datetime.now(TW_TIMEZONE),
                'type': type_,
                'name': product['name'],
                'url': product['url'],
                'image_url': product.get('image_url'),  # 添加图片URL
                'time': datetime.now(TW_TIMEZONE)
            }
            self.history.insert_one(history_data)
            
            # 如果有圖片 URL，確保使用短 URL
            if 'image_url' in product:
                product['image_url'] = self.shorten_image_url(product['image_url'])
            
            return True
        except Exception as e:
            logger.error(f"記錄歷史時發生錯誤：{str(e)}")
            return False

    def get_today_history(self, type_):
        """獲取今日的歷史記錄"""
        today = datetime.now(TW_TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
        query = {
            'date': {'$gte': today},
            'type': type_
        }
        return list(self.history.find(query, {'_id': 0}))

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
