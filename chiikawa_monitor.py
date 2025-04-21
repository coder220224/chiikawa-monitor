import requests
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import os
import time
import random
import sqlite3
import json
import pandas as pd
from pymongo import MongoClient
from config import MONGODB_URI
import certifi

class ChiikawaMonitor:
    def __init__(self):
        self.base_url = "https://chiikawamarket.jp"
        self.work_dir = os.path.dirname(os.path.abspath(__file__))
        self.excel_path = os.path.join(self.work_dir, 'chiikawa_products.xlsx')
        
        # MongoDB 設置
        try:
            # 簡化連接選項
            self.client = MongoClient(
                MONGODB_URI,
                serverSelectionTimeoutMS=30000,
                connectTimeoutMS=30000,
                tls=True,
                tlsCAFile=certifi.where()  # 使用 certifi 提供的證書
            )
            
            # 測試連接
            self.client.admin.command('ping')
            print("MongoDB 連接成功！")
            
            self.db = self.client['chiikawa']
            self.products = self.db['products']
            self.history = self.db['history']
            
            # 建立索引
            self.products.create_index('url', unique=True)
            self.history.create_index([('date', 1), ('type', 1)])
            
        except Exception as e:
            print(f"MongoDB 連接錯誤: {str(e)}")
            raise

        # 設置請求頭
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "zh-TW,zh;q=0.9,en-US;q=0.8,en;q=0.7,ja;q=0.6",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://chiikawamarket.jp/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

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
            print(f"已更新 Excel 文件：{self.excel_path}")
            
            return True
        except Exception as e:
            print(f"更新 Excel 時發生錯誤：{str(e)}")
            return False

    def fetch_products(self):
        """獲取所有商品信息"""
        try:
            print("\n=== 開始獲取商品數據 ===")
            print(f"基礎 URL: {self.base_url}")
            
            # 測試基本連接
            try:
                print("\n1. 測試基礎連接...")
                test_response = self.session.get(self.base_url, timeout=30, verify=False)
                print(f"基礎連接狀態碼: {test_response.status_code}")
                print(f"響應頭: {dict(test_response.headers)}")
                
                if test_response.status_code != 200:
                    print(f"警告：基礎連接返回非 200 狀態碼")
                    print(f"響應內容: {test_response.text[:500]}")
                    return []
                    
            except requests.exceptions.RequestException as e:
                print(f"基礎連接測試失敗: {str(e)}")
                print(f"請求標頭: {self.headers}")
                return []
            
            # 測試 API 端點
            print("\n2. 測試商品 API...")
            api_url = f"{self.base_url}/zh-hant/products.json"
            print(f"API URL: {api_url}")
            
            try:
                print("發送 API 請求...")
                api_response = self.session.get(
                    api_url, 
                    params={'page': 1, 'limit': 1}, 
                    timeout=30,
                    verify=False
                )
                print(f"API 響應狀態碼: {api_response.status_code}")
                print(f"API 響應頭: {dict(api_response.headers)}")
                
                if api_response.status_code == 200:
                    try:
                        data = api_response.json()
                        print("成功解析 JSON 響應")
                        print(f"響應數據預覽: {str(data)[:200]}")
                        
                        if 'products' not in data:
                            print("錯誤：響應中沒有 products 字段")
                            return []
                            
                    except json.JSONDecodeError as e:
                        print(f"JSON 解析失敗: {str(e)}")
                        print(f"原始響應內容: {api_response.text[:500]}")
                        return []
                else:
                    print(f"API 請求失敗，狀態碼: {api_response.status_code}")
                    print(f"錯誤響應: {api_response.text[:500]}")
                    return []
                    
            except requests.exceptions.RequestException as e:
                print(f"API 請求失敗: {str(e)}")
                print(f"請求標頭: {self.headers}")
                return []
                
            # 開始獲取所有商品
            print("\n3. 開始獲取完整商品列表...")
            total_products = 0
            page = 1
            new_products_data = []
            seen_handles = set()
            
            while True:
                try:
                    print(f"\n獲取第 {page} 頁...")
                    response = self.session.get(
                        api_url,
                        params={'page': page, 'limit': 250},
                        timeout=30,
                        verify=False
                    )
                    
                    if response.status_code != 200:
                        print(f"獲取第 {page} 頁失敗，狀態碼: {response.status_code}")
                        print(f"錯誤響應: {response.text[:200]}")
                        break
                        
                    try:
                        data = response.json()
                    except json.JSONDecodeError as e:
                        print(f"解析第 {page} 頁 JSON 失敗: {str(e)}")
                        print(f"原始響應: {response.text[:200]}")
                        break
                        
                    if not isinstance(data, dict) or 'products' not in data:
                        print(f"第 {page} 頁數據格式錯誤")
                        print(f"響應數據: {str(data)[:200]}")
                        break
                        
                    products = data['products']
                    if not products:
                        print("沒有更多商品")
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
                                
                            product_url = f"{self.base_url}/zh-hant/products/{handle}"
                            new_products_data.append({
                                'url': product_url,
                                'name': title,
                                'price': price,
                                'available': available,
                                'last_seen': datetime.now()
                            })
                            
                            total_products += 1
                            page_count += 1
                            
                        except Exception as e:
                            print(f"處理商品時出錯: {str(e)}")
                            continue
                            
                    print(f"第 {page} 頁處理完成，獲取 {page_count} 個商品")
                    if page_count == 0:
                        break
                        
                    page += 1
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"處理第 {page} 頁時出錯: {str(e)}")
                    break
                
            print(f"\n=== 商品獲取完成 ===")
            print(f"總共獲取: {total_products} 個商品")
            return new_products_data
            
        except Exception as e:
            print(f"商品獲取過程中發生錯誤: {str(e)}")
            import traceback
            print(f"錯誤詳情:\n{traceback.format_exc()}")
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
            print(f"更新數據庫時發生錯誤：{str(e)}")
            return False

    def get_all_products(self):
        """獲取所有商品"""
        return list(self.products.find({}, {'_id': 0}))

    def record_history(self, product, type_):
        """記錄商品歷史"""
        try:
            history_data = {
                'date': datetime.now(),
                'type': type_,
                'name': product['name'],
                'url': product['url'],
                'time': datetime.now()
            }
            self.history.insert_one(history_data)
            return True
        except Exception as e:
            print(f"記錄歷史時發生錯誤：{str(e)}")
            return False

    def get_today_history(self, type_):
        """獲取今日的歷史記錄"""
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
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

if __name__ == "__main__":
    # 測試代碼
    monitor = ChiikawaMonitor()
    try:
        print("測試獲取商品...")
        total = monitor.fetch_products()
        print(f"共獲取到 {len(total)} 個商品")
        
        print("\n獲取所有商品...")
        products = monitor.get_all_products()
        for product in products[:5]:  # 只顯示前5個
            print(f"- {product['name']}")
            
    finally:
        monitor.close() 
