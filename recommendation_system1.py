import psycopg2
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel
import datetime
import jieba
import numpy as np

class RecommendationSystem1:
    def __init__(self, db_config, jieba_dict_path):
        self.conn = psycopg2.connect(**db_config)
        jieba.set_dictionary(jieba_dict_path)
        self.food_categories = {
            "美式": ["美式", "漢堡", "薯條", "炸雞", "熱狗", "牛排", "沙拉", "烤肋排", "墨西哥捲餅", "橙汁雞翅", "燻煙熏肉", "炸魚薯條", "牛趴"],
            "日式": ["日式", "壽司", "拉麵", "烏冬麵", "天婦羅", "味噌湯", "刺身", "焼き鳥", "おでん", "鰻重", "とんかつ", "おにぎり"],
            "中式": ["中式", "炒飯", "餃子", "北京烤鴨", "麻婆豆腐", "宮保雞丁", "火鍋", "豬肚飯", "冬瓜湯", "蚵仔煎", "乾麵", "臭豆腐"],
            "義式": ["義式", "比薩", "義大利麵", "提拉米蘇", "卡布奇諾", "意式冰淇淋", "義大利肉醬麵", "白醬披薩", "奶油焗蝦", "意大利麵包", "義大利餅乾"],
            "法式": ["法式", "法式長棍", "鵝肝", "焗蜗牛", "法式洋蔥湯", "馬卡龍", "法式乳酪", "法式杏仁酥", "法式榛子糕", "法國藍紋芝士", "馬卡龍餅"],
            "韓式": ["韓式", "烤肉", "泡菜", "石鍋拌飯", "韓式煎餅", "辣炒年糕", "韓式辣炒年糕", "韓式烤牛肉", "泡菜煎餅", "韓國炸雞"],
            "泰式": ["泰式", "冬陰功湯", "綠咖哩", "泰式炒河粉", "涼拌木瓜絲", "芒果糯米飯", "紅咖哩", "炒冷米粉", "泰式涼拌青木瓜", "炒辣肉碎", "香蕉椰奶飯"]
        }

    def __del__(self):
        if self.conn:
            self.conn.close()

    def parse_opening_time(self, opening_time):
        all_day_hours = []
        for day_hours in opening_time.split(", 星期"):
            if day_hours.strip():
                all_day_hours.append(day_hours.strip())
        return all_day_hours

    def is_open(self, hours_dict, day, time):
        for hours in hours_dict:
            the_day = hours[0]
            if day == the_day:
                if '休息' in hours:
                    continue
                else:
                    time_periods = [period.strip() for period in hours[1:].split(' ') if period.strip()]
                    for time_period in time_periods:
                        try:
                            if "–" in time_period:
                                start, end = time_period.split('–')
                            elif "-" in time_period:
                                start, end = time_period.split('-')
                            elif "24 小時營業" in time_period:
                                start = "00:00"
                                end = "23:59"
                            else:
                                print(f"Unexpected time format: {time_period}")
                                continue
                            start_time = datetime.datetime.strptime(start.strip(), "%H:%M")
                            end_time = datetime.datetime.strptime(end.strip(), "%H:%M")
                            if start_time <= time <= end_time:
                                return True
                            else:
                                continue
                        except ValueError as e:
                            print(f"Error parsing time period '{time_period}': {e}")
                            continue
        return False
    
    def price_in_range(self, price, user_price):
        if price is None:
            return False
        price = price.replace(',', '')
        if "-" in price:
            try:
                price_range = price.split('-')
                lower_bound = int(price_range[0])
                upper_bound = int(price_range[1])
                return lower_bound <= int(user_price) <= upper_bound
            except Exception as e:
                print(f"Error parsing price range '{price}': {e}")
                return False
        return False

    def get_restaurants(self):
        cur = self.conn.cursor()
        query = """
            SELECT id, name, address, category, price, opening_time, rating, phone
            FROM restaurant
        """
        cur.execute(query)
        restaurants = cur.fetchall()
        cur.close()
        return restaurants

    def tokenize(self, text):
        return ' '.join(jieba.cut(text))

    def get_category_items(self, cuisine_type):
        for category, items in self.food_categories.items():
            if cuisine_type in items:
                return items
        return []

    def enhance_with_user_preferences(self, recommended_restaurants, user_preferences, cuisine_type):
        preference_texts = [' '.join(user_preferences)]
        category_items = self.get_category_items(cuisine_type)
        category_texts = [' '.join(category_items)]
        cuisine_type_texts = [cuisine_type]

        vectorizer = TfidfVectorizer(stop_words=["料", "理", "餐", "廳", "式", "國"], tokenizer=self.tokenize)
        combined_texts = preference_texts + category_texts + cuisine_type_texts
        combined_matrix = vectorizer.fit_transform(combined_texts)

        user_preference_vector = combined_matrix[0]
        category_vector = combined_matrix[1]
        cuisine_type_vector = combined_matrix[2]

        preference_scores = []
        for restaurant in recommended_restaurants:
            restaurant_text = f"{restaurant[1]} {restaurant[3]}"
            restaurant_vector = vectorizer.transform([restaurant_text])
            cuisine_similarity = linear_kernel(cuisine_type_vector, restaurant_vector).flatten()[0]
            user_preference_similarity = linear_kernel(user_preference_vector, restaurant_vector).flatten()[0]
            category_similarity = linear_kernel(category_vector, restaurant_vector).flatten()[0]

            score = (cuisine_similarity * 0.6) + (user_preference_similarity * 0.2) + (category_similarity * 0.2)
            preference_scores.append(score)

        sorted_indices = np.argsort(preference_scores)[::-1]
        enhanced_recommendations = [recommended_restaurants[idx] for idx in sorted_indices]

        return enhanced_recommendations

    def recommend_restaurants(self, location, cuisine_type, price_range, dining_day, dining_hour, user_preferences):
        restaurants = self.get_restaurants()
        dining_time = datetime.datetime.strptime(dining_hour, "%H:%M")

        open_restaurants = []
        for restaurant in restaurants:
            if location.lower() not in restaurant[2].lower():
                continue
            if not self.price_in_range(restaurant[4], price_range):
                continue
            hours_dict = self.parse_opening_time(restaurant[5])
            if self.is_open(hours_dict, dining_day, dining_time):
                open_restaurants.append(restaurant)

        if not open_restaurants:
            return []

        enhanced_recommendations = self.enhance_with_user_preferences(open_restaurants, user_preferences, cuisine_type)

        return enhanced_recommendations
