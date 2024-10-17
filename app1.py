from flask import Flask, request, jsonify
import datetime
from recommendation_system1 import RecommendationSystem1

class RestaurantAPI:
    def __init__(self, recommendation_system):
        self.recommendation_system = recommendation_system
        self.app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        @self.app.route('/recommend', methods=['POST'])
        def recommend():
            if request.content_type != 'application/json':
                return jsonify({'error': 'Content-Type must be application/json'}), 415

            data = request.json

            location = data.get('location')
            cuisine_type = data.get('category')
            price_range = data.get('price')
            dining_day = data.get('dining_day')
            dining_hour = data.get('dining_hour')
            user_preferences = data.get('user_preferences')

            if dining_day:
                try:
                    dining_day = int(dining_day)
                    if dining_day not in range(7):
                        raise ValueError("0-6 的數字")
                except ValueError as e:
                    return jsonify({'error': str(e)}), 400

                days_dict = {0: "日", 1: "一", 2: "二", 3: "三", 4: "四", 5: "五", 6: "六"}
                dining_day = days_dict[dining_day]
            else:
                dining_day = None

            if dining_hour:
                try:
                    datetime.datetime.strptime(dining_hour, "%H:%M")
                except ValueError:
                    return jsonify({'error': "用餐時間格式應為 HH:MM"}), 400
            else:
                dining_hour = None

            recommended_restaurants = self.recommendation_system.recommend_restaurants(
                location, cuisine_type, price_range, dining_day, dining_hour, user_preferences)

            if recommended_restaurants:
                response = [
                    {
                        'name': restaurant[1],
                        'address': restaurant[2],
                        'category': restaurant[3],
                        'price': restaurant[4],
                        'opening_time': restaurant[5],
                        'rating': restaurant[6],
                        'phone': restaurant[7]
                    }
                    for restaurant in recommended_restaurants
                ]
                return jsonify(response), 200
            else:
                return jsonify({'message': "抱歉，沒有找到符合條件的餐廳。"}), 404

    def run(self, host='0.0.0.0', port=5001):
        self.app.run(host=host, port=port)

if __name__ == '__main__':
    db_config = {
        'host': "",
        'user': "",
        'password': "",
        'database': "",
        'port': ""
    }
    jieba_dict_path = '/dict.txt.big'

    recommendation_system = RecommendationSystem1(db_config, jieba_dict_path)
    restaurant_api = RestaurantAPI(recommendation_system)
    restaurant_api.run()
