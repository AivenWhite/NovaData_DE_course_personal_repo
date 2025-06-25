import json
import os
from pprint import pprint
from pymongo import MongoClient
from datetime import datetime, timedelta

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27018/")
db = client["my_database"]
collection = db["user_events"]
archived_collection = db["archived_users"]

now = datetime.now()
thirty_days_ago = now - timedelta(days=30)
fourteen_days_ago = now - timedelta(days=14)

pipeline = [
    {
        "$group": {
            "_id": "$user_id",
            "registration_date": {"$min": "$user_info.registration_date"},
            "last_activity_date": {"$max": "$event_time"},
            "events": {"$push": "$$ROOT"}
        }
    },
    {
        "$match": {
            "registration_date": {"$lt": thirty_days_ago},
            "last_activity_date": {"$lt": fourteen_days_ago}
        }
    }
]

result = list(collection.aggregate(pipeline))

user_ids = []

for user_group in result:
    user_id = user_group["_id"]
    user_ids.append(user_group["_id"])
    events = user_group["events"]
    archived_collection.insert_many(events)
    collection.delete_many({"user_id": user_id})

print(f"Перемещено в архив {len(result)} пользователей.")


# Создаем отчет
report = {
    "date": now.strftime("%Y-%m-%d"),
    "archived_users_count": len(result),
    "archived_user_ids": user_ids
}

report_dir = "mongo_reports"
if not os.path.exists(report_dir):
    os.makedirs(report_dir)

# Сохраняем отчет в файл JSON
if len(user_ids) == 0:
    print("Нет пользователей для архивирования.")
else:
    report_filename = os.path.join(f"{report_dir, now.strftime('%Y-%m-%d')}.json")
    with open(report_filename, 'w') as report_file:
        json.dump(report, report_file, indent=4)
    print(f"Архивировано {len(user_ids)} пользователей. Отчет сохранен в {report_filename}.")