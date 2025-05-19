import time, json, pytz, os
from bs4 import BeautifulSoup
from datetime import datetime, time as dtime
from pymongo import MongoClient
from mega import Mega
import requests

# Environment variables
MONGO_URL = os.getenv("MONGO_URL")
M_TOKEN = os.getenv("M_TOKEN")

# Global client
client = None

# Ensure output folder exists
os.makedirs("json", exist_ok=True)

# Global error count
error_occured_count = 0

def report_error_to_server(error_message):
    global error_occured_count
    error_occured_count += 1

    error_message = f"FROM REPO - nse\n{'*' * 30}\n{str(error_message)}"

    try:
        url = 'https://pass-actions-status.vercel.app/report-error'
        headers = {'Content-Type': 'application/json'}
        data = {
            'error': error_message,
            'count': error_occured_count
        }
        requests.post(url, headers=headers, json=data)
    except Exception as report_ex:
        print("⚠️ Failed to report error:", report_ex)


def get_current_time(default_value=0):
    ist = pytz.timezone('Asia/Kolkata')
    if default_value == 1:
        return datetime.now(ist)
    return datetime.now(ist).strftime('%Y-%m-%d %H:%M:%S')


def is_after_3_35_pm():
    now = get_current_time(default_value=1)
    return now.time() > dtime(15, 35)


def is_market_hours():
    now = get_current_time(1)
    market_open = now.replace(hour=9, minute=15, second=0, microsecond=0)
    market_close = now.replace(hour=16, minute=0, microsecond=0)
    return now.weekday() < 5 and market_open <= now <= market_close  # weekday < 5 means Mon-Fri


def save_file_to_mega(m, file_name):
    try:
        m.upload(file_name)
        print(f"Uploaded {file_name} to MEGA.")
    except Exception as e:
        report_error_to_server(e)
        print("Error failed to upload:", e)


def save_collection_as_json():
    global client
    try:
        if client is None:
            client = MongoClient(MONGO_URL)

        db = client["OT_TRADING"]
        collection_names = db.list_collection_names()

        mega = Mega()
        keys = M_TOKEN.split("_")
        m = mega.login(keys[0], keys[1])

        collection_files = []
        time_stamp = get_current_time().replace(":", "-").replace(" ", "_")

        for name in collection_names:
            collection = db[name]
            data = list(collection.find({}, {'_id': False}))
            file_name = f"{name}_{time_stamp}.json"

            with open(file_name, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

            collection_files.append((file_name, collection))

        for file_name, collection in collection_files:
            save_file_to_mega(m, file_name)
            os.remove(file_name)
            collection.delete_many({})  # Clear after successful upload

    except Exception as e:
        report_error_to_server(e)
        print("Error in save_collection_as_json:", e)


def save_to_mongodb(index_name, index_json_data):
    global client
    try:
        if client is None:
            client = MongoClient(MONGO_URL)
        db = client["OT_TRADING"]
        collection = db[index_name]
        if index_json_data:
            collection.insert_one(index_json_data)
        else:
            print("⚠️ No data to insert.")
    except Exception as e:
        report_error_to_server(e)
        print(f"❌ MongoDB insertion failed for '{index_name}': {e}")


def get_nse_stocks():
    final_data = None
    url = "https://ow-scanx-analytics.dhan.co/customscan/fetchdt"

    headers = {
        "accept": "*/*",
        "content-type": "application/json; charset=UTF-8",
        "referer": "https://dhan.co/"
    }

    payload = {
        "data": {
            "sort": "Mcap",
            "sorder": "desc",
            "count": 2553,
            "params": [
                {"field": "OgInst", "op": "", "val": "ES"},
                {"field": "Exch", "op": "", "val": "NSE"}
            ],
            "fields": [
                "Isin", "DispSym", "Mcap", "Pe", "DivYeild", "Revenue", "Year1RevenueGrowth",
                "NetProfitMargin", "YoYLastQtrlyProfitGrowth", "EBIDTAMargin", "volume",
                "PricePerchng1year", "PricePerchng3year", "PricePerchng5year", "Ind_Pe", "Pb",
                "DivYeild", "Eps", "DaySMA50CurrentCandle", "DaySMA200CurrentCandle",
                "DayRSI14CurrentCandle", "ROCE", "Roe", "Sym", "PricePerchng1mon", "PricePerchng3mon"
            ],
            "pgno": 1
        }
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        final_data = response.json()
    else:
        msg = f"Failed to get response : {response.status_code}"
        report_error_to_server(msg)
        return

    try:
        mongo_data = {
            "timestamp": get_current_time(),
            "data": final_data
        }
        save_to_mongodb('nse-stocks-data', mongo_data)
    except Exception as e:
        report_error_to_server(e)
        print("Error failed to save:", e)

    if is_after_3_35_pm():
        save_collection_as_json()


def runner(max_attempts=3):
    attempt = 0
    while attempt < max_attempts:
        if not is_market_hours():
            print(f"[Instance] Market is closed. Stopping.")
            break
        try:
            get_nse_stocks()
            attempt = 0  # reset after success
            time.sleep(7)
        except Exception as e:
            report_error_to_server(e)
            attempt += 1
            if attempt < max_attempts:
                print(f"[Instance] Retrying in 5 seconds due to error: {e}")
                time.sleep(5)
            else:
                print(f"[Instance] ❌ All retry attempts failed.")


if __name__ == "__main__":
    try:
        runner()
    except Exception as e:
        report_error_to_server(e)
        print("❌ Fatal error in main block:", e)
