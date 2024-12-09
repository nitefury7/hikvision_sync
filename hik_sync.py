import requests
import json
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import os
import config


def setup_logging():
    logging.basicConfig(level=logging.INFO)
    success_handler = RotatingFileHandler(
        config.LOG_CONFIG["success_log"], mode="a", maxBytes=5 * 1024 * 1024, backupCount=5
    )
    failure_handler = RotatingFileHandler(
        config.LOG_CONFIG["failure_log"], mode="a", maxBytes=5 * 1024 * 1024, backupCount=5
    )

    success_handler.setLevel(logging.INFO)
    failure_handler.setLevel(logging.ERROR)

    success_logger = logging.getLogger("success_logger")
    failure_logger = logging.getLogger("failure_logger")

    success_logger.addHandler(success_handler)
    failure_logger.addHandler(failure_handler)

    return success_logger, failure_logger


success_logger, failure_logger = setup_logging()


def load_sent_timestamps():
    if not os.path.exists(config.LOG_CONFIG["sent_timestamps_log"]):
        return {}
    with open(config.LOG_CONFIG["sent_timestamps_log"], "r") as f:
        data = f.readlines()
    sent_timestamps = {}
    for line in data:
        employee_no, timestamp = line.strip().split(" ", 1)
        if employee_no not in sent_timestamps:
            sent_timestamps[employee_no] = set()
        sent_timestamps[employee_no].add(timestamp)
    return sent_timestamps


def save_sent_timestamp(employee_no, timestamp):
    with open(config.LOG_CONFIG["sent_timestamps_log"], "a") as f:
        f.write(f"{employee_no} {timestamp}\n")


def fetch_attendance_logs(start_time, end_time):
    url = f"http://{config.HIKVISION_DEVICE_CONFIG['device_ip']}{config.HIKVISION_DEVICE_CONFIG['attendance_endpoint']}"
    params = {
        "format": "json",
        "security": "1",
    }
    payload = {
        "SearchRecord": {
            "searchID": "AttendanceFetch",
            "searchResultPosition": 0,
            "maxResults": 100,
            "timeRange": {"startTime": start_time, "endTime": end_time},
        }
    }
    try:
        response = requests.post(
            url,
            params=params,
            auth=(
                config.HIKVISION_DEVICE_CONFIG["username"],
                config.HIKVISION_DEVICE_CONFIG["password"],
            ),
            json=payload,
            timeout=10,
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        failure_logger.error(f"Failed to fetch logs: {e}")
        return None


def send_to_erpnext(employee_field_value, timestamp, device_id, log_type):
    url = f"{config.ERPNEXT_CONFIG['base_url']}/api/method/hrms.hr.doctype.employee_checkin.employee_checkin.add_log_based_on_employee_field"
    headers = {
        "Authorization": f"token {config.ERPNEXT_CONFIG['api_key']}:{config.ERPNEXT_CONFIG['api_secret']}",
        "Content-Type": "application/json",
    }
    payload = {
        "employee_field_value": employee_field_value,
        "timestamp": timestamp,
        "device_id": device_id,
        "log_type": log_type,
    }
    try:
        response = requests.post(url, headers=headers, json=payload)
        response.raise_for_status()
        success_logger.info(f"Successfully pushed log: {payload}")
        return True
    except Exception as e:
        failure_logger.error(f"Failed to push log {payload}: {e}")
        return False


def fetch_all_shift_types():
    url = f"{config.ERPNEXT_CONFIG['base_url']}/api/resource/Shift Type"
    headers = {
        "Authorization": f"token {config.ERPNEXT_CONFIG['api_key']}:{config.ERPNEXT_CONFIG['api_secret']}",
        "Content-Type": "application/json",
    }
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        shift_types = response.json().get("data", [])
        return [shift_type["name"] for shift_type in shift_types]
    except Exception as e:
        failure_logger.error(f"Failed to fetch shift types: {e}")
        return []


def update_last_sync_time(shift_type_name, sync_timestamp):
    url = f"{config.ERPNEXT_CONFIG['base_url']}/api/resource/Shift Type/{shift_type_name}"
    headers = {
        "Authorization": f"token {config.ERPNEXT_CONFIG['api_key']}:{config.ERPNEXT_CONFIG['api_secret']}",
        "Content-Type": "application/json",
    }
    payload = {
        "last_sync_of_checkin": sync_timestamp
    }
    try:
        response = requests.put(url, headers=headers, json=payload)
        response.raise_for_status()
        success_logger.info(f"Updated shift type sync: {shift_type_name} to {sync_timestamp}")
    except Exception as e:
        failure_logger.error(f"Failed to update shift type sync for {shift_type_name}: {e}")

def process_logs(logs, device_id, sent_timestamps):
    for employee in logs:
        employee_no = employee.get("employeeNo")
        if not employee_no:
            continue

        if employee_no not in sent_timestamps:
            sent_timestamps[employee_no] = set()

        for detail in employee.get("detailInfo", []):
            date = detail.get("dateTime")
            for time in detail.get("timeList", []):
                timestamp = f"{date} {convert_minutes_to_time(time)}"
                if timestamp not in sent_timestamps[employee_no]:
                    log_type = "IN"
                    if send_to_erpnext(employee_no, timestamp, device_id, log_type):
                        sent_timestamps[employee_no].add(timestamp)
                        save_sent_timestamp(employee_no, timestamp)

    sync_timestamp = datetime.now().strftime(config.DATE_FORMAT)
    shift_types = fetch_all_shift_types()
    for shift_type in shift_types:
        update_last_sync_time(shift_type, sync_timestamp)


def convert_minutes_to_time(minutes):
    hours = minutes // 60
    minutes = minutes % 60
    return f"{hours:02}:{minutes:02}:00"

def fetch_logs_from_json(file_path):
    try:
        with open(file_path, "r") as file:
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"Error: The file {file_path} does not exist.")
        return None
    except json.JSONDecodeError:
        print("Error: The JSON file is not properly formatted.")
        return None

def main():
    sent_timestamps = load_sent_timestamps()
    end_time = datetime.now()
    start_time = end_time - timedelta(minutes=config.FETCH_INTERVAL)

    while True:
        logs = fetch_attendance_logs(
            start_time.strftime(config.DATE_FORMAT), end_time.strftime(config.DATE_FORMAT)
        )
        
        if logs:
            process_logs(logs, config.HIKVISION_DEVICE_CONFIG["device_ip"], sent_timestamps)

        start_time = end_time
        end_time = datetime.now()


if __name__ == "__main__":
    main()
