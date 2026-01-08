import json
from datetime import datetime
import paho.mqtt.client as mqtt
import os
import time
import csv

# =========================
# MQTT 基本設定
# =========================
BROKER_IP = "169.254.11.110"
BROKER_PORT = 1883
TOPIC = "vrb/pcs/read"

# =========================
# Log 檔案設定（一定要是檔案）
# =========================
LOG_FILE = "/usr/plc/PCSlog.csv"

# 若目錄不存在，自動建立（工程必備）
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

HEADER_PRINTED = False

# =========================
# PCS 資料解析
# =========================
def get_PCS_Value(msg):

    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        print("Invalid JSON")
        return None


    # ---------- 2. 先讀控制指令 ----------
    PCS_REM_P_SET = payload.get("PCS_REM_P_SET_40032")

    try:
        PCS_REM_P_SET = float(PCS_REM_P_SET)
    except (TypeError, ValueError):
        return None

    # ---------- 3. 若指令為 0，不處理 ----------
    if PCS_REM_P_SET == 0:
        return None

    # ---------- 4. 時間戳記 ----------
    timestamp = datetime.now().isoformat(timespec="seconds")

    # ---------- 5. 抓即時資料 ----------
    data = {
        "timestamp": timestamp,
        "PCS_LOC_P_SET_40004": PCS_REM_P_SET,
        "PCS_REAL_P_SET_30000": payload.get("PCS_REAL_P_SET_30000"),
        "PCS_ACTIVE_POWER_30044": payload.get("PCS_ACTIVE_POWER_30044"),
        "PCS_BATTERY_CURR_30048": payload.get("PCS_BATTERY_CURR_30048"),
        "PCS_BATTERY_VOLT_30049": payload.get("PCS_BATTERY_VOLT_30049"),
        "PCS_BATTERY_POWER_30050": payload.get("PCS_BATTERY_POWER_30050"),
        "PCS_INLET_AIR_TEMP_30060": payload.get("PCS_INLET_AIR_TEMP_30060"),
        "PCS_OUTLET_AIR_TEMP_30061": payload.get("PCS_OUTLET_AIR_TEMP_30061"),
        "PCS_IGBT_MAX_TEMP_30062": payload.get("PCS_IGBT_MAX_TEMP_30062"),
    }

    return data


# =========================
# MQTT Callback
# =========================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        client.subscribe(TOPIC)
        print(f"Subscribed to {TOPIC}")
    else:
        print(f"MQTT connection failed, rc={rc}")


def on_message(client, userdata, msg):
    global HEADER_PRINTED

    pcs_data = get_PCS_Value(msg)
    if pcs_data is None:
        return

    try:
        with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=pcs_data.keys())

            # 🔴 只會進來一次
            if not HEADER_PRINTED:
                # 1️⃣ 印出變數名稱（只一次）
                print("CSV HEADER =", list(pcs_data.keys()))

                # 2️⃣ 寫 header（只一次）
                writer.writeheader()

                # 3️⃣ 設 flag
                HEADER_PRINTED = True

            # 4️⃣ 每筆都寫資料
            writer.writerow(pcs_data)

        print("CSV write OK")

    except OSError as e:
        print(f"File write error: {e}")



# =========================
# 主程式
# =========================
def main():
    print("Starting PCS MQTT logger...")

    client = mqtt.Client()

    client.on_connect = on_connect
    client.on_message = on_message

    # keepalive 60 秒
    client.connect(BROKER_IP, BROKER_PORT, keepalive=60)

    # 非阻塞 loop（可處理 reconnect）
    client.loop_start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping MQTT logger...")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
