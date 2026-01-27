import os
import json
import time
import csv
import paho.mqtt.client as mqtt

# =========================
# MQTT 基本設定
# =========================
BROKER_IP = "169.254.11.110"
BROKER_PORT = 1883
TOPIC = "vrb/pcs/read"

# =========================
# CSV 設定
# =========================
LOG_FILE = "C:\\Users\\huiting\\PY\\MQTTgetValue\\PCS.csv"
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# =========================
# 全域狀態
# =========================
LOG_INTERVAL = 1.0

LAST_LOG_TIME = 0
LOGGING_ACTIVE = False

RUN_ID = 0
SAMPLE_INDEX = 0

HEADER = [
    "run_id",
    "sample_index",
    "PCS_BATTERY_POWER_30050",
    "PCS_INLET_AIR_TEMP_30060",
    "PCS_IGBT_MAX_TEMP_30062",
]

# =========================
# MQTT callbacks
# =========================
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("MQTT connected")
        client.subscribe(TOPIC)
    else:
        print("MQTT connect failed:", rc)


def on_message(client, userdata, msg):
    global LAST_LOG_TIME
    global LOGGING_ACTIVE, RUN_ID, SAMPLE_INDEX

    now = time.time()
    if now - LAST_LOG_TIME < LOG_INTERVAL:
        return
    LAST_LOG_TIME = int(now)

    try:
        payload = json.loads(msg.payload.decode("utf-8", errors="replace"))
    except json.JSONDecodeError:
        return

    PCS_REM_CMD = payload.get("PCS_REM_CMD_40030", 0)
    PCS_REM_P_SET = payload.get("PCS_REM_P_SET_40032", 0)

    try:
        PCS_REM_P_SET = float(PCS_REM_P_SET)
    except:
        PCS_REM_P_SET = 0

    # =========================
    # ▶ START logging
    # =========================
    if  PCS_REM_P_SET != 0:

        if not LOGGING_ACTIVE:
            RUN_ID += 1
            SAMPLE_INDEX = 0
            LOGGING_ACTIVE = True
            print(f"=== RUN {RUN_ID} START ===")

        SAMPLE_INDEX += 1

        row = {
            "run_id": RUN_ID,
            "sample_index": SAMPLE_INDEX,
            "PCS_BATTERY_POWER_30050": payload.get("PCS_BATTERY_POWER_30050"),
            "PCS_INLET_AIR_TEMP_30060": payload.get("PCS_INLET_AIR_TEMP_30060"),
            "PCS_IGBT_MAX_TEMP_30062": payload.get("PCS_IGBT_MAX_TEMP_30062"),
        }

    # =========================
    # ■ STOP logging
    # =========================
    else:
        if LOGGING_ACTIVE:
            print(f"=== RUN {RUN_ID} END ({SAMPLE_INDEX} samples) ===")
        LOGGING_ACTIVE = False
        return

    # =========================
    # Write CSV
    # =========================
    write_header = not os.path.exists(LOG_FILE)

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=HEADER)

        # 🔴 第一次建立檔案時
        if write_header:
            print("CSV HEADER =", HEADER)   # ✅ 只印一次
            writer.writeheader()

        writer.writerow(row)



# =========================
# main
# =========================
def main():
    print("Starting PCS MQTT logger...")

    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(BROKER_IP, BROKER_PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopping...")
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
