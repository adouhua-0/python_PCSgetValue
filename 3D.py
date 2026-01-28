import pandas as pd
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D

# ==============================
# 1. 讀取 CSV
# ==============================

csv_path = r"C:\Users\huiting\PY\MQTTgetValue\PCSlog.csv"

df = pd.read_csv(csv_path)

# 如果欄位有空白問題（很常見）
df.columns = df.columns.str.strip()

# ==============================
# 2. 取出三軸資料
# ==============================

# Z：時間
z = df["sample_index"]

# X：功率
x = df["PCS_BATTERY_POWER_30050"]

# Y：溫差（如果你CSV已經算好）
y = df["PCS_IGBT_MAX_TEMP_30062"] - df["PCS_INLET_AIR_TEMP_30060"]


# ==============================
# 3. 繪製 3D 圖
# ==============================

fig = plt.figure(figsize=(9,6))
ax = fig.add_subplot(111, projection="3d")

ax.scatter(x, y, z)

ax.set_xlabel("PCS_BATTERY_POWER_30050")
ax.set_ylabel("ΔT = IGBT - Inlet Air")
ax.set_zlabel("sample_index")

ax.set_title("PCS Thermal 3D Behavior Map")

plt.show()
