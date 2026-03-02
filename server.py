from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder

import pvlib
import pandas as pd
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo

_tf = TimezoneFinder()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

_tf = TimezoneFinder()

# -------------- 方案 A：DST-aware 的 Sydney 时区 (Australia/Sydney) --------------
def fnum(x) -> float:
    # 将 None/NaN 转为 0，避免 JSON 出 NaN
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0

def tmy(lat: float, lon: float):
    # 1) 获取 PVGIS TMY 数据
    df, meta = pvlib.iotools.get_pvgis_tmy(lat, lon, map_variables=True)

    # 2) 将时间轴本地化为 UTC（PVGIS 输出通常为 UTC 时序，若 df.index 已有时区则跳过）
    if df.index.tz is None:
        df = df.tz_localize("UTC")

    # 3) 根据坐标判定时区
    tz_name = _tf.timezone_at(lng=lon, lat=lat)
    if tz_name is None:
        # 无法定位时区，回退到 UTC
        tz_used = ZoneInfo("UTC")
        tz_used_name = "UTC"
    else:
        tz_used = ZoneInfo(tz_name)
        tz_used_name = tz_name

    # 4) 将时间转换到确定的本地时区
    try:
        df = df.tz_convert(tz_used)
    except Exception:
        # 转换失败，回退到 UTC
        df = df.tz_convert("UTC")
        tz_used = ZoneInfo("UTC")
        tz_used_name = "UTC"

    # 5) 构造输出记录：dayN 为本地日序数，hourN 为本地小时 (0-23)
    records = []
    for ts, row in df.iterrows():
        ts_local = ts  # 已经带时区信息
        dayN = int(ts_local.dayofyear)
        hourN = int(ts_local.hour)

        # 提取需要的字段，兜底为 0
        dni  = fnum(row.get("dni"))
        dhi  = fnum(row.get("dhi"))
        ghi  = fnum(row.get("ghi"))

        ta    = fnum(row.get("t2m", row.get("temp_air")))
        vwind = fnum(row.get("ws10m", row.get("wind_speed")))

        records.append({
            "dayN": dayN,
            "hourN": hourN,
            "dni": dni,
            "dhi": dhi,
            "ghi": ghi,
            "ta": ta,
            "vwind": vwind
        })

    return {
        "meta": jsonable_encoder(meta),
        "tz": str(tz_used_name),
        "records": records
    }