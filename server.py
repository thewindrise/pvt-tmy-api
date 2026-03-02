from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder

import pvlib
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder
import pandas as pd  # 如还使用到 Timedelta，请保留导入

_tf = TimezoneFinder()  # 全局实例，提升性能
def fnum(x, default=0.0):
    try:
        if x is None:
            return default
        return float(x)
    except (TypeError, ValueError):
        return default

def tmy(lat: float, lon: float):
    # 0) 获取 PVGIS TMY 数据
    try:
        df, meta = pvlib.iotools.get_pvgis_tmy(lat, lon, map_variables=True)
    except Exception as e:
        # 这里保留一个简单的错误返回，避免直接崩溃
        return {
            "error": f"TMY fetch failed: {str(e)}",
            "tz": "UTC",
            "records": [],
            "meta": None
        }

    # 1) 将时间轴本地化为 UTC（PVGIS 输出通常为 UTC 时序，若 df.index 已有时区则跳过）
    if df.index.tz is None:
        df = df.tz_localize("UTC")

    # 2) 根据坐标判定时区
    tz_name = _tf.timezone_at(lng=lon, lat=lat)
    if tz_name is None:
        tz_used = ZoneInfo("UTC")
        tz_used_name = "UTC"
    else:
        tz_used = ZoneInfo(tz_name)
        tz_used_name = tz_name

    # 3) 将时间转换到确定的本地时区
    try:
        df = df.tz_convert(tz_used)
    except Exception:
        df = df.tz_convert("UTC")
        tz_used = ZoneInfo("UTC")
        tz_used_name = "UTC"

    # 4) 不进行任何额外的小时偏移，直接使用转换后的本地时间
    records = []
    for ts, row in df.iterrows():
        ts_local = ts  # 已经带时区信息，表示本地时间
        dayN = int(ts_local.dayofyear)
        hourN = int(ts_local.hour)

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