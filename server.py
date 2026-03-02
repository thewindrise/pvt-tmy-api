from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder

import pvlib
import pandas as pd
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

tf = TimezoneFinder()

def fnum(x) -> float:
    # 把 None/NaN 都变成 0，避免 JSON 出 NaN
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0.0
    try:
        return float(x)
    except Exception:
        return 0.0

@app.get("/tmy")
def tmy(lat: float, lon: float):
    # 1) 获取 PVGIS TMY 数据
    df, meta = pvlib.iotools.get_pvgis_tmy(lat, lon, map_variables=True)

    # 2) 找到该经纬度的时区信息（优先元数据 TZ，其次经纬度推测）
    tz_from_meta = None
    try:
        # meta 有时是 dict，有时是对象，请兼容性处理
        if isinstance(meta, dict) and 'TZ' in meta:
            tz_from_meta = float(meta['TZ'])
        elif hasattr(meta, 'TZ'):
            tz_from_meta = float(getattr(meta, 'TZ'))
    except Exception:
        tz_from_meta = None

    # 3) 本地化时间
    # PVGIS 数据通常是 UTC 时序；若 df.index 没有时区，需要本地化到 UTC
    if df.index.tz is None:
        df = df.tz_localize("UTC")

    # 4) 根据可用信息选择本地化的时区
    if tz_from_meta is not None:
        # 使用固定偏移本地时区，例如 TZ=-9.0 -> UTC-9
        from datetime import timezone, timedelta
        fixed_tz = timezone(timedelta(hours=tz_from_meta))
        df = df.tz_convert(fixed_tz)  # 将时间轴转到固定偏移时区
        tz_used = fixed_tz
    else:
        # 尝试使用命名时区（如 America/Los_Angeles），包含 DST
        tzname = tf.timezone_at(lat=lat, lng=lon) or "UTC"
        try:
            df = df.tz_convert(ZoneInfo(tzname))
            tz_used = ZoneInfo(tzname)
        except Exception:
            # 回退到 UTC
            df = df.tz_convert("UTC")
            tz_used = ZoneInfo("UTC")

    # 5) 构造输出记录：dayN 为本地日序数，hourN 为本地小时 (0-23)
    records = []
    for ts, row in df.iterrows():
        # 已经是带时区的时间戳，直接用本地时间字段
        ts_local = ts
        dayN = int(ts_local.dayofyear)
        hourN = int(ts_local.hour)  # 0-23

        # 取出需要的字段，兜底为 0
        dni  = fnum(row.get("dni"))
        dhi  = fnum(row.get("dhi"))
        ghi  = fnum(row.get("ghi"))

        # 其他字段按需保留
        ta = fnum(row.get("temp_air", row.get("t2m")))
        vwind = fnum(row.get("wind_speed", row.get("ws10m")))

        records.append({
            "dayN": dayN,
            "hourN": hourN,
            "dni": dni,
            "dhi": dhi,
            "ghi": ghi,
            "ta": ta,
            "vwind": vwind
        })

    return {"meta": jsonable_encoder(meta), "tz": str(tz_used) if isinstance(tz_used, ZoneInfo) else tz_used, "records": records}