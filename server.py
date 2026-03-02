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

    # 3) 使用 Sydney 时区（Australia/Sydney，DST-aware）
    from zoneinfo import ZoneInfo
    tz_sydney = ZoneInfo("Australia/Sydney")  # 会自动处理夏令时

    try:
        df = df.tz_convert(tz_sydney)
        tz_used = tz_sydney
    except Exception:
        # 回退保护：无法转换时使用 UTC
        df = df.tz_convert("UTC")
        tz_used = ZoneInfo("UTC")

    # 4) 构造输出记录：dayN 为本地日序数，hourN 为本地小时 (0-23)
    records = []
    for ts, row in df.iterrows():
        # ts 已经带时区，直接以本地时间处理
        ts_local = ts
        dayN = int(ts_local.dayofyear)
        hourN = int(ts_local.hour)  # 0-23

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
        "tz": str(tz_used),
        "records": records
    }