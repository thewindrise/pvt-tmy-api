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

    # 2) 找到该经纬度的本地时区
    tzname = tf.timezone_at(lat=lat, lng=lon) or "UTC"
    tz = ZoneInfo(tzname)

    # 3) PVGIS TMY 常见为 UTC 时序；确保有本地时区后再转本地
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    df = df.tz_convert(tz)

    # 4) 构造输出记录
    records = []
    for ts, row in df.iterrows():
        # 将时间戳转换为本地时间（确保 dayN/hourN 与本地日历对齐，尤其 DST 情况）
        ts_local = ts.tz_convert(tz)

        # dayN 使用本地日期的日序数
        dayN = int(ts_local.dayofyear)
        # hourN 使用本地小时，范围 0-23
        hourN = int(ts_local.hour)

        # 读取可能存在的字段，若不存在则给 0
        dni  = fnum(row.get("dni"))
        dhi  = fnum(row.get("dhi"))
        ghi  = fnum(row.get("ghi"))  # 可选，若存在就用
        ta   = fnum(row.get("temp_air", row.get("t2m")))
        vwind = fnum(row.get("wind_speed", row.get("ws10m")))
        pv_kWh = fnum(row.get("pv_kWh", 0.0))
        th_kWh = fnum(row.get("th_kWh", 0.0))
        totalFlow_l = fnum(row.get("totalFlow_l", 0.0))
        Tout_C = fnum(row.get("Tout_C", 0.0))

        records.append({
            "dayN": dayN,
            "hourN": hourN,
            "dni": dni,
            "dhi": dhi,
            "ghi": ghi,
            "pv_kWh": pv_kWh,
            "th_kWh": th_kWh,
            "totalFlow_l": totalFlow_l,
            "Tout_C": Tout_C,
            "ta": ta,
            "vwind": vwind
        })

    return {"meta": jsonable_encoder(meta), "tz": tzname, "records": records}