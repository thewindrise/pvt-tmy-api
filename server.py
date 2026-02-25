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
    df, meta = pvlib.iotools.get_pvgis_tmy(lat, lon, map_variables=True)

    # 1) 找到该经纬度的本地时区
    tzname = tf.timezone_at(lat=lat, lng=lon) or "UTC"
    tz = ZoneInfo(tzname)

    # 2) PVGIS TMY 很常见是 UTC；确保有 tz 后转成本地
    if df.index.tz is None:
        df = df.tz_localize("UTC")
    df = df.tz_convert(tz)

    records = []
    for ts, row in df.iterrows():
        records.append({
            "dayN": int(ts.dayofyear),
            "hourN": int(ts.hour) + 1,  # 1..24（你前端会再减 1 变 0..23）
            "dni": fnum(row.get("dni")),
            "dhi": fnum(row.get("dhi")),
            "ta":  fnum(row.get("temp_air", row.get("t2m"))),
            "vwind": fnum(row.get("wind_speed", row.get("ws10m"))),
        })

    return {"meta": jsonable_encoder(meta), "tz": tzname, "records": records}