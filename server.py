from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pvlib

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/tmy")
def tmy(lat: float, lon: float):
    df, meta = pvlib.iotools.get_pvgis_tmy(lat, lon, map_variables=True)

    records = []
    for ts, row in df.iterrows():
        records.append({
            "dayN": int(ts.dayofyear),
            "hourN": int(ts.hour) + 1,
            "dni": float(row.get("dni", 0.0) or 0.0),
            "dhi": float(row.get("dhi", 0.0) or 0.0),
            "ta": float(row.get("temp_air", row.get("t2m", 0.0)) or 0.0),
            "vwind": float(row.get("wind_speed", row.get("ws10m", 0.0)) or 0.0),
        })

    return {"meta": meta, "records": records}