#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
TMY API 后端示例：基于 FastAPI 的 /tmy 路由实现
依赖: fastapi, uvicorn, pvlib, timezonefinder, tzdata, pandas
实现要点:
- 调用 PVGIS 的 TMY 数据（pvlib.iotools.get_pvgis_tmy）
- 根据经纬度自动推导时区并本地化时间
- 将结果整理为 {"meta": ..., "tz": "...", "records": [...]} 的结构
- 当 PVGIS 调用或数据处理出错时，返回带有 "error" 的字典，并通过 HTTP 404 反馈给前端
- 提供 /health 健康端点及 CORS 设置，便于部署和测试
"""

from __future__ import annotations

import os
from typing import Optional, Any, Dict

import pvlib
import pandas as pd  # 保留，若现有代码中有相关处理
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.encoders import jsonable_encoder
from zoneinfo import ZoneInfo
from timezonefinder import TimezoneFinder

# 全局时区查找器，提升性能
_tf = TimezoneFinder()

# FastAPI 应用实例
app = FastAPI(title="TMY API")

# CORS 设置（根据需要调整）
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def fnum(x: Any, default: float = 0.0) -> float:
    """
    安全地将值转换为 float，出错时返回默认值。
    """
    try:
        if x is None:
            return default
        return float(x)
    except (TypeError, ValueError):
        return default

def tmy(lat: float, lon: float, tzName: Optional[str] = None, gmtOffset: Optional[float] = None) -> Dict[str, Any]:
    """
    获取 PVGIS TMY 数据并做时区、本地化处理，返回统一的字典结构。
    如果发生错误，返回：
      {"error": "TMY fetch failed: <原因>", "tz": "UTC", "records": [], "meta": None}
    """
    # 0) 获取 PVGIS TMY 数据
    try:
        df, meta = pvlib.iotools.get_pvgis_tmy(lat, lon, map_variables=True)
    except Exception as e:
        return {
            "error": f"TMY fetch failed: {str(e)}",
            "tz": "UTC",
            "records": [],
            "meta": None
        }

    # 1) 将时间轴本地化为 UTC（PVGIS 输出通常为 UTC 时序）
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
        # 如果转换失败，回退到 UTC
        df = df.tz_convert("UTC")
        tz_used = ZoneInfo("UTC")
        tz_used_name = "UTC"

    # 4) 逐条记录构建输出
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

# 健康检查端点（可选，便于运维）
@app.get("/health")
def health():
    return {"status": "ok"}

# 脚本中的核心路由：暴露 /tmy
@app.get("/tmy")
def tmy_route(lat: float, lon: float, tzName: Optional[str] = None, gmtOffset: Optional[float] = None):
    """
    /tmy?lat=...&lon=...&tzName=...&gmtOffset=...
    返回 PVGIS TMY 数据，遇到错误时返回 404 与错误信息。
    """
    res = tmy(lat, lon, tzName, gmtOffset)
    if isinstance(res, dict) and res.get("error"):
        # 将后端错误以 404 HTTPException 返回给前端，便于定位
        raise HTTPException(status_code=404, detail=res["error"])
    return res

# 启动入口（本地测试用）
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=port, reload=True)