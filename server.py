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

def tmy(
    lat: float,
    lon: float,
    tzName: Optional[str] = None,
    gmtOffset: Optional[float] = None,
    rotate_last_n_day1: int = 0  # 新增：从 day1 的结尾取 N 条挪到开头
) -> Dict[str, Any]:
    """
    获取 PVGIS TMY 数据并做时区、本地化处理，返回统一的字典结构。
    额外参数：
      - rotate_last_n_day1: 如果大于 0，将 day1 的最后 N 条记录挪到 CSV 开头，并重新计算 hourN。
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

    # 1) 将时间轴本地化为 UTC
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

    # 4) 逐条记录构建输出
    records = []
    prev_dayN = None
    hour_counter = 0

    for ts, row in df.iterrows():
        ts_local = ts
        dayN = int(ts_local.dayofyear)

        if dayN != prev_dayN:
            hour_counter = 1
            prev_dayN = dayN
        else:
            hour_counter += 1

        # 初始的 hourN
        hourN = int(hour_counter)

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

    # 旋转逻辑：将 day1 的最后 N 条挪到开头
    if rotate_last_n_day1 and rotate_last_n_day1 > 0:
        # 找到 day1 的记录范围
        m = 0
        for idx, rec in enumerate(records):
            if rec["dayN"] != 1:
                m = idx
                break
        if m == 0:
            m = len(records)  # 整个数据都在 day1 的情况
        n = min(rotate_last_n_day1, m)
        if n > 0:
            lastN = records[m - n : m]
            firstPart = records[: m - n]
            rotated = lastN + firstPart + records[m:]
            records = rotated

            # 重新按日重新分配 hourN
            new_records = []
            current_day = None
            counter = 0
            for r in records:
                if r["dayN"] != current_day:
                    current_day = r["dayN"]
                    counter = 1
                else:
                    counter += 1
                r["hourN"] = int(counter)
                new_records.append(r)
            records = new_records

    return {
        "meta": jsonable_encoder(meta),
        "tz": str(tz_used_name),
        "records": records
    }