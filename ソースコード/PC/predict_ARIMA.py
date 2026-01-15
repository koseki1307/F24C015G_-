#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
syuron_predict_pressure_fixed_threshold_with_replacement.py

要件:
- 予測テーブルは1つ: syuron_pressure_forecast
- テーブル構造は最小:
    base_time, target_time, yhat, residual, is_anomaly
- 閾値は固定 (TH_LO, TH_HI)。閾値情報はDBに保存しない
- 異常の置換系は残す:
    - master(=同テーブル)の異常履歴を参照して、学習系列の異常近傍を置換
    - resid_pool により eps を生成（不足時はフォールバック正規ノイズ）
    - レジーム判定 (連続片側異常 >= REGIME_K) の場合は置換スキップ

動作:
- MQTTで (sensor/dt, sensor/pres) を受信し、1分境界の arrival_min を確定
- arrival_min の実測が来た時:
    1) sensor_data_minute_YYYYMMDD に実測を保存
    2) target_time=arrival_min の予測がDBにあれば residual/is_anomaly 更新
    3) 直近 WINDOW_MINUTES の実測系列を取得し、置換込み学習系列を作りARIMA fit
    4) base=arrival_min の t+FORECAST_AHEAD_MIN を予測し、DB保存（ON DUPLICATEで上書き）
"""

import os
import logging
from typing import Optional, List, Tuple, Set
from datetime import datetime, timedelta
from collections import deque

import numpy as np
import pandas as pd
import paho.mqtt.client as mqtt
import pymysql
from pymysql.cursors import DictCursor
from pymysql import err as pymysql_err
from dateutil import tz
from statsmodels.tsa.arima.model import ARIMA
from numpy.random import default_rng

# ============================== ロガー ==============================
logging.basicConfig(
    level=getattr(logging, os.getenv("LOG_LEVEL", "INFO")),
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ============================== TZ ==============================
JST = tz.gettz(os.getenv("TZ", "Asia/Tokyo"))

# ============================== MQTT ==============================
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC_DT   = os.getenv("MQTT_TOPIC_DT", "sensor/dt")
MQTT_TOPIC_PRES = os.getenv("MQTT_TOPIC_PRES", "sensor/pres")

# ============================== DB ==============================
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "****")
DB_USER = os.getenv("DB_USER", "****")
DB_PASS = os.getenv("DB_PASS", "****")  

FORECAST_TABLE = os.getenv("FORECAST_TABLE", "syuron_pressure_forecast")

# 分データ（既存のあなたの構成）
SENSOR_TABLE_PREFIX = "sensor_data_minute_"
DATETIME_COL = "datetime"   # DATETIME(3)
PRESSURE_COL = "pressure"   # DECIMAL(6,2)

# ============================== ARIMA ==============================
ARIMA_ORDER = (
    int(os.getenv("ARIMA_P", "2")),
    int(os.getenv("ARIMA_D", "1")),
    int(os.getenv("ARIMA_Q", "2")),
)
FORECAST_AHEAD_MIN = int(os.getenv("FORECAST_AHEAD_MIN", "5"))
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", "1440"))

# ============================== 固定閾値 ==============================
TH_HI = float(os.getenv("TH_HI", "0.2937"))
TH_LO = float(os.getenv("TH_LO", "-0.2967"))

# ============================== 置換 & レジーム ==============================
REGIME_K = int(os.getenv("REGIME_K_MINUTES", "10"))            # 連続片側異常 >= K でレジーム扱い
REPLACE_RADIUS_MIN = int(os.getenv("REPLACE_RADIUS_MIN", "3")) # 異常中心±R分を置換
RESID_EXCLUDE_RADIUS_MIN = int(os.getenv("RESID_EXCLUDE_RADIUS_MIN", "5"))  # pool更新から除外する半径

# ============================== 残差プール（置換ノイズ源） ==============================
RESID_POOL_MAX = int(os.getenv("RESID_POOL_MAX", "5000"))
RESID_POOL_MIN_FOR_SAMPLE = int(os.getenv("RESID_POOL_MIN_FOR_SAMPLE", "200"))
RESID_CLIP_Q = float(os.getenv("RESID_CLIP_Q", "0.95"))
RESID_WARMUP_DROP = int(os.getenv("RESID_WARMUP_DROP", "20"))
NOISE_SCALE = float(os.getenv("NOISE_SCALE", "1.0"))

# 鮮度ガード（遅延データを捨てる）
MAX_ACCEPTABLE_LAG_MIN = float(os.getenv("MAX_ACCEPTABLE_LAG_MIN", "100"))

# MQTTペアリング
_last_dt_min: Optional[datetime] = None
_last_pres_value: Optional[float] = None

# RNG / pool
_rng = default_rng()
_resid_pool = deque(maxlen=RESID_POOL_MAX)


# ============================== DB helpers ==============================
def db_connect():
    return pymysql.connect(
        host=DB_HOST, user=DB_USER, password=DB_PASS, database=DB_NAME,
        cursorclass=DictCursor, autocommit=True, charset="utf8mb4"
    )

def _retry_once(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        logger.warning(f"DB op failed once, retrying: {e}")
        return fn(*args, **kwargs)

def _to_naive_jst(dt_aw: datetime) -> datetime:
    if dt_aw.tzinfo is None:
        return dt_aw
    return dt_aw.astimezone(JST).replace(tzinfo=None)

def _fmt_dt_sec(dt_aw: datetime) -> str:
    return _to_naive_jst(dt_aw).strftime("%Y-%m-%d %H:%M:%S")

def _fmt_dt_msec3(dt_aw: datetime) -> str:
    dt_naive = _to_naive_jst(dt_aw)
    return dt_naive.strftime("%Y-%m-%d %H:%M:%S.") + f"{int(dt_naive.microsecond/1000):03d}"


# ============================== forecast table（最小スキーマ） ==============================
DDL_FORECAST = """CREATE TABLE IF NOT EXISTS `{tbl}` (
  `target_time` DATETIME NOT NULL,
  `base_time`   DATETIME NOT NULL,
  `yhat`        DOUBLE(10,6) NOT NULL,
  `residual`    DOUBLE(10,6) NULL,
  `is_anomaly`  TINYINT(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`target_time`),
  KEY `idx_base_time` (`base_time`),
  KEY `idx_is_anomaly_time` (`is_anomaly`, `target_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""

def ensure_forecast_table_exists() -> None:
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(DDL_FORECAST.format(tbl=FORECAST_TABLE))
    _retry_once(_exec)

def insert_forecast(target_time: datetime, base_time: datetime, yhat: float) -> None:
    sql = (
        f"INSERT INTO `{FORECAST_TABLE}` (target_time, base_time, yhat) "
        "VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE base_time=VALUES(base_time), yhat=VALUES(yhat)"
    )
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (_fmt_dt_sec(target_time), _fmt_dt_sec(base_time), float(yhat)))
    _retry_once(_exec)

def get_saved_yhat(target_time: datetime) -> Optional[float]:
    sql = f"SELECT yhat FROM `{FORECAST_TABLE}` WHERE target_time=%s"
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (_fmt_dt_sec(target_time),))
            row = cur.fetchone()
            return None if not row else float(row["yhat"])
    return _retry_once(_exec)

def update_evaluation(target_time: datetime, residual: float, is_anomaly: bool) -> None:
    sql = (
        f"UPDATE `{FORECAST_TABLE}` "
        "SET residual=%s, is_anomaly=%s "
        "WHERE target_time=%s"
    )
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (float(residual), int(is_anomaly), _fmt_dt_sec(target_time)))
    _retry_once(_exec)


# ============================== minute table（既存） ==============================
DDL_MINUTE = """CREATE TABLE IF NOT EXISTS `{tbl}` (
  `id` INT NOT NULL AUTO_INCREMENT,
  `datetime` DATETIME(3) NOT NULL,
  `pressure` DECIMAL(6,2) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_datetime` (`datetime`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"""

def ensure_minute_table_exists(table_name: str) -> None:
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(DDL_MINUTE.format(tbl=table_name))
    _retry_once(_exec)

def upsert_actual_minute(ts_min_boundary: datetime, pressure_value: float) -> None:
    tbl = f"{SENSOR_TABLE_PREFIX}{ts_min_boundary.strftime('%Y%m%d')}"
    ensure_minute_table_exists(tbl)

    dt_str = _fmt_dt_msec3(ts_min_boundary)
    pres_str = f"{float(pressure_value):.2f}"

    sql = (
        f"INSERT INTO `{tbl}` (`{DATETIME_COL}`, `{PRESSURE_COL}`) "
        "VALUES (%s, %s) "
        "ON DUPLICATE KEY UPDATE pressure=VALUES(pressure)"
    )
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (dt_str, pres_str))
    try:
        _retry_once(_exec)
    except pymysql_err.OperationalError as e:
        if getattr(e, "args", [None])[0] in (1146,):
            ensure_minute_table_exists(tbl)
            _retry_once(_exec)
        else:
            raise


# ============================== utils ==============================
def to_min_boundary(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)

def parse_iso_msec_jst(ts: str) -> datetime:
    dtp = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S.%f")
    return dtp.replace(tzinfo=JST)


# ============================== load series ==============================
def _select_series_from_table(cur, tbl: str, q_start_incl: datetime, q_end_excl: datetime) -> Optional[pd.Series]:
    sql = (
        f"SELECT `{DATETIME_COL}` AS dt, `{PRESSURE_COL}` AS lf_hpa "
        f"FROM `{tbl}` "
        f"WHERE `{DATETIME_COL}` >= %s AND `{DATETIME_COL}` < %s "
        f"AND SECOND(`{DATETIME_COL}`)=0 AND MICROSECOND(`{DATETIME_COL}`)=0 "
        f"ORDER BY `{DATETIME_COL}`"
    )
    cur.execute(sql, (_fmt_dt_msec3(q_start_incl), _fmt_dt_msec3(q_end_excl)))
    rows = cur.fetchall()
    if not rows:
        return None

    df = pd.DataFrame(rows)
    s_ts = pd.to_datetime(df["dt"], errors="coerce")
    idx = s_ts.dt.tz_localize("Asia/Tokyo")
    return pd.Series(df["lf_hpa"].astype(float).values, index=idx)

def load_minute_series(end_ts: datetime, minutes: int) -> pd.Series:
    start_ts = end_ts - timedelta(minutes=minutes - 1)
    start_date = start_ts.date()
    end_date = end_ts.date()

    day_start = datetime(end_date.year, end_date.month, end_date.day, 0, 0, 0, tzinfo=JST)
    end_excl = end_ts + timedelta(minutes=1)

    ranges: List[Tuple[str, datetime, datetime]] = []
    if start_date == end_date:
        ranges.append((f"{SENSOR_TABLE_PREFIX}{start_date.strftime('%Y%m%d')}", start_ts, end_excl))
    else:
        ranges.append((f"{SENSOR_TABLE_PREFIX}{start_date.strftime('%Y%m%d')}", start_ts, day_start))
        ranges.append((f"{SENSOR_TABLE_PREFIX}{end_date.strftime('%Y%m%d')}", day_start, end_excl))

    frames: List[pd.Series] = []
    with db_connect() as conn, conn.cursor() as cur:
        for tbl, q_start, q_end in ranges:
            cur.execute("SHOW TABLES LIKE %s", (tbl,))
            if not cur.fetchone():
                continue
            s = _select_series_from_table(cur, tbl, q_start, q_end)
            if s is not None and not s.empty:
                frames.append(s)

    if not frames:
        return pd.Series(dtype=float)

    s_all = pd.concat(frames).sort_index()
    s_all = s_all[~s_all.index.duplicated(keep="last")]

    target_idx = pd.date_range(start=start_ts, end=end_ts, freq="min", tz=JST)
    s_exact = s_all.reindex(target_idx)
    return s_exact.dropna()


# ============================== anomaly minutes / regime (同一テーブル参照) ==============================
def get_anomaly_minutes_set(window_start_incl: datetime, window_end_incl: datetime, radius_min: int) -> Set[pd.Timestamp]:
    sql = f"""
        SELECT target_time
          FROM `{FORECAST_TABLE}`
         WHERE is_anomaly=1 AND target_time >= %s AND target_time <= %s
         ORDER BY target_time
    """
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (_fmt_dt_sec(window_start_incl), _fmt_dt_sec(window_end_incl)))
            return [row["target_time"] for row in cur.fetchall()]
    rows = _retry_once(_exec)

    out: Set[pd.Timestamp] = set()
    for t in rows:
        center = pd.Timestamp(t).tz_localize("Asia/Tokyo")
        for d in range(-radius_min, radius_min + 1):
            out.add(center + pd.Timedelta(minutes=d))
    return out

def detect_regime_shift(window_start_incl: datetime, window_end_incl: datetime, k: int) -> bool:
    # 「異常が連続して同符号」>=k をレジーム扱い
    sql = f"""
        SELECT target_time, residual
          FROM `{FORECAST_TABLE}`
         WHERE is_anomaly=1 AND residual IS NOT NULL
           AND target_time >= %s AND target_time <= %s
         ORDER BY target_time
    """
    def _exec():
        with db_connect() as conn, conn.cursor() as cur:
            cur.execute(sql, (_fmt_dt_sec(window_start_incl), _fmt_dt_sec(window_end_incl)))
            return cur.fetchall()
    rows = _retry_once(_exec)
    if not rows:
        return False

    longest = 0
    current = 0
    last_sign = 0
    for r in rows:
        resid = float(r["residual"])
        sign = 1 if resid > 0 else (-1 if resid < 0 else 0)
        if sign == 0:
            current = 0
            last_sign = 0
            continue
        if sign == last_sign:
            current += 1
        else:
            current = 1
            last_sign = sign
        longest = max(longest, current)
        if longest >= k:
            return True
    return False


# ============================== fit/predict ==============================
def fit_arima(y: pd.Series):
    if y is None or y.empty or len(y) < 60:
        return None, None
    model = ARIMA(y, order=ARIMA_ORDER, enforce_stationarity=False, enforce_invertibility=False)
    try:
        res = model.fit(method_kwargs={"maxiter": 200})
    except TypeError:
        res = model.fit(maxiter=200)
    return model, res

def predict_at(results, base_time: datetime, target_time: datetime) -> float:
    steps = int((target_time - base_time).total_seconds() // 60)
    fc = results.get_forecast(steps=max(1, steps))
    return float(fc.predicted_mean.iloc[-1])


# ============================== pool & replacement ==============================
def _update_resid_pool(resid_clean: pd.Series) -> int:
    if resid_clean is None or resid_clean.empty:
        return 0
    added = 0
    for v in resid_clean.astype(float).tolist():
        if np.isfinite(v):
            _resid_pool.append(float(v))
            added += 1
    return added

def _pool_clip_abs() -> Optional[float]:
    if len(_resid_pool) < max(50, RESID_POOL_MIN_FOR_SAMPLE):
        return None
    arr = np.abs(np.array(_resid_pool, dtype=float))
    if arr.size == 0:
        return None
    q = float(np.quantile(arr, RESID_CLIP_Q))
    return float(np.clip(q, 0.02, 2.00))

def build_training_series_with_replacement(now_min: datetime) -> pd.Series:
    s_raw = load_minute_series(now_min, WINDOW_MINUTES)
    if s_raw.empty:
        return s_raw

    window_start = s_raw.index.min()
    window_end = s_raw.index.max()

    replace_set = get_anomaly_minutes_set(window_start.to_pydatetime(), window_end.to_pydatetime(), REPLACE_RADIUS_MIN)
    exclude_for_pool_set = get_anomaly_minutes_set(window_start.to_pydatetime(), window_end.to_pydatetime(), RESID_EXCLUDE_RADIUS_MIN)

    # 予備モデル（置換なし）→ resid_pool 更新
    _, prelim_results = fit_arima(s_raw)
    if prelim_results is None:
        return pd.Series(dtype=float)

    resid_series = pd.Series(prelim_results.resid, index=s_raw.index).astype(float)

    # 立ち上がり除外
    if RESID_WARMUP_DROP > 0 and len(resid_series) > RESID_WARMUP_DROP:
        resid_series_for_pool = resid_series.iloc[RESID_WARMUP_DROP:].copy()
    else:
        resid_series_for_pool = resid_series.copy()

    # 異常近傍は pool 更新から除外
    pool_mask = ~resid_series_for_pool.index.isin(list(exclude_for_pool_set))
    resid_clean_for_pool = resid_series_for_pool[pool_mask].dropna()
    added = _update_resid_pool(resid_clean_for_pool)

    # レジームなら置換しない
    if detect_regime_shift(window_start.to_pydatetime(), window_end.to_pydatetime(), REGIME_K):
        logger.warning(f"[REPLACE] regime shift suspected (>= {REGIME_K} one-sided). skip replacement this window.")
        return s_raw

    # 予備モデルの fitted（置換のベース）
    fitted = prelim_results.get_prediction(start=s_raw.index[0], end=s_raw.index[-1]).predicted_mean
    fitted.index = pd.DatetimeIndex(fitted.index).tz_localize("Asia/Tokyo") if fitted.index.tz is None else fitted.index

    # フォールバックσ（pool不足時）
    if len(resid_clean_for_pool) >= 30:
        med = float(np.median(resid_clean_for_pool))
        mad = float(np.median(np.abs(resid_clean_for_pool - med)))
        sigma_fallback = 1.4826 * mad
    else:
        tmp = resid_series_for_pool.dropna()
        sigma_fallback = float(tmp.std(ddof=1)) if len(tmp) > 1 else 0.1
        sigma_fallback = max(sigma_fallback, 0.05)
    sigma_fallback *= NOISE_SCALE

    clip_abs = _pool_clip_abs()
    if clip_abs is None:
        clip_abs = max(3.0 * sigma_fallback, 0.10)

    # 置換本体
    s = s_raw.copy()
    replaced = 0
    for t in s.index:
        if t not in replace_set:
            continue

        # base yhat: DBに保存されている予測があればそれ優先、無ければ fitted
        yhat_db = get_saved_yhat(t.to_pydatetime())
        if yhat_db is not None:
            yhat_base = float(yhat_db)
        else:
            try:
                yhat_base = float(fitted.loc[t])
            except KeyError:
                yhat_base = float(fitted.iloc[(np.abs(fitted.index - t)).argmin()])

        # eps: pool優先、足りなければ正規
        if len(_resid_pool) >= RESID_POOL_MIN_FOR_SAMPLE:
            eps = float(_rng.choice(list(_resid_pool)))
        else:
            eps = float(_rng.normal(loc=0.0, scale=sigma_fallback))

        eps = float(np.clip(eps, -clip_abs, clip_abs))
        s.loc[t] = yhat_base + eps
        replaced += 1

    logger.info(f"[POOL] size={len(_resid_pool)} add={added}  [REPLACE] replaced={replaced} eps_clip=±{clip_abs:.3f}")
    return s


# ============================== per-minute process ==============================
def process_minute(arrival_min: datetime, actual_payload: Optional[float]) -> None:
    # 鮮度ガード
    now_jst = datetime.now(JST).replace(second=0, microsecond=0)
    lag_min = (now_jst - arrival_min).total_seconds() / 60.0
    if lag_min > MAX_ACCEPTABLE_LAG_MIN:
        logger.warning(f"skip outdated minute {arrival_min:%Y-%m-%d %H:%M} (lag={lag_min:.1f}m)")
        return

    ensure_forecast_table_exists()

    # 実測保存
    if actual_payload is not None:
        try:
            upsert_actual_minute(arrival_min, actual_payload)
        except Exception as e:
            logger.exception(f"failed to upsert actual minute: {e}")

    # 1) 評価（固定閾値）
    if actual_payload is not None:
        yhat_saved = get_saved_yhat(arrival_min)
        if yhat_saved is not None:
            residual = float(actual_payload) - float(yhat_saved)
            is_anom = (residual >= TH_HI) or (residual <= TH_LO)
            update_evaluation(arrival_min, residual, is_anom)
            logger.info(
                f"[EVAL] t={arrival_min:%H:%M} resid={residual:+.6f} "
                f"th=({TH_LO:+.6f},{TH_HI:+.6f}) anom={int(is_anom)}"
            )
        else:
            logger.info("[EVAL] no saved forecast for this minute (skip)")

    # 2) 学習（置換あり）→ 予測（t+5）→ 保存
    y_train = build_training_series_with_replacement(arrival_min)
    if y_train.empty or len(y_train) < 60:
        logger.info(f"[FIT] warm-up/insufficient: train_len={len(y_train)}")
        return

    _, results = fit_arima(y_train)
    if results is None:
        logger.warning("[FIT] ARIMA fit skipped")
        return

    target_time = arrival_min + timedelta(minutes=FORECAST_AHEAD_MIN)
    yhat = predict_at(results, arrival_min, target_time)
    insert_forecast(target_time, arrival_min, yhat)
    logger.info(f"[PRED] base={arrival_min:%H:%M} target={target_time:%H:%M} yhat={yhat:.6f}")


# ============================== MQTT callbacks ==============================
def _parse_dt_payload(payload: bytes) -> Optional[datetime]:
    try:
        dtp = parse_iso_msec_jst(payload.decode("utf-8").strip())
        return to_min_boundary(dtp)
    except Exception as e:
        logger.warning(f"dt parse error: {e}")
        return None

def _parse_pres_payload(payload: bytes) -> Optional[float]:
    try:
        return float(payload.decode("utf-8").strip())
    except Exception as e:
        logger.warning(f"pres parse error: {e}")
        return None

def _try_commit_pair():
    global _last_dt_min, _last_pres_value
    if _last_dt_min is None or _last_pres_value is None:
        return
    t_min = _last_dt_min
    val = _last_pres_value
    _last_dt_min = None
    _last_pres_value = None
    process_minute(t_min, actual_payload=val)

def on_connect(client, userdata, flags, reason_code, properties):
    logger.info(f"connected rc={reason_code}")
    client.subscribe([(MQTT_TOPIC_DT, 0), (MQTT_TOPIC_PRES, 0)])
    logger.info(f"subscribed: {MQTT_TOPIC_DT}, {MQTT_TOPIC_PRES}")

def on_message(client, userdata, msg):
    global _last_dt_min, _last_pres_value
    try:
        if msg.topic == MQTT_TOPIC_DT:
            dt_min = _parse_dt_payload(msg.payload)
            if dt_min:
                _last_dt_min = dt_min
                _try_commit_pair()
        elif msg.topic == MQTT_TOPIC_PRES:
            val = _parse_pres_payload(msg.payload)
            if val is not None:
                _last_pres_value = val
                _try_commit_pair()
    except Exception as e:
        logger.exception(f"on_message error: {e}")


# ============================== entry ==============================
def main():
    ensure_forecast_table_exists()
    logger.info("forecast_table=%s", FORECAST_TABLE)
    logger.info("ARIMA order=%s, window=%dmin ahead=%dmin", ARIMA_ORDER, WINDOW_MINUTES, FORECAST_AHEAD_MIN)
    logger.info("FIXED threshold: TH_LO=%+.6f TH_HI=%+.6f", TH_LO, TH_HI)
    logger.info("REPLACE: radius=%d  POOL_EXCLUDE=%d  REGIME_K=%d  pool_min=%d",
                REPLACE_RADIUS_MIN, RESID_EXCLUDE_RADIUS_MIN, REGIME_K, RESID_POOL_MIN_FOR_SAMPLE)

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("stopped by user")

if __name__ == "__main__":
    main()

