#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
monitor_anomaly_terminal.py

役割:
- MySQL を一定間隔でポーリングし、
  (1) 微気圧センサ側（sensor_data_YYYYMMDD）
  (2) 大気圧予測側（pressure_predict）
  の異常フラグを読み取り、
  4パターン（両方正常 / 微気圧のみ異常 / 予測のみ異常 / 両方異常）
  に応じたメッセージをターミナルへ出力する。

注意:
- Gmail通知は行わない（ターミナル出力のみ）
- ルールは「規模（strong/weak）」ではなく「状態の分類」に基づく
"""

import time
from dataclasses import dataclass
from datetime import datetime
import mysql.connector


# =========================
# 設定
# =========================

@dataclass
class MysqlConfig:
    host: str = "127.0.0.1"
    port: int = 3306
    user: str = "YOUR_USER"
    password: str = "YOUR_PASSWORD"
    database: str = "YOUR_DB"
    charset: str = "utf8mb4"


MYSQL = MysqlConfig()

# --- 監視対象テーブル ---
SENSOR_TABLE_PREFIX = "sensor_data_"
PRESSURE_PREDICT_TABLE = "pressure_predict"

# --- sensor_data_YYYYMMDD ---
SENSOR_PK = "id"
SENSOR_DT_COL = "datetime"

# --- pressure_predict ---
PREDICT_NEWKEY_COL = "target_time"   # 新規挿入検知キー
PREDICT_RESIDUAL_COL = "residual"    # residualがNULLでない最新1件を評価
PREDICT_BASE_COL = "base_time"
PREDICT_TARGET_COL = "target_time"

# --- 共通 ---
ANOMALY_COL = "is_anomaly"

# 監視ループ間隔（秒）
POLL_INTERVAL_SEC = 1.0

# 微気圧側は「最新N件のどこかで異常フラグが立っていれば異常」とする
SENSOR_LOOKBACK_N = 10


# =========================
# MySQL / 共通
# =========================

def get_today_sensor_table_name() -> str:
    yyyymmdd = datetime.now().strftime("%Y%m%d")
    return f"{SENSOR_TABLE_PREFIX}{yyyymmdd}"


def mysql_connect():
    return mysql.connector.connect(
        host=MYSQL.host,
        port=MYSQL.port,
        user=MYSQL.user,
        password=MYSQL.password,
        database=MYSQL.database,
        charset=MYSQL.charset,
        autocommit=True,
    )


def table_exists(cur, table: str) -> bool:
    cur.execute("SHOW TABLES LIKE %s", (table,))
    return cur.fetchone() is not None


def fetch_max_value(cur, table: str, col: str):
    cur.execute(f"SELECT MAX({col}) FROM {table}")
    row = cur.fetchone()
    return row[0] if row else None


# =========================
# 監視（異常判定の読み取り）
# =========================

def fetch_latest_n_anomaly_sensor(cur, sensor_table: str, n: int):
    """
    sensor_data_YYYYMMDD の最新 n 件を見て、
    is_anomaly=1 が1つでもあれば True。
    戻り: (anom1:bool, newest_id:int|None, newest_dt:any|None)
    """
    q = f"""
        SELECT {SENSOR_PK}, {SENSOR_DT_COL}, {ANOMALY_COL}
        FROM {sensor_table}
        ORDER BY {SENSOR_PK} DESC
        LIMIT {int(n)}
    """
    cur.execute(q)
    rows = cur.fetchall() or []
    if not rows:
        return False, None, None

    any_anom = any(int(r[2]) == 1 for r in rows if r[2] is not None)
    newest_id = rows[0][0]
    newest_dt = rows[0][1]
    return any_anom, newest_id, newest_dt


def fetch_latest_anomaly_predict(cur):
    """
    pressure_predict の residual が NULL でない最新1件を評価する。
    戻り: (anom2:bool, latest_target_time:any|None, base_time:any|None, residual:any|None)
    """
    q = f"""
        SELECT {PREDICT_TARGET_COL}, {PREDICT_BASE_COL}, {PREDICT_RESIDUAL_COL}, {ANOMALY_COL}
        FROM {PRESSURE_PREDICT_TABLE}
        WHERE {PREDICT_RESIDUAL_COL} IS NOT NULL
        ORDER BY {PREDICT_TARGET_COL} DESC
        LIMIT 1
    """
    cur.execute(q)
    row = cur.fetchone()
    if not row:
        return False, None, None, None

    target_time, base_time, residual, anom = row
    is_anom = (anom is not None and int(anom) == 1)
    return is_anom, target_time, base_time, residual


# =========================
# 状態分類（4パターン）
# =========================

def classify_state(anom1: bool, anom2: bool) -> str:
    """
    4パターン:
    - both_normal         : 両方正常
    - micro_only_anomaly  : 微気圧のみ異常
    - predict_only_anomaly: 予測のみ異常
    - both_anomaly        : 両方異常
    """
    if (not anom1) and (not anom2):
        return "both_normal"
    if anom1 and (not anom2):
        return "micro_only_anomaly"
    if (not anom1) and anom2:
        return "predict_only_anomaly"
    return "both_anomaly"


def make_terminal_message(state: str, anom1: bool, anom2: bool,
                          sensor_table: str,
                          newest_sensor_id, newest_sensor_dt,
                          pred_target_time, pred_base_time, pred_residual) -> str:
    """
    状態に応じて、ターミナルへ出すメッセージ本文を作る。
    ※「適したこと」の具体内容は、ここを編集すれば差し替え可能。
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    header_map = {
        "both_normal": "[STATE] 両方正常",
        "micro_only_anomaly": "[STATE] 微気圧のみ異常",
        "predict_only_anomaly": "[STATE] 予測のみ異常",
        "both_anomaly": "[STATE] 両方異常",
    }
    header = header_map.get(state, "[STATE] unknown")

    lines = [
        f"{header} ({now_str})",
        f"  anom1(micro): {anom1}",
        f"  anom2(predict): {anom2}",
        "",
        f"  sensor_table: {sensor_table}",
        f"    newest_id: {newest_sensor_id}",
        f"    newest_dt: {newest_sensor_dt}",
        "",
        f"  predict_table: {PRESSURE_PREDICT_TABLE}",
        f"    latest_target_time: {pred_target_time}",
        f"    base_time: {pred_base_time}",
        f"    residual: {pred_residual}",
    ]

    # 状態ごとの短い補足（必要ならここをあなたの言いたい表現に変更）
    if state == "both_normal":
        lines.append("")
        lines.append("  -> 状態: 安定（監視継続）")
    elif state == "micro_only_anomaly":
        lines.append("")
        lines.append("  -> 状態: 微気圧センサ側で異常検知（外乱/イベントの可能性）")
    elif state == "predict_only_anomaly":
        lines.append("")
        lines.append("  -> 状態: 予測側で異常判定（短時間の気象変動や外乱の可能性）")
    elif state == "both_anomaly":
        lines.append("")
        lines.append("  -> 状態: 両系統で異常（注視）")

    return "\n".join(lines)


# =========================
# メイン
# =========================

def main():
    # 新規更新検知用
    last_sensor_max_id = None
    last_predict_max_target_time = None  # MAX(target_time)

    # 状態（最新評価値）
    anom1 = False
    anom2 = False

    # 重複出力防止：状態変化時だけ出す
    last_printed_state = None

    print("[monitor] start (terminal output only)")

    while True:
        start = time.time()
        sensor_table = get_today_sensor_table_name()

        # 付随情報（ログに出す用）
        newest_sensor_id = None
        newest_sensor_dt = None
        pred_target_time = None
        pred_base_time = None
        pred_residual = None

        try:
            conn = mysql_connect()
            cur = conn.cursor()

            # --- 異常1: sensor_data_YYYYMMDD ---
            if table_exists(cur, sensor_table):
                sensor_max_id = fetch_max_value(cur, sensor_table, SENSOR_PK)
                if sensor_max_id is not None and sensor_max_id != last_sensor_max_id:
                    anom1, newest_sensor_id, newest_sensor_dt = fetch_latest_n_anomaly_sensor(
                        cur, sensor_table, SENSOR_LOOKBACK_N
                    )
                    last_sensor_max_id = sensor_max_id
                else:
                    # 新規がなくても、最新情報を表示するために newest_* は取得しておく
                    # （負荷を増やしたくない場合はこのブロックは削除してもOK）
                    anom1, newest_sensor_id, newest_sensor_dt = fetch_latest_n_anomaly_sensor(
                        cur, sensor_table, SENSOR_LOOKBACK_N
                    )
            else:
                # 日付切替直後など
                anom1 = False
                last_sensor_max_id = None

            # --- 異常2: pressure_predict (target_timeで新規検知) ---
            if table_exists(cur, PRESSURE_PREDICT_TABLE):
                predict_max_target = fetch_max_value(cur, PRESSURE_PREDICT_TABLE, PREDICT_NEWKEY_COL)
                if predict_max_target is not None and predict_max_target != last_predict_max_target_time:
                    anom2, pred_target_time, pred_base_time, pred_residual = fetch_latest_anomaly_predict(cur)
                    last_predict_max_target_time = predict_max_target
                else:
                    # 新規がなくても、表示用に最新評価値を取得
                    anom2, pred_target_time, pred_base_time, pred_residual = fetch_latest_anomaly_predict(cur)
            else:
                anom2 = False
                last_predict_max_target_time = None

            # --- 状態分類 ---
            state = classify_state(anom1, anom2)

            # --- 状態変化時のみ出力 ---
            if state != last_printed_state:
                msg = make_terminal_message(
                    state=state,
                    anom1=anom1,
                    anom2=anom2,
                    sensor_table=sensor_table,
                    newest_sensor_id=newest_sensor_id,
                    newest_sensor_dt=newest_sensor_dt,
                    pred_target_time=pred_target_time,
                    pred_base_time=pred_base_time,
                    pred_residual=pred_residual,
                )
                print(msg)
                print("-" * 60)
                last_printed_state = state

            cur.close()
            conn.close()

        except mysql.connector.Error as e:
            print(f"[error] mysql: {e}")
        except Exception as e:
            print(f"[error] unexpected: {e}")

        # ループ周期調整
        elapsed = time.time() - start
        time.sleep(max(0.0, POLL_INTERVAL_SEC - elapsed))


if __name__ == "__main__":
    main()
