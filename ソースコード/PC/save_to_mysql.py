#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
program_B_save_env_and_infra_batches.py

役割:
- Raspberry Pi のスクリプトAが publish する
    sensor/raw/env_batch トピック
  を subscribe して、MySQL に保存する。

前提:
- env_batch の payload は JSON 形式で、
    {
        "samples": [
            {
                "datetime": "2025-01-01 12:34:56.7",
                "temperature": 23.45,
                "pressure": 1013.25,
                "humidity": 45.678,
                "voltage": 0.0123
            },
            ...
        ]
    }
  のような形。

"""

import json
import time
import datetime

import mysql.connector
import paho.mqtt.client as mqtt

# ====== 設定 ======

# MySQL 接続設定（必要に応じて変更）
DB_HOST = "localhost"
DB_USER = "****"
DB_PASS = "****"
DB_NAME = "****"

# MQTT ブローカー設定（スクリプトAと合わせる）
MQTT_BROKER_IP = "localhost"
MQTT_PORT = 1883
MQTT_KEEP_ALIVE = 60

# 購読するトピック（スクリプトAと合わせる）
TOPIC_ENV_BATCH = "sensor/raw/env_batch"

# infrasound(=voltage) の異常判定しきい値（今回の静穏データから算出した「平均±4σ」）
ANOMALY_THRESHOLD_LOW  = 0.00273
ANOMALY_THRESHOLD_HIGH = 0.00999


# ====== MySQL 関係 ======

def get_db_connection():
    """MySQL への接続を作成して返す"""
    conn = mysql.connector.connect(
        user=DB_USER,
        passwd=DB_PASS,
        host=DB_HOST,
        db=DB_NAME,
    )
    return conn


def ensure_table_exists(conn, day_str):
    """
    指定日 (YYYYMMDD) の sensor_data_YYYYMMDD テーブルを作成する。
    既に存在する場合も安全に呼べる。

    さらに、既存テーブルに is_anomaly カラムが無い場合は ALTER TABLE で追加する。
    """
    table_name = "sensor_data_" + day_str

    create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name}(
            id INT NOT NULL AUTO_INCREMENT,
            datetime    DATETIME(3)  NOT NULL,
            temperature DECIMAL(4,2) NOT NULL,
            pressure    DECIMAL(8,4) NOT NULL,
            humidity    DECIMAL(6,3) NOT NULL,
            infrasound  DECIMAL(9,6) NOT NULL,
            is_anomaly  TINYINT(1)   NOT NULL DEFAULT 0,
            PRIMARY KEY(id)
        );
    """

    cur = conn.cursor()
    cur.execute(create_table_query)
    conn.commit()

    # 既存テーブルに is_anomaly が無いケースへの対応（後方互換）
    # INFORMATION_SCHEMA で列の存在チェック
    col_check_sql = """
        SELECT COUNT(*)
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s
          AND TABLE_NAME = %s
          AND COLUMN_NAME = 'is_anomaly'
    """
    cur.execute(col_check_sql, (DB_NAME, table_name))
    exists = cur.fetchone()[0]

    if exists == 0:
        alter_sql = f"ALTER TABLE {table_name} ADD COLUMN is_anomaly TINYINT(1) NOT NULL DEFAULT 0;"
        cur.execute(alter_sql)
        conn.commit()
        print(f"[DB] Added column is_anomaly to {table_name}")

    return table_name


def insert_samples(conn, table_name, records):
    """
    records: list of (datetime_str, temp, pres, humi, infra, is_anomaly)
    を指定テーブルに INSERT する。
    """
    if not records:
        return

    cur = conn.cursor()
    insert_sql = f"""
        INSERT INTO {table_name}
            (datetime, temperature, pressure, humidity, infrasound, is_anomaly)
        VALUES
            (%s, %s, %s, %s, %s, %s)
    """
    cur.executemany(insert_sql, records)
    conn.commit()
    print(f"[DB] Inserted {len(records)} rows into {table_name}")


# ====== MQTT コールバック ======

class EnvBatchSubscriber:
    def __init__(self, conn):
        self.conn = conn
        self.anomaly_times = []  # （追加）異常を検知した datetime を記録
        self.client = mqtt.Client()

        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message

    def start(self):
        self.client.connect(MQTT_BROKER_IP, MQTT_PORT, MQTT_KEEP_ALIVE)
        self.client.loop_start()

    def stop(self):
        self.client.loop_stop()
        self.client.disconnect()

    def on_connect(self, client, userdata, flags, rc):
        print("MQTT Connection Success" if rc == 0 else f"MQTT Connection refuse rc={rc}")
        client.subscribe(TOPIC_ENV_BATCH)
        print(f"[MQTT] Subscribed to '{TOPIC_ENV_BATCH}'")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("Unexpected MQTT disconnection.")

    def on_message(self, client, userdata, msg):
        """
        sensor/raw/env_batch のメッセージを受信したときに呼ばれる。
        → JSON をパースして、日付ごとにテーブル作成＆INSERT。
        → 各サンプルごとに voltage 閾値判定して is_anomaly を保存。
        """
        try:
            payload_str = msg.payload.decode("utf-8")
            data = json.loads(payload_str)
            samples = data.get("samples", [])
            if not samples:
                print("[MQTT] Received env_batch with no samples.")
                return

            # 日付ごとにグルーピング
            day_to_records = {}  # day_str -> list of (dt_str, temp, pres, humi, infra, is_anomaly)

            for s in samples:
                dt_str = s["datetime"]
                temp = float(s["temperature"])
                pres = float(s["pressure"])
                humi = float(s["humidity"])
                infra = float(s["voltage"])  # publish 側が voltage キーで送っている前提

                # dt_str 例: "2025-01-01 12:34:56.7"
                dt = datetime.datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S.%f")
                day_str = dt.strftime("%Y%m%d")

                # （変更）閾値判定：範囲外なら異常
                is_anomaly = 1 if (infra < ANOMALY_THRESHOLD_LOW or infra > ANOMALY_THRESHOLD_HIGH) else 0

                # （変更）異常時刻を記録（1行＝1時刻なので対応は1対1）
                if is_anomaly == 1:
                    self.anomaly_times.append(dt_str)
                    print(
                        f"[ANOMALY] voltage={infra} out of range "
                        f"[{ANOMALY_THRESHOLD_LOW}, {ANOMALY_THRESHOLD_HIGH}] at {dt_str}"
                    )

                if day_str not in day_to_records:
                    day_to_records[day_str] = []

                day_to_records[day_str].append((dt_str, temp, pres, humi, infra, is_anomaly))

            # 日付ごとにテーブル作成→INSERT
            for day_str, records in day_to_records.items():
                table_name = ensure_table_exists(self.conn, day_str)
                insert_samples(self.conn, table_name, records)

        except Exception as e:
            print("[ERROR] on_message handling failed:", e)
            # subscriber 全体は落とさずログだけ出す運用でOK


# ====== メイン ======

def main():
    conn = get_db_connection()
    print("[DB] Connected to MySQL.")

    subscriber = EnvBatchSubscriber(conn)
    subscriber.start()

    print("Start subscribing env_batch and saving to MySQL... (Ctrl+C to stop)")

    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("KeyboardInterrupt: stopping...")
    finally:
        subscriber.stop()
        conn.close()
        print("Stopped and closed DB connection.")

        # 終了時に直近の異常時刻を表示
        if subscriber.anomaly_times:
            print("[ANOMALY] detected times (last up to 20):")
            for t in subscriber.anomaly_times[-20:]:
                print(" -", t)


if __name__ == "__main__":
    main()
