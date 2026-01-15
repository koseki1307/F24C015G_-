#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
script_A_measure_and_publish_10hz.py

Raspberry Pi 上で動作する「測定＋MQTT publish 専用」スクリプト（B案）。
- BME280 (temp, pres, humi)
- MCP3425 (infrasound / 微気圧センサ用 A/D)

を 10 Hz で取得し、MQTT ブローカーへ publish する。

・10件ごとのバッチを常時観測保存用トピックへ publish
    - sensor/raw/env_batch   ：temp, pres, humi,voltages, datetime の配列

・ちょうど「○分00.0秒」のサンプルだけは、
  予測プログラム用トピックへ 1件だけ publish
    - sensor/dt   ：{"datetime": "..."}
    - sensor/pres ：{"pressure": ...}

DB への保存は行わず、ノートPC側の subscriber プログラムが
MySQL 等へ保存・予測処理を行うことを想定。
"""

from smbus2 import SMBus
import datetime
from datetime import timedelta
import time
import json
import sys

import paho.mqtt.client as mqtt

# ====== 設定 ======

# MQTT ブローカー設定
MQTT_BROKER_IP   = "****"  # ノートPC等の IP に合わせて変更
MQTT_PORT        = 1883
MQTT_KEEP_ALIVE  = 60
MQTT_QOS         = 1

# MQTT トピック名
TOPIC_ENV_BATCH   = "sensor/raw/env_batch"     # 10件バッチ（temp,pres,humi,datetime）
TOPIC_INFRA_BATCH = "sensor/raw/infra_batch"   # 10件バッチ（voltage,datetime）

TOPIC_DT_MIN      = "sensor/dt"                # 00秒サンプル1件（datetimeのみ）
TOPIC_PRES_MIN    = "sensor/pres"              # 00秒サンプル1件（pressureのみ）

# I2C アドレス設定
MCP_ADDR = 0x68   # MCP3425
BME_ADDR = 0x76   # BME280

# MCP3425 設定（元コード準拠）
Vref       = 2.048
config     = 0b10011000
BUS_NUMBER = 1

# ====== グローバル変数（BME280 補正用） ======
bus = SMBus(BUS_NUMBER)

digT = []
digP = []
digH = []
t_fine = 0.0


# ====== MQTT Publisher クラス ======
class Publisher(object):
    def __init__(self, broker_ip_address, port, keep_alive_time, qos):
        self.qos = qos
        self.client = mqtt.Client()
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.connect(broker_ip_address, port, keep_alive_time)
        self.client.loop_start()

    def on_connect(self, client, obj, flags, rc):
        print('MQTT Connection Success' if rc == 0 else f'MQTT Connection refuse rc={rc}')

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print("Unexpected MQTT disconnection.")

    def send_json(self, topic, dict_data):
        """辞書やリスト入り辞書を JSON 文字列にして publish"""
        payload = json.dumps(dict_data)
        self.client.publish(topic, payload, qos=self.qos)
        # デバッグ用
        #print(f"[MQTT] topic='{topic}' payload={payload}")
        
    def send_text(self,topic,text:str):
        self.client.publish(topic,text,qos=self.qos)
        #デバック用
        #print(f"[MQTT] topic='{topic}' payload='{text}'")


# ====== BME280 関連関数 ======
def writeReg(reg_address, data):
    bus.write_byte_data(BME_ADDR, reg_address, data)


def get_calib_param():
    """BME280 のキャリブレーションパラメータを取得（date_update_day_simpo.py 由来）"""
    calib = []

    for i in range(0x88, 0x88 + 24):
        calib.append(bus.read_byte_data(BME_ADDR, i))
    calib.append(bus.read_byte_data(BME_ADDR, 0xA1))
    for i in range(0xE1, 0xE1 + 7):
        calib.append(bus.read_byte_data(BME_ADDR, i))

    digT.append((calib[1] << 8) | calib[0])
    digT.append((calib[3] << 8) | calib[2])
    digT.append((calib[5] << 8) | calib[4])
    digP.append((calib[7] << 8) | calib[6])
    digP.append((calib[9] << 8) | calib[8])
    digP.append((calib[11] << 8) | calib[10])
    digP.append((calib[13] << 8) | calib[12])
    digP.append((calib[15] << 8) | calib[14])
    digP.append((calib[17] << 8) | calib[16])
    digP.append((calib[19] << 8) | calib[18])
    digP.append((calib[21] << 8) | calib[20])
    digP.append((calib[23] << 8) | calib[22])
    digH.append(calib[24])
    digH.append((calib[26] << 8) | calib[25])
    digH.append(calib[27])
    digH.append((calib[28] << 4) | (0x0F & calib[29]))
    digH.append((calib[30] << 4) | (calib[29] >> 4) & 0x0F)
    digH.append(calib[31])

    for i in range(1, 2):
        if digT[i] & 0x8000:
            digT[i] = (-digT[i] ^ 0xFFFF) + 1

    for i in range(1, 8):
        if digP[i] & 0x8000:
            digP[i] = (-digP[i] ^ 0xFFFF) + 1

    for i in range(0, 6):
        if digH[i] & 0x8000:
            digH[i] = (-digH[i] ^ 0xFFFF) + 1


def setup_bme280():
    """BME280 の設定（元スクリプトの setup 相当）"""
    osrs_t = 1          # Temperature oversampling x 1
    osrs_p = 1          # Pressure oversampling x 1
    osrs_h = 1          # Humidity oversampling x 1
    mode   = 1          # Normal mode
    t_sb   = 0          # Tstandby 0.5ms
    filter_ = 0         # Filter off
    spi3w_en = 0        # 3-wire SPI Disable

    ctrl_meas_reg = (osrs_t << 5) | (osrs_p << 2) | mode
    config_reg    = (t_sb << 5) | (filter_ << 2) | spi3w_en
    ctrl_hum_reg  = osrs_h

    writeReg(0xF2, ctrl_hum_reg)
    writeReg(0xF4, ctrl_meas_reg)
    writeReg(0xF5, config_reg)


def compensate_T(adc_T):
    global t_fine
    v1 = (adc_T / 16384.0 - digT[0] / 1024.0) * digT[1]
    v2 = (adc_T / 131072.0 - digT[0] / 8192.0) * (adc_T / 131072.0 - digT[0] / 8192.0) * digT[2]
    t_fine = v1 + v2
    temperature = t_fine / 5120.0
    return temperature


def compensate_P(adc_P):
    global t_fine
    pressure = 0.0

    v1 = (t_fine / 2.0) - 64000.0
    v2 = (((v1 / 4.0) * (v1 / 4.0)) / 2048) * digP[5]
    v2 = v2 + ((v1 * digP[4]) * 2.0)
    v2 = (v2 / 4.0) + (digP[3] * 65536.0)
    v1 = (((digP[2] * (((v1 / 4.0) * (v1 / 4.0)) / 8192)) / 8) + ((digP[1] * v1) / 2.0)) / 262144
    v1 = ((32768 + v1) * digP[0]) / 32768

    if v1 == 0:
        return 0

    pressure = ((1048576 - adc_P) - (v2 / 4096)) * 3125
    if pressure < 0x80000000:
        pressure = (pressure * 2.0) / v1
    else:
        pressure = (pressure / v1) * 2
    v1 = (digP[8] * (((pressure / 8.0) * (pressure / 8.0)) / 8192.0)) / 4096
    v2 = ((pressure / 4.0) * digP[7]) / 8192.0
    pressure = pressure + ((v1 + v2 + digP[6]) / 16.0)
    return pressure / 100.0  # hPa


def compensate_H(adc_H):
    global t_fine
    var_h = t_fine - 76800.0
    if var_h != 0:
        var_h = (adc_H - (digH[3] * 64.0 + digH[4] / 16384.0 * var_h)) * (
            digH[1] / 65536.0 * (1.0 + digH[5] / 67108864.0 * var_h *
                                 (1.0 + digH[2] / 67108864.0 * var_h))
        )
    else:
        return 0
    var_h = var_h * (1.0 - digH[0] * var_h / 524288.0)
    if var_h > 100.0:
        var_h = 100.0
    elif var_h < 0.0:
        var_h = 0.0
    return var_h


def readData_bme280():
    data = []
    for i in range(0xF7, 0xF7 + 8):
        data.append(bus.read_byte_data(BME_ADDR, i))
    pres_raw = (data[0] << 12) | (data[1] << 4) | (data[2] >> 4)
    temp_raw = (data[3] << 12) | (data[4] << 4) | (data[5] >> 4)
    hum_raw  = (data[6] << 8)  | data[7]

    temp = compensate_T(temp_raw)
    pres = compensate_P(pres_raw)
    humi = compensate_H(hum_raw)
    return temp, pres, humi


# ====== MCP3425 関連 ======
def swap16(x):
    return (((x << 8) & 0xFF00) |
            ((x >> 8) & 0x00FF))


def sign16(x):
    return (-(x & 0b1000000000000000) |
             (x & 0b0111111111111111))


def readData_mcp3425():
    """MCP3425 から電圧値を取得"""
    data = bus.read_word_data(MCP_ADDR, config)
    raw = swap16(int(hex(data), 16))
    raw_s = sign16(int(hex(raw), 16))
    volts = round((Vref * raw_s / 32767), 4)
    return volts  # [V]


# ====== メイン処理 ======
def main():
    # BME280 初期化
    get_calib_param()
    setup_bme280()

    # MQTT Publisher 初期化
    publisher = Publisher(
        broker_ip_address=MQTT_BROKER_IP,
        port=MQTT_PORT,
        keep_alive_time=MQTT_KEEP_ALIVE,
        qos=MQTT_QOS
    )

    # 10件バッチ用のバッファ
    env_batch = []    # {"datetime", "temperature", "pressure", "humidity}
    infra_batch = []  # {"datetime", "voltage"}

    # 秒=0 まで待機（10 Hz ループ開始タイミングを揃える）
    while True:
        now = datetime.datetime.now()
        if now.second == 0:
            break
        time.sleep(0.001)

    base_time = time.time()

    print("Start 10 Hz measurement & MQTT publish...")

    while True:
        try:
            # BME280 設定（元コード踏襲）
            setup_bme280()

            # 時刻を 0.1 秒刻みに丸める
            dt_ori = datetime.datetime.now()
            dt_s = dt_ori - timedelta(microseconds=dt_ori.microsecond % 100000)

            # センサ読み取り
            temp, pres, humi = readData_bme280()
            infra_volts = readData_mcp3425()

            # 時刻を文字列化（ミリ秒まで。末尾1桁カットは元コード準拠）
            dt_str = format(dt_s, '%Y-%m-%d %H:%M:%S.%f')

            # --- バッチ用バッファに追加 ---
            env_sample = {
                "datetime": dt_str,
                "temperature": round(temp, 2),
                "pressure": round(pres, 4),
                "humidity": round(humi, 3),
                "voltage": infra_volts,
            }

            env_batch.append(env_sample)
            
            # --- ちょうど 00秒 サンプルなら、予測用の2トピックに publish ---
            is_min_sample = (dt_s.second == 0 and dt_s.microsecond == 0)
            if is_min_sample:
                # sensor/dt → datetimeのみ
                publisher.send_text(TOPIC_DT_MIN, dt_str)
                # sensor/pres → pressureのみ
                publisher.send_text(TOPIC_PRES_MIN, f"{pres:2f}")

            # --- 10件たまったらバッチ publish ---
            if len(env_batch) >= 10:
                publisher.send_json(TOPIC_ENV_BATCH, {"samples": env_batch})
                env_batch = []

            # --- 次の 0.1 秒境界まで sleep（元コードのロジックを踏襲） ---
            next_time = ((base_time - time.time()) % 0.1)
            time.sleep(next_time)

        except KeyboardInterrupt:
            print("KeyboardInterrupt: stop script.")
            break
        except Exception as e:
            print("Error in main loop:", e)
            sys.exit(1)


if __name__ == '__main__':
    main()

