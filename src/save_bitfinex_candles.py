# -*- coding: utf-8 -*-

import sys
import os
import requests
import logging
import logging.handlers
import json
import time
import datetime as dt
import pymysql


class ApiException(Exception):
    pass


'''
    bitfinex(거래소)의 특정 통화의 candle(시가, 종가, 고가, 저가, 거래량)을 db(MySql)에 저장.
    db에서 가장 최근의 저장한 캔들의 시간을 읽어와서, 해당 시간부터 이어서 동작한다.
    db가 비어있는 경우는 START_DATE 부터 시작한다.
'''
class SaveBitfinexCandles:
    LIMIT_CANDLES = 100                     # 조회할 최대 캔들 개수.
    START_DATE = dt.datetime(2017, 1, 1)    # db가 비어있는 경우에 시작할 날짜.

    def __init__(self,
                 currency,
                 candle_time,
                 db_table_name,
                 log_level,
                 log_foramt='[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > %(message)s',
                 db_info={'host': 'localhost', 'user': 'root'}):
        self.logger = self.init_logger(log_level, log_foramt)
        self.currency = currency                            # tBTCUSD, tETHUSD ...
        self.candle_time = candle_time                      # [1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 7D, 14D, 1M]
        self.db_table_name = "{}".format(db_table_name)
        self.min_period = self.get_min_period()             # api로 한번에 조회하는 기간(분).
        self.db_info = db_info
        self.sec_save_interval = 5                          # 과거 데이터 처리 시, 저장하고 대기하는 시간.
        self.sec_work_interval = 10                         # 메인 루프 대기 시간.
        self.conn_db = None
        self.date_now = None                                # 작업 중인 현재 시간.


    def init_logger(self, log_level, log_foramt):
        logger = logging.getLogger(os.path.basename(__file__))
        logger.setLevel(log_level)
        formatter = logging.Formatter(log_foramt)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

        return logger


    def get_min_period(self):
        value = int(self.candle_time[:-1])
        unit = self.candle_time[-1]

        min_period = self.LIMIT_CANDLES
        if unit == 'm':
            min_period *= value
        elif unit == 'h':
            min_period *= (value * 60)
        elif unit == 'D':
            min_period *= (value * 60) * 24
        elif unit == 'M':
            min_period *= ((value * 60) * 24) * 30
        else:
            raise Exception("invalid candle_time: {}".format(self.candle_time))

        return min_period


    def start(self):
        while True:
            try:
                self.conn_db = pymysql.connect(host=self.db_info['host'],
                                               port=self.db_info['port'],
                                               user=self.db_info['user'],
                                               password=self.db_info['password'],
                                               db=self.db_info['db'],
                                               charset='utf8',
                                               cursorclass=pymysql.cursors.DictCursor)

                self.date_now = dt.datetime.now()
                date_last = self.get_last_candle_date_in_db()
                if date_last:
                    # 실시간 업데이트를 위해, 마지막 데이터와 일정 시간이 차이나면 작업 수행.
                    sec_diff = (self.date_now - date_last).seconds
                    need_work = 60 <= sec_diff
                else:   # if db is empty, start default date.
                    date_last = self.START_DATE
                    need_work = True

                if need_work:
                    self.logger.info("start working...")
                    self.work(date_last)
                    self.logger.info("end.")
                else:
                    self.logger.info("waiting...")

            except Exception as e:
                self.logger.error(e)
            finally:
                self.conn_db.close()
                self.conn_db = None
                time.sleep(self.sec_work_interval)


    def get_last_candle_date_in_db(self):
        with self.conn_db.cursor() as cursor:
            select_query =  "SELECT start_timestamp AS t " + \
                            "FROM {} ".format(self.db_table_name) + \
                            "ORDER BY id DESC LIMIT 1"
            cursor.execute(select_query)

            result = None
            for row in cursor:
                result = row

            if result:
                return result['t']  # datetime.
            else:
                return None         # empty db.


    def work(self, date_last):
        self.save(date_last, self.date_now)


    def save(self, start_date, end_date):
        def date_range(start, end, delta):
            curr = start
            while curr < end:
                yield curr
                curr += delta

        for save_date in date_range(start_date, end_date, dt.timedelta(minutes=self.min_period)):
            sucess = False
            while sucess is False:
                try:
                    self.save_period(save_date, self.min_period)
                    sucess = True
                except ApiException as e:
                    time.sleep(60)          # 대부분의 경우에 너무 많은 요청을 보낸 것.
                except Exception as e:
                    self.logger.error(e)
                    sucess = False
                finally:
                    time.sleep(self.sec_save_interval)


    def save_period(self, save_date, min_period):
        start_timestamp = int(time.mktime(save_date.timetuple()))
        candles = self.get_candles(start_timestamp=start_timestamp, min_period=min_period)
        self.save_db(candles)

        self.logger.info("n_candles: {}".format(len(candles)))

        # 과거 데이터처리가 끝나고, 최신 데이터만 업데이트하면 되므로 대기시간을 늘림.
        if len(candles) <= 1:
            self.sec_work_interval = 30


    def get_candles(self, start_timestamp, min_period):
        start_timestamp *= 1000
        end_timestamp = start_timestamp + (min_period * 60 * 1000)
        end_timestamp = min(end_timestamp, int(time.mktime(self.date_now.timetuple())) * 1000)
        end_timestamp -= 100   # -100ms 로 경계 제외.
        url = "https://api.bitfinex.com/v2/candles/trade:{}:{}/hist".format(self.candle_time, self.currency) + \
                    "?start={}&end={}&sort=1&limit={}".format(start_timestamp, end_timestamp, self.LIMIT_CANDLES)
        self.logger.debug(url)

        response = requests.get(url)
        if (response.status_code != 200):
            self.logger.error("url: {}".format(url))
            self.logger.error("status_code: {}".format(response.status_code))
            raise ApiException('fail api call.')

        content = response.content.decode('utf-8')
        candles = eval(content)
        self.logger.debug("candle count: {}".format(len(candles)))
        return candles


    def save_db(self, candles):
        query = "INSERT INTO {}(`start_timestamp`, `open`, `high`, `low`, `close`, `volume`) ".format(self.db_table_name) + \
                'VALUES '

        query_value = ''
        for candle in candles:
            timestamp = int(candle[0] / 1000)
            date_candle = dt.datetime.fromtimestamp(timestamp)
            date_candle_str = date_candle.strftime('%Y-%m-%d %H:%M:%S')
            o = candle[1]   # open.     시작가.
            h = candle[3]   # high.     최고가.
            l = candle[4]   # low.      최저가.
            c = candle[2]   # close.    종가.
            v = candle[5]   # volume.   거래량.
            query_value += "('{}', '{}', '{}', '{}', '{}', '{}'),".format(date_candle_str, o, h, l, c, v)

            self.logger.info("[{}] O:{}, H:{}, L:{} C:{} V:{}".format(date_candle_str, o, h, l, c, v))

        query += query_value[:-1]
        query += " ON DUPLICATE KEY UPDATE " + \
                 "`open` = VALUES(`open`), `high` = VALUES(`high`), `high` = VALUES(`high`), `low` = VALUES(`low`), `close` = VALUES(`close`), `volume` = VALUES(`volume`)"

        try:
            with self.conn_db.cursor() as cursor:
                cursor.execute(query)
                self.conn_db.commit()
        except Exception as e:
            self.logger.error('[query] ' + query)


def main(argv):
    ''' ###### configurations. ###### '''
    db_info = {
            'host': 'localhost',
            'port': 3306,
            'user': 'user',
            'password': 'password',
            'db': 'candle'
        }
    ''' ###### configurations. ###### '''

    n_arg = 4

    if len(argv) < n_arg:
        print("usage: {} {} {} {}".format(os.path.basename(__file__), 'currency', 'candle_time', 'db_table_name', '[loglevel]'))
        print("  currency: tBTCUSD, tETHUSD ...")
        print('    Trading pairs symbols are formed prepending a "t" before the pair (i.e tBTCUSD, tETHUSD).')
        print("  candle_time: 1m, 5m, 15m, 30m, 1h, 3h, 6h, 12h, 1D, 7D, 14D, 1M")
        print("  db_table_name: MySQL table name.")
        print("  [loglevel]: DEBUG, INFO, WARNING, ERROR, CRITICAL")
        return

    # last argument can be loglevel.
    log_level = 'INFO'
    if n_arg < len(argv):
        log_level = argv[n_arg]

    save_bitfinex_candles = SaveBitfinexCandles(currency=argv[1],
                                                candle_time=argv[2],
                                                db_table_name=argv[3],
                                                db_info=db_info,
                                                log_level=log_level)
    save_bitfinex_candles.start()


if __name__ == "__main__":
    main(sys.argv)
