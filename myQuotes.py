import threading
import math
import threading
import time
from datetime import datetime
from decimal import Decimal

import numpy as np
import pymongo
from binance import Client, ThreadedWebsocketManager


# TODO 初始化補資料補過去月份時會正常，但補當月份會把過去全部資料補在同一個月
# TODO 補 RECENT MONTH 時只補到1分Ｋ，不確定是被覆寫過去了還是沒寫成功

class Quotes:
    # start_time Y-m-d
    def __init__(self, exchange, target_symbol=None, max_bars=500, start_time: str = None,
                 end_time: str = None, api_key=None, api_secret=None):
        print("建立 DB 基本連線參數")
        self.local_db = pymongo.MongoClient("mongodb://localhost:27017/")
        self.dataDB = self.local_db['DATAFEED']
        self.max_bars = max_bars
        self.api_key = api_key
        self.api_secret = api_secret
        self.exchange = exchange.upper()
        self.exchange_name = exchange.upper().split("_")[0]

        # 建立幣安物件
        self.binance_api = Client(self.api_key, self.api_secret)

        # TODO WS_socket service 服務管理，這個之後要進DB
        self.socket_db_switch = 0

        print("更新 現貨與期貨的交易細節資訊至DB")
        self.update_spot_exchange_info()
        self.update_future_exchange_info()

        print("更新 DB 的訂閱內容 init 基本不用改，那是每個商品上交易所的時間")
        self.update_db_subscribe()

        self.sub_data = self.dataDB['SUBSCRIBE'].find_one({"SUBSCRIBE_TARGET": "BINANCE"}, {"_id": 0})
        # print(self.sub_data)

        print("拿取DB相關參數")
        self.feed = self.sub_data['data_source']
        self.sub_list = self.sub_data['sub']['symbol_list']
        self.sub_period_list = self.sub_data['sub']['peroid_list']
        # print(self.sub_period_list) ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M']
        # self.sub_period_list = ['1m']
        self.init_data_list = self.sub_data['sub']['init_date']
        # print(self.exchange_name)

        # 建立字串反查字典
        # {"BTC-USDT-PERP": "BTCUSDT"}
        self.input_difficult_symbol_name = {}
        for i in self.sub_list:
            self.input_difficult_symbol_name.update({i: i.replace("-PERP", "").replace("-", "")})

        # {"BTCUSDT": "BTC-USDT-PERP"}
        self.input_easy_symbol_name = {v: k for k, v in self.input_difficult_symbol_name.items()}
        print(self.input_easy_symbol_name)
        # print(self.input_easy_symbol_name)
        print("建立幣安參數用商品清單與websocket訂閱內容")

        print("建立K棒資料字典")
        self.ram_data = {}
        self.new_sub_list = [i.replace("-PERP", "").replace("-", "") for i in self.sub_list]

        # print(self.ram_data)
        self.target_symbol = target_symbol

        if self.target_symbol is not None:
            if isinstance(self.target_symbol, str):
                self.target_symbol = [self.target_symbol]
            else:
                self.target_symbol = self.target_symbol
            for index, delete_symbol in enumerate(self.target_symbol):
                if delete_symbol not in self.sub_list:
                    self.target_symbol.remove(delete_symbol)
                    print(delete_symbol + ' symbol removed')
        else:
            self.target_symbol = self.new_sub_list

        try:
            self.update_ram_data()
        except Exception as e:
            print(e)
            for checked in self.data_init_check():
                check_if_exist = self.data_sub_symbol_check(checked)
                if check_if_exist[0]:
                    print("查無資料，回補一個月的：{}".format(check_if_exist[1]))
                    self.refilled_data(
                        check_if_exist[1],
                        self.sub_period_list,
                        refilled_time_type="RECENT_MONTH"
                    )
            print("RELOAD DONE.")
            self.update_ram_data()

        print("建立呼叫基本參數")
        if start_time is not None:
            self.cunrent_time = int(math.floor(datetime.now().timestamp()))
            self.start_time = start_time
            self.ts_start_time = self.datetimestr_to_timestamp(self.start_time)

        if end_time is not None:
            self.end_time = end_time
            self.ts_end_time = self.datetimestr_to_timestamp(self.end_time)

        print("檢查是否有基礎商品資料表，沒有得先建立 target_symbol 且回補一個月後，再繼續往下跑服務")
        for checked in self.data_init_check():
            check_if_exist = self.data_sub_symbol_check(checked)
            if check_if_exist[0]:
                print("查無資料，回補一個月的：{}".format(check_if_exist[1]))
                self.refilled_data(
                    check_if_exist[1],
                    self.sub_period_list,
                    refilled_time_type="RECENT_MONTH"
                )
        print("初始化完成")
        print("啟用 websocket 服務")
        self.main_socket()

        threading.Thread(target=self.fill_all_latest_data_ram_data, args=(200,)).start()

    def data_sub_symbol_check(self, check_symbol=dict):
        # TODO 檢查是否有基礎商品資料表，沒有得先建立 target_symbol 且回補一個月後，再繼續往下跑服務

        if list(check_symbol.values())[0] == 0 and list(check_symbol.keys())[0] in self.sub_list:
            # print('表示幣安有這商品但是DB沒有資料: {}'.format(list(check_symbol.keys())[0]))
            return True, list(check_symbol.keys())[0]
        else:
            # print("幣安有商品DB也有資料，或是幣安沒商品ＤＢ當然也沒資料，故都不用處理")
            # print(list(check_symbol.keys())[0])
            return False, list(check_symbol.keys())[0]

    def data_init_check(self):
        if isinstance(self.target_symbol, str):
            result = self.dataDB[self.exchange].find_one({
                "broker": self.exchange,
                "datetime": self.generate_last_month_day_1(),
                "symbol": self.input_easy_symbol_name[self.target_symbol]
            }, {"_id": 0})['symbol']
        else:
            result = []
            for i in self.target_symbol:
                result.append({
                    i: self.dataDB[self.exchange].count_documents({
                        "broker": self.exchange,
                        "datetime": self.generate_last_month_day_1(),
                        "symbol": i
                    }, limit=1)
                })
        return result

    def datetimestr_to_timestamp(self, target_str):
        return datetime.strptime(target_str, "%Y-%m-%d").timestamp()

    def get_all_Binance_future_symbol_data(self):
        exchange_data = self.binance_api.futures_exchange_info()
        sub_symbol_list = []
        for info in exchange_data['symbols']:
            if info['quoteAsset'] == "USDT" and info['status'] == "TRADING":
                sub_symbol_list.append({info['baseAsset'] + '-' + info['quoteAsset'] + '-' + "PERP": str(
                    datetime.fromtimestamp(info['onboardDate'] / 1000).date())})
        return sub_symbol_list

    def update_spot_exchange_info(self):
        col = self.local_db[self.exchange_name.upper()]['ExchangeInfo']
        col.update_one(
            {
                "InfoName": "SPOT"
            },
            {
                "$set": {
                    "InfoData": [spot_info for spot_info in self.binance_api.get_exchange_info()['symbols'] if
                                 spot_info['quoteAsset'] == "USDT" and spot_info['status'] == "TRADING"]
                }
            },
            upsert=True
        )
        print("SPOT INFO UPDATE COMPLETED.")

    def update_future_exchange_info(self):
        col = self.local_db[self.exchange_name.upper()]['ExchangeInfo']
        col.update_one(
            {
                "InfoName": "FUTURES"
            },
            {
                "$set": {
                    "InfoData": [future_info for future_info in self.binance_api.futures_exchange_info()['symbols'] if
                                 future_info['quoteAsset'] == "USDT" and future_info['status'] == "TRADING"]
                }
            },
            upsert=True
        )
        print("FUTURES INFO UPDATE COMPLETED.")

    def update_db_subscribe(self, sub_target_list=None, sub_target_peroid=None, init_data=None):
        if sub_target_list is None:
            # print(self.get_all_Binance_future_symbol_data())
            sub_list = [list(i.keys())[0] for i in self.get_all_Binance_future_symbol_data()]
        else:
            sub_list = sub_target_list

        if sub_target_peroid is None:
            sub_period_list = [
                '1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w', '1M'
            ]  #
            # sub_period_list = ['1d', "4h"]
        else:
            sub_period_list = sub_target_peroid

        if init_data is None:
            init_data_list = self.get_all_Binance_future_symbol_data()
            # sub_period_list = ['1d', "4h"]
        else:
            init_data_list = init_data

        self.dataDB['SUBSCRIBE'].update_one({"SUBSCRIBE_TARGET": "BINANCE"}, {"$set":
            {
                "data_source": "BINANCE_FUTURES",
                "sub": {
                    "symbol_list": sub_list,
                    "peroid_list": sub_period_list,
                    "init_date": init_data_list
                }
            }
        }, upsert=True)
        # 重新更新清單
        self.sub_data = self.dataDB['SUBSCRIBE'].find_one({"SUBSCRIBE_TARGET": "BINANCE"}, {"_id": 0})
        self.feed = self.sub_data['data_source']
        self.sub_list = self.sub_data['sub']['symbol_list']
        self.sub_period_list = self.sub_data['sub']['peroid_list']
        self.init_data_list = self.sub_data['sub']['init_date']

    def days_generate(self, years, month):
        if int(month) in [1, 3, 5, 7, 8, 10, 12]:
            return 31
        elif int(month) in [4, 6, 9, 11]:
            return 30
        elif int(month) == 2:
            if int(years) % 4 == 0:
                return 29
            else:
                return 28

    def data_update(self, symbol, interval, list_data):
        """
        :param broker: "BINANCE_FUTURES"
        :param symbol: "symbol": "LINK-USDT-PERP",
        :param interval: "interval": "1m",
        :return:
        """
        target_payload = {"symbol": symbol, "interval": interval}
        try:
            last_one = self.dataDB[self.exchange].find_one(target_payload, {"_id": 0})['KLine'][-1]
        except:
            last_one = None

        self.dataDB[self.exchange].update_one({}, {"$set": {
            "KLine": [last_one]
        }})

        # print(self.dataDB[self.exchange].find_one(target_payload,{"_id":0})['KLine'])

        for LLL in range(len(list_data)):
            if len(list_data) > 500:
                target_len = 500
                self.dataDB[self.exchange].update_one(
                    target_payload,
                    {
                        "$push": {
                            "KLine": {
                                "$each": list_data[-target_len:],
                                "$position": 0
                            }
                        }
                    }
                )
                list_data = list_data[:-target_len]

            else:
                target_len = len(list_data)
                self.dataDB[self.exchange].update_one(
                    target_payload,
                    {
                        "$push": {
                            "KLine": {
                                "$each": list_data[-target_len:],
                                "$position": 0
                            }
                        }
                    }
                )
                print("{} Done.".format(symbol))
                break

    def date_generate(self, d_str):
        s = d_str.split('-')
        if int(s[2]) >= 28:
            s[2] = 28

        if int(s[1]) + 1 > 12:
            s[0] = int(s[0]) + 1
            s[1] = 1
            if s[1] < 10:
                s[1] = str("0{}".format(s[1]))
            else:
                s[1] = str(s[1])
        else:
            s[1] = int(s[1]) + 1
            if s[1] < 10:
                s[1] = str("0{}".format(s[1]))
            else:
                s[1] = str(s[1])
        return "{}-{}-{}".format(s[0], s[1], s[2])

    def refilled_all_symbol(self, if_recent=False):
        if if_recent:
            recent_str = "RECENT_MONTH"
        else:
            recent_str = "ALL"

        for single_symbol in self.sub_list:
            print("{} | {} | {}".format(
                single_symbol, self.sub_period_list, recent_str)
            )
            self.refilled_data(
                single_symbol,
                self.sub_period_list,
                recent_str
            )

    def generate_last_month_day_1(self, since_month_num=0):
        dt_now = datetime.now()
        str_month = int(dt_now.month)
        str_year = int(dt_now.year)
        year_count = 0
        for i in range(since_month_num):
            str_month = str_month - 1
            if str_month == 0:
                str_month = 12
                year_count = year_count + 1

            print(year_count, str_month)

        if str_month < 10:
            str_month = "0{}".format(str_month)

        else:
            str_month = "{}".format(str_month)

        str_year = str_year - year_count

        return "{}-{}-01".format(str_year, str_month)

    def refilled_symbol_single_period_kline(self, db_payload, sym, interval, begin_dt, end_dt):
        try:
            print('回補 幣安 {}   {} 資料'.format(sym, interval))
            needtofilled = self.binance_api.futures_historical_klines(
                sym,
                interval,
                int(datetime.strptime(begin_dt, "%Y-%m-%d").timestamp()) * 1000,
                int(datetime.strptime(end_dt, "%Y-%m-%d").timestamp()) * 1000
            )
            db_list = [self.generate_data_set(need) for index, need in enumerate(needtofilled)]

            print("refilled_symbol_single_period_kline  : {}".format(db_payload))
            # TODO 死在這邊，寫進去前要把時間戳切分開，寫入前先確認 db_payload 符合時間戳
            self.update_newest_interval_data(db_payload, interval, db_list)
            self.reload_data_check(sym, interval, 300)

        except Exception as e:
            self.update_newest_interval_data(db_payload, interval, [])
            print(e)
            pass

    def refilled_data(self, symbol, interval_list, refilled_time_type="ALL"):
        """
        :param symbol: "BTC-USDT-PERP"
        :param interval_list: ['1m', '15m']
        :param refilled_time_type:  "ALL" 回補所有時間，"RECENT_MONTH" 回補最近一個月
        :return:
        """
        # remove BTC-USDT-PERP to BTCUSDT
        history_symbol_name = symbol.replace("-PERP", "").replace("-", "")

        if refilled_time_type.upper() == "ALL":
            begin_time = [i[symbol] for i in self.init_data_list if symbol in i][0]
            month_round = (datetime.now() - datetime.strptime(begin_time, "%Y-%m-%d")).days // 30
            month_left = (datetime.now() - datetime.strptime(begin_time, "%Y-%m-%d")).days % 30
            if month_left > 0:
                month_round = month_round + 1


        elif refilled_time_type.upper() == "RECENT_MONTH":
            print("FILLED :RECENT_MONTH INTERVAL:{}".format(interval_list))
            month_round = 1
            begin_time = self.generate_last_month_day_1(month_round - 1)  # 0 = 最近一個月

        elif refilled_time_type.upper() == "LAST_2_MONTH":
            month_round = 2
            begin_time = self.generate_last_month_day_1(month_round - 1)  # 0 = 最近二個月

        elif refilled_time_type.upper() == "BEYOND_DAYS":
            begin_time = [i[symbol] for i in self.init_data_list if symbol in i][0]
            month_round = (datetime.now() - datetime.strptime(begin_time, "%Y-%m-%d")).days // 30
            month_left = (datetime.now() - datetime.strptime(begin_time, "%Y-%m-%d")).days % 30
            interval_list = ["1d", "1w", "1M"]
            if month_left > 0:
                month_round = month_round + 1

        # 跑每個月份
        for i in range(month_round):
            # 跑每個週期
            end_date = self.date_generate(begin_time)
            sp_date = begin_time.split('-')
            single_month_begin = "{}-{}-{}".format(sp_date[0], sp_date[1], "01")
            if int(sp_date[1]) + 1 < 10:
                ed_year = sp_date[0]
                ed_month = "0{}".format(int(sp_date[1]) + 1)

            elif int(sp_date[1]) + 1 <= 12 and int(sp_date[1]) + 1 >= 10:
                ed_year = sp_date[0]
                ed_month = "{}".format(int(sp_date[1]) + 1)

            # over an year
            if int(sp_date[1]) + 1 == 13:
                ed_year = int(sp_date[0]) + 1
                ed_month = "01"

            # elif int(sp_date[1])+1 <=12 and int(sp_date[1])+1 >=10:
            single_month_end = "{}-{}-{}".format(ed_year, ed_month, "01")

            target_payload = {
                "broker": self.feed,
                "symbol": symbol,
                "datetime": single_month_begin,
                "timestamp": int(datetime.strptime(single_month_begin, "%Y-%m-%d").timestamp()),

            }
            begin_time = end_date

            for each_interval in interval_list:
                print(each_interval)
                self.refilled_symbol_single_period_kline(
                    target_payload,
                    history_symbol_name,
                    each_interval,
                    single_month_begin,
                    single_month_end
                )

    def generate_barN_timestamp(self, interval=str):
        if "m" in interval:
            barN = int(interval.replace("m", "")) * 60  # 1m = 60S

        elif "h" in interval:
            barN = int(interval.replace("h", "")) * 60 * 60  # 1h = 60S *60min

        elif "d" in interval:
            barN = int(interval.replace("d", "")) * 60 * 60 * 24

        elif "w" in interval:
            barN = int(interval.replace("w", "")) * 60 * 60 * 24 * 7
        return barN

    def generate_data_set(sefl, data):
        return {
            "timestamp": int(data[0]),
            "data": {
                "open": float(Decimal(data[1])),
                "high": float(Decimal(data[2])),
                "low": float(Decimal(data[3])),
                "close": float(Decimal(data[4])),
                "vol": float(Decimal(data[5])),
                "trade": float(Decimal(data[8])),
                "start_time": int(data[0]),
                "end_time": int(data[6]),
                "closed": True
            }
        }

    def update_newest_interval_data(self, target_dict, interval, data):

        self.dataDB[self.feed].update_one(
            target_dict, {"$set": {'KLine.{}'.format(interval): data}}, upsert=True
        )
        print('update: {} {} DONE.'.format(target_dict['symbol'], interval))

    def reload_data_check(self, symbol, interval, check_len=500):
        """
        :param symbol: "ETHUSDT"
        :param interval: "1m"
        :param check_len: 500
        :return:
        """
        col = self.dataDB[self.feed]
        dt_now = datetime.now()
        if int(dt_now.month) < 10:
            str_month = "0{}".format(dt_now.month)
        else:
            str_month = "{}".format(dt_now.month)
        begin_time = "{}-{}-01".format(dt_now.year, str_month)
        old_data = []
        target_payload = {
            "broker": self.feed,
            "symbol": symbol,
            "datetime": begin_time
        }
        for sd in col.find({
            "broker": self.feed,
            "symbol": symbol
        }, {"_id": 0}).sort("timestamp"):
            old_data = old_data + sd['KLine'][interval]
        gap_time_UTC = None
        for index, dc in enumerate(old_data[-1 * check_len:]):
            if index >= 2 and old_data[index - check_len]['timestamp'] / 1000 - old_data[index - check_len - 1][
                'timestamp'] / 1000 > self.generate_barN_timestamp(interval):
                need_to_reload = (index >= 2 and old_data[index - check_len]['timestamp'] / 1000 -
                                  old_data[index - check_len - 1]['timestamp'] / 1000) / self.generate_barN_timestamp(
                    interval)
                target_index = index - check_len - 1
                gap_time = old_data[index - check_len - 1]['timestamp']
                gap_time_UTC = old_data[index - check_len - 1]['timestamp'] - 28800000

        if gap_time_UTC is not None:
            reload_data = self.binance_api.futures_historical_klines(
                symbol, interval, str(gap_time_UTC)
            )
            add_time = 0
            for get_data in reload_data:
                if get_data[0] > gap_time:
                    old_data.insert(target_index + 1, self.generate_data_set(get_data))
                    target_index = target_index + 1
                    add_time = add_time + 1
                    if add_time == int(need_to_reload) - 1:
                        break
            # TODO 這邊沒有裁切後寫入，故會寫進整個月份
            self.update_newest_interval_data(target_payload, interval, old_data)
        else:
            print('data completed.')

    def update_ram_data(self):
        for sin_name in self.target_symbol:
            self.ram_data[self.input_difficult_symbol_name[sin_name]] = {}
            for sin_period in self.sub_period_list:
                self.ram_data[self.input_difficult_symbol_name[sin_name]][sin_period] = self.get_history_data(sin_name,
                                                                                                              sin_period)

    def split_list(self, data_list, single_len):
        return [list(i) for i in np.array_split(data_list, len(data_list) / single_len)]

    def binance_symbol_list(self):
        return [i.replace("-PERP", "").replace("-", "") for i in self.sub_list]

    def add_new_data(self, symbol, interval, data):
        if len(self.ram_data[symbol]["{}".format(interval)]) >= 1:
            # print(data)
            if self.ram_data[symbol]["{}".format(interval)][-1]['timestamp'] == data['timestamp']:
                self.ram_data[symbol]["{}".format(interval)][-1] = data
            else:
                self.ram_data[symbol]["{}".format(interval)].append(data)
        else:
            self.ram_data[symbol]["{}".format(interval)].append(data)

    def get_history_data(self, symbol, interval):
        """
        將目標資料存進 ram_data

        :param symbol:  "BTC-USDT-PERP"
        :param interval:  "1m"
        :return:
        """
        history_data = []
        print("get history data: {}".format(symbol))
        for sd in self.dataDB[self.exchange.upper()].find({
            "broker": self.exchange.upper(),
            "symbol": symbol,
        }, {"_id": 0}).sort("timestamp"):
            try:
                history_data = history_data + sd['KLine'][interval]

            except Exception as e:
                pass

        # 資料不足需求長度，重補一次日K以上的資料
        print(interval, len(history_data), self.get_oldest_data(symbol))
        if len(history_data) < self.max_bars and (
                interval == "1d" or interval == "1w" or interval == "1M") and self.get_oldest_data(symbol):
            print('get full data')
            self.refilled_data(symbol, interval, "BEYOND_DAYS")
            for sd in self.dataDB[self.exchange.upper()].find({
                "broker": self.exchange.upper(),
                "symbol": symbol,
            }, {"_id": 0}).sort("timestamp"):
                try:
                    history_data = history_data + sd['KLine'][interval]

                except Exception as e:
                    print(e)

        return history_data

    def get_oldest_data(self, symbol):
        """

        :param symbol: need to be "ETH-USDT-PERP"
        :param interval:
        :return:
        """
        target = [i for i in self.dataDB[self.exchange.upper()].find({
            "broker": self.exchange.upper(),
            "symbol": symbol,
        }, {"_id": 0}).sort("timestamp", 1).limit(1)][0]['timestamp']

        for kk in self.init_data_list:
            if list(kk.keys())[0] == symbol:
                full_data_datetime = int(datetime.strptime(kk[symbol], "%Y-%m-%d").timestamp())
                break
        # print(target)
        # 如果你的日期(target)已經超過最大可拿資料(full_data_datetime)，那就表示已經補完不用再回補了
        if target <= full_data_datetime:
            return False
        else:
            return True

    def binance_KLine_handler(self, msg):
        # print(msg)
        # if msg['data']['k']['i']
        for restart_time in range(5):
            try:
                c = msg['data']['k']
                sub_period = c['i']
                sub_symbol = c['s']
                datapayload = {
                    "timestamp": int(c['t']),
                    "data": {
                        "open": float(Decimal(c['o'])),
                        "high": float(Decimal(c['h'])),
                        "low": float(Decimal(c['l'])),
                        "close": float(Decimal(c['c'])),
                        "vol": float(Decimal(c['v'])),
                        "trade": float(Decimal(c['n'])),
                        "start_time": int(c['t']),
                        "end_time": int(c['T']),
                        "closed": c['x']
                    }
                }
                self.add_new_data(sub_symbol, sub_period, datapayload)
                # if sub_period == '1d':
                #     print("{} | {}".format(sub_symbol,sub_period),
                #           [datetime.fromtimestamp(ok['timestamp'] / 1000) for ok in
                #                self.ram_data[sub_symbol][sub_period][-30:]],
                #           len(self.ram_data[sub_symbol][sub_period])
                #           )

            except Exception as e:
                if msg['e'] == 'error':
                    print(e)
                    print(msg)
                    self.main_socket()

    def get_currnet_month_db_str(self):
        dt_now = datetime.now()
        if int(dt_now.month) < 10:
            str_month = "0{}".format(dt_now.month)
        else:
            str_month = "{}".format(dt_now.month)
        return "{}-{}-01".format(dt_now.year, str_month)

    def get_Kline_number_one_month(self, interval):
        if "m" in interval:
            period_num = int(interval.replace("m", ""))
            barN = period_num * (60 // period_num) * 24 * 32

        elif "h" in interval:
            period_num = int(interval.replace("h", ""))
            barN = period_num * (24 // period_num) * 32

        elif "d" in interval:
            period_num = int(interval.replace("d", ""))
            barN = period_num * (32 // period_num)

        else:
            barN = 6

        return barN

    def get_single_symbol_month_livedata(self, symbol, interval):
        """
        get data from ram_data
        :param symbol:
        :param interval:
        :return:
        """
        # symbol = "BTCUSDT"
        current_month_timestamp = datetime.strptime(self.get_currnet_month_db_str(), "%Y-%m-%d").timestamp()
        # print(self.get_Kline_number_one_month(interval))
        target_len = self.get_Kline_number_one_month(interval)
        # print("before in: ", target_len, len(self.ram_data[symbol][interval]))

        for index, cut_data in enumerate(
                self.ram_data[symbol][interval][-1 * target_len:]):
            # print("{} | {}".format(interval, index), cut_data['timestamp'] / 1000, datetime.fromtimestamp(cut_data['timestamp'] / 1000), current_month_timestamp)
            if cut_data['timestamp'] / 1000 > current_month_timestamp:
                recent_data = self.ram_data[symbol][interval][-1 * target_len + index:]
                print(
                    interval,
                    index,
                    datetime.fromtimestamp(cut_data['timestamp'] / 1000),
                    datetime.fromtimestamp(current_month_timestamp),
                    len(recent_data)
                )
                # for inside_index, rrr in enumerate(recent_data):
                #     if rrr['timestamp'] / 1000 > current_month_timestamp:
                #         recent_data = recent_data[inside_index:]
                #         break
                #
                # try:
                #     print(datetime.fromtimestamp(recent_data[0]['timestamp']/1000))
                # except:
                #     print(recent_data)
                return recent_data

    def collect_symbol_data(self, symbol, single_month_begin):
        """

        :param symbol:
        :param single_month_begin:
        :return:
        """
        # symbol = "BTC-USDT-PERP"
        single_month_data_payload = {
            "broker": self.exchange,
            "symbol": symbol,
            "datetime": single_month_begin,
            "timestamp": int(datetime.strptime(single_month_begin, "%Y-%m-%d").timestamp()),
            "KLine": {
                '1m': [],
                '3m': [],
                '5m': [],
                '15m': [],
                '30m': [],
                '1h': [],
                '2h': [],
                '4h': [],
                '6h': [],
                '8h': [],
                '12h': [],
                '1d': [],
                '3d': [],
                '1w': [],
                '1M': [],
            }
        }
        for each_interval in self.sub_period_list:
            single_month_data_payload['KLine'][each_interval] = self.get_single_symbol_month_livedata(
                self.input_difficult_symbol_name[symbol],
                each_interval
            )
        # print(single_month_data_payload.keys())
        # print(single_month_data_payload['KLine']['1m'])
        return single_month_data_payload

    def stop_service_store_data(self):
        """
        Stop ws Service
        :return:
        """
        # if db status == 0:
        # stop socket and storing data of self.ram_data to each db_document
        this_month_str = self.get_currnet_month_db_str()
        for sin_name in self.target_symbol:
            # self.update_single_lastest_month_db_data(sin_name, this_month_str)
            threading.Thread(target=self.update_single_lastest_month_db_data, args=(sin_name, this_month_str,)).start()
        print("DONE")
        self.socket_db_switch = 0

    def stop_script(self):
        go_ws = ThreadedWebsocketManager()
        go_ws.stop()

    def update_single_lastest_month_db_data(self, symbol, month_str):
        self.dataDB[self.exchange].update_one(
            {
                "broker": self.feed,
                "symbol": symbol,
                "datetime": self.get_currnet_month_db_str(),
                "timestamp": int(datetime.strptime(self.get_currnet_month_db_str(), "%Y-%m-%d").timestamp())
            },
            {
                "$set": {
                    'KLine': self.collect_symbol_data(symbol, month_str)['KLine']
                }
            }, upsert=True
        )

    def generate_symbol_list(self):
        final_list = []
        for sym in self.target_symbol:
            for inter in self.sub_period_list:
                final_list.append("{}@kline_{}".format(self.input_difficult_symbol_name[sym].lower(), inter))
        return final_list

    def get_binance_kline_data(self, symbol, interval, start_timestamp):
        new_d = self.binance_api.futures_historical_klines(
            symbol,
            interval,
            start_timestamp,
        )
        return [self.generate_data_set(nd) for nd in new_d]

    def fill_latest_data_ram_data(self, symbol, interval, limit):
        total_len = len(self.ram_data[symbol][interval])
        # print(limit * -1)
        # print(len(self.ram_data[symbol][interval]))
        if limit >= len(self.ram_data[symbol][interval]):
            limit = len(self.ram_data[symbol][interval])
        start_timestamp = self.ram_data[symbol][interval][limit * -1]['timestamp']
        new_data = self.get_binance_kline_data(
            symbol, interval, str(start_timestamp - 28800000)
        )
        self.ram_data[symbol][interval] = self.ram_data[symbol][interval][:(total_len - limit)] + new_data

    def fill_all_latest_data_ram_data(self, limit):
        num_time = 0
        print("補足中間資料差值")
        for sym in self.target_symbol:
            for inter in self.sub_period_list:
                print("fill: {}  {}".format(sym, inter))
                self.fill_latest_data_ram_data(self.input_difficult_symbol_name[sym], inter, limit)
                # if num_time > 100:
                #     time.sleep(1.5)
                #     num_time = num_time - 100
                # threading.Thread(target=self.fill_latest_data_ram_data, args=(sym, inter, limit,))
                # num_time = num_time + 1

    def multi_socket_go(self, symbolList):
        go_ws = ThreadedWebsocketManager()
        if self.socket_db_switch == 1:
            go_ws.start()
            go_ws.start_futures_multiplex_socket(callback=self.binance_KLine_handler,
                                                 streams=list(symbolList))
            print(go_ws._socket_running)
            # print(go_ws._socket)
        while True:
            if self.socket_db_switch == 0:
                for name_stream in go_ws._socket_running:
                    go_ws.stop_socket(name_stream)
                    print("socket thread stop")
                    break
            elif self.socket_db_switch == -1:
                go_ws.stop()
                print("SSSS Stop")
                break
            time.sleep(1)

    def split_substream_list(self):
        all_multi_sym = set(self.generate_symbol_list())
        out_list = []
        inside_list = []
        for aaa in all_multi_sym:
            if len(inside_list) <= 198:
                inside_list.append(aaa)
            else:
                out_list.append(inside_list)
                inside_list = []
        out_list.append(inside_list)

        return out_list

    def main_socket(self):
        for out in self.split_substream_list():
            print("socket stream number: {}".format(len(out)))
            # print(out, len(out))

            threading.Thread(target=self.multi_socket_go, args=(out,)).start()

    # def get_daterange_kbar_num(self, start, end):
    #     return self.user.aggregate({ "$match" : { "timestamp" : { "$gt" : start, "$lte" : end}}})

    def date_range_pick(self):
        pass

    def get_history(self):
        pass

    def subscribe(self):
        pass

    def subscribe_callback(self):
        pass

    def get_all_data(self, symbol: str, period: str):
        # print("ram_data", self.ram_data)
        return [i["data"] for i in self.ram_data[symbol][period]]

    def open(self, symbol: str, period: str, index: int):
        return self.ram_data[symbol][period][(abs(index) * -1) - 1]['data']['open']

    def high(self, symbol: str, period: str, index: int):
        return self.ram_data[symbol][period][(abs(index) * -1) - 1]['data']['high']

    def low(self, symbol: str, period: str, index: int):
        return self.ram_data[symbol][period][(abs(index) * -1) - 1]['data']['low']

    def close(self, symbol: str, period: str, index: int):
        return self.ram_data[symbol][period][(abs(index) * -1) - 1]['data']['close']

    def vol(self, symbol: str, period: str, index: int):
        return self.ram_data[symbol][period][(abs(index) * -1) - 1]['data']['vol']

    def trade(self, symbol: str, period: str, index: int):
        return self.ram_data[symbol][period][(abs(index) * -1) - 1]['data']['trade']

    def bar_timestamp(self, symbol: str, period: str, index: int):
        return int(self.ram_data[symbol][(abs(index) * -1) - 1][period]['data']['start_time']) / 1000

    def bar_datetime(self, symbol: str, period: str, index: int):
        return datetime.fromtimestamp(
            int(self.ram_data[symbol][period][(abs(index) * -1) - 1]['data']['start_time']) / 1000)

    def bar_str_datetime(self, symbol: str, period: str, index: int):
        return datetime.strftime(
            datetime.fromtimestamp(
                int(self.ram_data[symbol][period]['data'][(abs(index) * -1) - 1]['start_time']) / 1000),
            "%Y-%m-%d_%H:%M:%S")

    def barstatus(self, symbol: str, period: str):
        return self.ram_data[symbol][period][-1]['data']['closed']

    def get_data(self, symbol: str, period: str, index: int):
        return self.ram_data[symbol][period][(abs(index) * -1) - 1]


if __name__ == "__main__":
    test_symbol_list = ["EOS-USDT-PERP", "BAL-USDT-PERP", "AAA-USDT-PERP"]
    target = Quotes('BINANCE_FUTURES', target_symbol=test_symbol_list)

    target.update_spot_exchange_info()
    # target.get_history_data("AAA-USDT-PERP", '15m')
    # target.main_socket()
    #
    # # socket 起完後補中間的差額資料
    # time.sleep(30)
    # target.fill_all_latest_data_ram_data(200)
    #
    # # 程式結束後，將中間存的資料都更新至DB內
    # time.sleep(60)
    # target.stop_service_store_data()
    #
    # # 重啟 ws
    # time.sleep(15)
    # target.socket_db_switch = 1
    # target.main_socket()
    # time.sleep(15)
    #
    # # 創立收行情資料的商品
    # test_sym = target.input_difficult_symbol_name[target.target_symbol[0]]
    # start_counting = 0
    # while True:
    #     time.sleep(1)
    #     start_counting += 1
    #     print(target.bar_datetime(test_sym, "1m", 1000), target.close(test_sym, "1m", 1000))
    #     print(target.bar_datetime(test_sym, "1m", 0), target.close(test_sym, "1m", 0))
    #     if start_counting > 30:
    #         target.stop_service_store_data()
    #         break
    #
    # target.socket_db_switch = -1
    print('Stop ALL.')