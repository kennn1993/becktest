import importlib
from modules import *
from TradingService import TradingService
from PerformanceReport import PerformanceReport
from Quotes import *
import pandas as pd
from copy import deepcopy


class BackTesting:

    def __init__(self, ea_class_name, symbol, paratemers_dict, is_opt=False, maxbarback=30):
        print(symbol)
        self.symbol = symbol
        self.quotes = Quotes("BINANCE_FUTURES", symbol, maxbarback)
        history_dict = self.quotes.get_all_data(self.quotes.input_difficult_symbol_name[symbol], "1d")
        print(history_dict)
        self.history_quotes = load_history(history_dict) #load_csv(csv_path)
        self.paratemers_dict = paratemers_dict
        self.ea_class_name = ea_class_name
        self.is_opt = is_opt
        self.maxbarback = maxbarback

    # 產生最佳化參數與步進值
    def convert_for_opt(self):
        start = self.paratemers_dict["start"]
        step = self.paratemers_dict["step"]
        end = self.paratemers_dict["end"]
        result = []
        prepare = {}
        for index, paratemer in enumerate(start):
            if self.is_opt:
                prepare[index] = [
                    paratemer,
                    end[index],
                    step[index]
                ]
            else:
                prepare[index] = [
                    paratemer,
                    paratemer,
                    0
                ]

        # 步驟4.參數組合走訪
        # product(*iterables)參數說明:
        # iterables:先用b()函式分別包住步驟3所設定的參數，然後放入product()
        keys = list(prepare.keys())
        for p in self._product(prepare):
            input_index = 0
            tmp_result = {}
            for v in p:
                key = keys[input_index]
                tmp_result[key] = v
                input_index += 1
            result.append(tmp_result)

        return result

    # 暴力破解內建函式區
    def _product_core(self, result, input):
        # [[],[]]
        if len(input) == 0:
            yield result
        else:
            for item in input[0]:
                yield from self._product_core(result + [item], input[1:])

    def _product(self, input):
        # {'distance':[3,10,1], 'break':[5,20,5]}
        new_input = []
        for k, v in input.items():
            new_list = []
            if v[2] == 0:
                new_input.append([v[0]])
            else:
                for value in range(v[0], v[1] + 1, v[2]):
                    new_list.append(value)
                new_input.append(new_list)
        return self._product_core([], new_input)

    def run(self):
        performance_report_instance = PerformanceReport(self.quotes.input_difficult_symbol_name[self.symbol])
        strategy_parameters_list = self.convert_for_opt()

        for parameters_list in strategy_parameters_list:
            self.parameters_list = []
            for key in parameters_list:
                self.parameters_list.append(parameters_list[key])

            ea = self.import_ea_class(self.ea_class_name, self.history_quotes["quotes_df"])
            service = TradingService(self.history_quotes, trade_type="backtesting", maxbarback=self.maxbarback,
                                     graphing=True, trading_detail=True)
            service.load_strategy(ea)
            service.start()
            performance_report_instance.set_data({
                "order_list": service.get_order_list(),
                "info": service.get_info(),
                "trade_position": service.get_trade_position(),
                "quotes_df": service.get_quotes_df(),
                "indicators": service.get_indicators()
            })
            if self.is_opt:
                # 填最佳化報表與其參數的值  , value = [a, b] = 取決參數長度
                performance_report_instance.append_report_df(
                    performance_report_instance.df_result(
                        performance_report_instance.get_report(
                            performance_report_instance.report_col()
                        )
                        , self.parameters_list
                    )
                )

        if self.is_opt:
            performance_report_instance.set_opt_data()
            performance_report_instance.show_opt()
        else:
            performance_report_instance.show(True, True)

    def import_ea_class(self, ea_class_name, history_quotes):
        file = ("strategies", ea_class_name)
        path = '.'.join(file)
        ea = getattr(importlib.import_module(path), ea_class_name)
        return ea(self.parameters_list, history_quotes)

# if __name__ == '__main__':
# history_quotes = load_csv("C:\\Users\\Hung\\PycharmProjects\\PortfolioTesting\\2330台積電.csv")
# bt = BackTesting("TestEA", [12, 26, 9], "C:\\Users\\Hung\\PycharmProjects\\PortfolioTesting\\2330台積電.csv")
# bt.run()
