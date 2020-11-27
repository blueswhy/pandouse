from pandouse.util.funcs import *


def load_data():
    csv_file = '/data/ark/rawdata/choice/stock/b_stk_bar_5min/20201124.csv'
    HandleCH.load_csv('b_stk_bar_5min', csv_file)


if __name__ == '__main__':
    load_data()