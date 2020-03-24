import tushare as ts
from jqdatasdk import get_factor_values, get_all_factors,auth
import pandas as pd

def get_current_on_trade_stock_list(token='1e32f37b003ea0d5f24994f085a1f47c982af5c4a365bcedb08a0e7d')->'list':
    """
    获取当前所有正常上市交易的股票列表
    :param token: ts 的token
    :return: 当前所有正常上市交易的股票列表
    """
    # 获取股票列表
    token = '1e32f37b003ea0d5f24994f085a1f47c982af5c4a365bcedb08a0e7d'
    ts.set_token(token)  # 可忽略
    pro = ts.pro_api()
    data = pro.stock_basic(exchange='', list_status='L', fields='symbol')
    return data['symbol'].map( lambda x: x + '.XSHG' if x[0] == '6' or x[0] == '5' else x + '.XSHE' ).tolist()

def get_on_trade_stock_list(date, token='1e32f37b003ea0d5f24994f085a1f47c982af5c4a365bcedb08a0e7d'):
    """
    使用 tushare 的获取股票日线行情来获取某一天的正常上市股票列表
    :param date: 交易日期
    :param token:
    :return:
    """
    token = '1e32f37b003ea0d5f24994f085a1f47c982af5c4a365bcedb08a0e7d'
    ts.set_token(token)  # 可忽略
    pro = ts.pro_api()
    df = pro.daily(trade_date=date.strftime('%Y%m%d'))
    return df['ts_code'].map(lambda x: x[0:-3] + '.XSHG' if x[0] == '6' or x[0] == '5' else x[0:-3] + '.XSHE').tolist()
    pass

def partition(all_secs, batch_size=1000):
    """
    对一个列表进行划分
    :param all_secs: 待划分的列表
    :param batch_size: 分片的大小
    :return: 包含所有分片的列表
    """
    return [all_secs[i:i+batch_size] for i in range(0, len(all_secs), batch_size)]


def extract_one_day_data(secs, _secs, facs, cols, day):
    """
    提取指定日期的 secs中的股票的 facs 因子值，并生成DataFrame
    :param secs: 要获取的股票代码
    :param _secs: 返回的DataFrame中的 Ticker 的值
    :param facs: 要获取的单因子
    :param cols: 返回的DataFrame 的列
    :param day: 日期
    :return: DataFrame
    """
    today = day.strftime('%Y-%m-%d')
    # 获取一天全部数据的字典
    facs_val_dict = get_factor_values(securities=secs, factors=facs, start_date=today, end_date=today)
    # 创建空Dateframe
    df = pd.DataFrame(columns=cols )
    df['Ticker'] = _secs
    df['As_Of_Date'] = [day ] * len(_secs)

    try:
        for i in facs:
            # 获取一个因子数据列表
            fac_list = facs_val_dict[i].values.tolist()[0]
            # print('factor',i,len(factor_list))
            df[i] = fac_list
    except Exception as e:
        print(day, '----', e)
        print('------------------------------------')
        return None
    return df
    pass

def get_factors_of_category(category:'str')->list:
    """
    获取指定类型的因子所包含的单因子
    :param category: 因子种类
    :return: 单因子列表
    """
    all_factors = get_all_factors()
    quality_factor = all_factors[
        all_factors.category.str.contains(category, case=True, flags=0, na=False, regex=True)]
    return quality_factor['factor'].tolist()

    pass

if __name__ == "__main__":
    # print(get_current_on_trade_stock_list())
    # from datetime import datetime, timedelta
    # now = datetime(2020,3,11)
    # s = datetime(2019,12,19)
    # while s <= now:
    #     l = len(get_on_trade_stock_list(now))
    #     print("{} {}\n{}".format(now, l, '-'*20))
    #     now -= timedelta(days=1)
#     auth('15313851316', 'kdz235711')
#     cls = ['pershare',
# 'risk',
# 'quality',
# 'technical',
# 'emotion',
# 'growth',
# 'style',
# 'basics',
# 'momentum']
#     for c in cls:
#         print(c)
#         print("{} : \n{}\n{}".format(c, get_factors_of_category(c), '-'*20))
    pass