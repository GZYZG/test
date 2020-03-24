from jqdatasdk import *
from sqlalchemy import create_engine
from datetime import datetime,timedelta

from utils.tools import *
from utils.factor_data import FACTOR_CATEGORY_FACTORS

"""
    'pershare'
    'risk'
    'quality'
    'technical'
    'emotion'
    'growth'
    'style'
    'basics'
    'momentum'
"""
# 以下数据需要手动设置
FACTOR_CATEGORY = "quality" # 需要更新的因子种类
TODAY = datetime.today()
YEAR = TODAY.year
MONTH = TODAY.month
DAY = TODAY.day
# 设置起始日期
# START_DATE = datetime(2020, 3, 13) # 更新区间的其实日期，包含在区间内
# NOW = datetime(2020,3,19)   #更新区间的结束日期， 包含在区间内

#获取今天的日期
START_DATE = datetime(YEAR, MONTH, DAY)
NOW = datetime(YEAR, MONTH, DAY )


auth('15313851316', 'kdz235711')
count = get_query_count()   # 获取可调用次数
print(count)
if count['spare'] == 0:
    print("-----今日可调用次数已经用完！-----")
    exit(1)

# 以下数据不需手动设置
FACTOR_CATEGORY_TABLE_NAME = {
    'quality': 'quality_factor',
    'risk': 'risk_factor',
    'growth': 'growth_factor',
    'emotion': 'emotion_factor',
    'momentum': 'momentum_factor',
}   #因子种类与数据库表名的映射
# SINGLE_FACTORS = FACTOR_CATEGORY_FACTORS(FACTOR_CATEGORY)   # 使用 静态保存的质量因子列表
SINGLE_FACTORS = get_factors_of_category( FACTOR_CATEGORY )
COLUMNS = ['Ticker', 'As_Of_Date'] + SINGLE_FACTORS



from chinese_calendar import is_workday

# 存入aliyun数据库中
engine_iddb = create_engine(
        'mysql+pymysql://cn_ainvest_db:cn_ainvest_sd3a1@rm-2zewagytttzk6f24xno.mysql.rds.aliyuncs.com:3306/iddb')
# 存入aws数据库中
# engine_orgin_data = create_engine(
#     'mysql+pymysql://ainvest_666:ainvest_aws_123@ainvest.cvl4joshuam5.rds.cn-north-1.amazonaws.com.cn:3306/orgin_data')

startdate = START_DATE
# now是当前正要抓取的信息的日期
now = NOW
while startdate <= now:
    #如果now不是中国的工作日则不添加
    if not is_workday(now):
        now -= timedelta(days=1)
        continue

    # 获取 交易日为 now 的正常上市公司的股票列表
    # securities 用于 get_factor_values 的查询
    # _securities 用于存入数据库
    securities = get_on_trade_stock_list(now)
    _securities = list(
        map(lambda x: 'SH' + x[0:-5] if x[0] == '6' or x[0] == '5' else  'SZ' + x[0:-5], securities))

    # 由于使用 get_factor_values 获取因子信息时有请求数据量的限制，所以进行分批次请求
    securities_partitions = partition(securities)
    _securities_partitions = partition(_securities)

    #分批次提取数据
    for id, sec in enumerate(securities_partitions):
        df3 = extract_one_day_data(sec, _securities_partitions[id], SINGLE_FACTORS, COLUMNS, now )
        if df3 is None:
            continue
        try:
            df3.to_sql(FACTOR_CATEGORY_TABLE_NAME[FACTOR_CATEGORY], engine_iddb, if_exists='append', index=False)
            print('aliyun存储完毕 --- ', now.strftime('%Y-%m-%d'), " 批次-",id)
            print('-----------------')
        except Exception as e:
            print(now.strftime('%Y-%m-%d'), " 批次-",id, '----', e)
            print('------------------------------------')

    # 日期减一天
    now -= timedelta(days=1)

#检查数据
# import pandas as pd
# factor_data_dict = get_factor_values(securities=['600000.XSHG'], factors=['net_profit_to_total_operate_revenue_ttm'], start_date='2019-12-19', end_date='2019-12-19')
# #print(factor_data_dict)  ####!!!!!!!!!!!!!
# a = factor_data_dict['net_profit_to_total_operate_revenue_ttm']
# df2 = pd.DataFrame()
# df2['net_profit_to_total_operate_revenue_ttm']=a['600000.XSHG']
# df2.reset_index(inplace=True,drop=True)
# df2.head()