import concurrent.futures
from sqlalchemy import create_engine,  text
import pandas as pd
import numpy as np
import threading
import os
from openai import OpenAI
import FUNCS.DATA_PROCESS as DP
import GD


user = os.getenv("DB_USER")  # 无默认值，不存在时返回 None
host = os.getenv("DB_HOST")  # 有默认值，不存在时返回 "localhost"
port = os.getenv("DB_PORT")
password = os.getenv("DB_PASSWORD")
name = os.getenv("DB_NAME")
database = os.getnv("DB_DATABASE")


def qwen(question):
    client = OpenAI(
        # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx",
        api_key="skXXX",
        # 如何获取API Key：https://help.aliyun.com/zh/model-studio/developer-reference/get-api-key
        base_url="XXX"
    )

    completion = client.chat.completions.create(
        model="qwen-turbo",  # 此处以 deepseek-r1 为例，可按需更换模型名称。
        messages=[
            {'role': 'user', 'content': f'{question}'}
        ]
    )
    print("最终答案：")
    print(completion.choices[0].message.content)
    return completion.choices[0].message.content


# def data_select():
#     engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
#     df_merge = pd.read_sql(
#         '''
#         SELECT  b.point
#         FROM revenue_address ra
#         RIGHT JOIN (
#             SELECT CONCAT(发货地点, 到货地点) AS point
#             FROM business
#         ) b ON ra.point = b.point
#         where point_standard is null
#         ;
#
#         ''', con=engine
#     )
#     print('查询完成')
#     return df_merge


def data_select():
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df_merge = pd.read_sql(
        '''
        SELECT 发货地点 AS point
        FROM business 
        WHERE 日期 LIKE "202509%%"
        UNION
        SELECT 到货地址  AS point
        FROM car
        UNION
        SELECT CONCAT(省, 市, 县区) AS point
        FROM car
        WHERE 日期 LIKE "202509%%"
        ;
        ''', con=engine
    )
    print('数据库提取成功')
    df_merge = df_merge[~df_merge['point'].isnull()]
    df_merge['point'] = df_merge['point'].str.replace(' ', '')
    df_merge['point'] = df_merge['point'].apply(DP.full_to_half)
    df_point_cleaned = pd.read_sql('SELECT point FROM revenue_address', con=engine)
    df_merge = df_merge[~df_merge['point'].isin(df_point_cleaned['point'])]
    df_merge = df_merge.drop_duplicates()
    print('清洗成功')
    df_merge['point_（'] = df_merge['point'].str.split('（', n=1).str[1]
    df_merge['point_num_no'] = df_merge['point'].str.replace(r'[\d]|[^\u4e00-\u9fa5a-zA-Z\s/-]', '', regex=True)
    df_merge['point_slash'] = df_merge['point'].str.split('/', n=1).str[0]
    df_merge['point_space'] = df_merge['point'].str.split(' ', n=1).str[0]
    df_merge['point_-'] = df_merge['point'].str.split('-', n=1).str[0]
    df_merge['point_slash_num_no'] = df_merge['point_num_no'].str.split('/', n=1).str[0]
    df_merge['point_space_num_no'] = df_merge['point_num_no'].str.split(' ', n=1).str[0]
    df_merge['point_slash_space_num_no'] = df_merge['point_slash_num_no'].str.split(' ', n=1).str[0]
    print('查询完成')
    return df_merge


def data_select_ai():
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df_merge = pd.read_sql(
        '''
        SELECT 发货地点 AS point
        FROM business
        WHERE 发货地点 NOT IN (SELECT point FROM revenue_address)
        UNION
        SELECT 到货地址  AS point
        FROM car
        WHERE 到货地址 NOT IN (SELECT point FROM revenue_address)
        UNION
        SELECT CONCAT(省, 市, 县区) AS point
        FROM car
        WHERE CONCAT(省, 市, 县区) NOT IN (SELECT point FROM revenue_address)
        ;
        ''', con=engine
    )

    print('查询完成')
    return df_merge


def data_select_2():
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df_merge = pd.read_sql(
        '''
            SELECT 发货地点 AS point FROM revenue_old
            UNION
            SELECT 到货地点 FROM revenue_old

        ''', con=engine
    )

    return df_merge


def data_select_manual(path):
    df = pd.read_excel(path)
    return df


def product_start():
    point = {'point': [('广东省,茂名市,高州市'),
                       ('福建省,漳州市,漳浦县'),
                       ('吉林省,长春市,九台区'),
                       ('新疆维吾尔自治区,乌鲁木齐市,沙依巴克区'),
                       ('四川省,成都市,龙泉驿区'),
                       ('天津市,天津市,滨海新区'),
                       ('湖南省,长沙市,宁乡市'),
                       ('安徽省,六安市,金安区'),
                       ('浙江省,杭州市,钱塘区'),
                       ('陕西省,西安市,雁塔区'),
                       ('浙江省,绍兴市,新昌县'),
                       ('浙江省,衢州市,衢江区')]}
    point = pd.DataFrame(point)
    return point


def coordination_point_num_no(df):
    df = df.fillna('/')
    db_lock = threading.Lock()
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df[['point_standard', 'coordination']] = df.apply(
        lambda x: GD.gaode().coordination_point(GD.gaode().geo_data(x['point_num_no'])), axis=1
    )
    df[['longitude', 'latitude']] = df['coordination'].str.split(',', expand=True)
    df['point_confirmed'] = '/'
    df = df[['point', 'point_standard', 'point_confirmed', 'longitude', 'latitude']]
    with db_lock:
        df.to_sql('revenue_address', con=engine, if_exists='append', index=False,
                  )
    print(df)


def Thread_to_sql_num_no(data):
    df_merge = data
    df_chunks = np.array_split(df_merge, 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for chunk in df_chunks:
            future = executor.submit(coordination_point_num_no, chunk)  # 直接传递数据块
            futures.append(future)
        concurrent.futures.wait(futures)


def coordination_point_clean(df, column):
    df = df.fillna('/')
    db_lock = threading.Lock()
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df[['point_standard', 'coordination']] = df.apply(
        lambda x: GD.gaode().coordination_point(GD.gaode().geo_data(x[f'{column}'])), axis=1
    )
    df[['longitude', 'latitude']] = df['coordination'].str.split(',', expand=True)
    df['point_confirmed'] = '/'
    df = df[['point', 'point_standard', 'point_confirmed', 'longitude', 'latitude']]
    with db_lock:
        df.to_sql('revenue_address', con=engine, if_exists='append', index=False,
                  )
    print(df)


def Thread_to_sql_clean(data, column):
    df_merge = data
    df_chunks = np.array_split(df_merge, 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for chunk in df_chunks:
            future = executor.submit(coordination_point_clean, chunk, column)  # 直接传递数据块
            futures.append(future)
        concurrent.futures.wait(futures)


def Thread_to_sql_ai(data, column):
    df_merge = data
    df_merge['point_ai'] = df_merge.apply(lambda x: qwen('清洗地址，传回纯地址，不要有其他内容' + x['point']), axis=1)
    df_chunks = np.array_split(df_merge, 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for chunk in df_chunks:
            future = executor.submit(coordination_point_clean, chunk, column)  # 直接传递数据块
            futures.append(future)
        concurrent.futures.wait(futures)


def coordination_point_slash(df):
    df = df.fillna('/')
    db_lock = threading.Lock()
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df[['point_standard', 'coordination']] = df.apply(
        lambda x: GD.gaode().coordination_point(GD.gaode().geo_data(x['point_slash'])), axis=1
    )
    df[['longitude', 'latitude']] = df['coordination'].str.split(',', expand=True)
    df['point_confirmed'] = '/'
    df = df[['point', 'point_standard', 'point_confirmed', 'longitude', 'latitude']]
    with db_lock:
        df.to_sql('revenue_address', con=engine, if_exists='append', index=False,
                  )
    print(df)


def Thread_to_sql_slash(data):
    df_merge = data
    df_chunks = np.array_split(df_merge, 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for chunk in df_chunks:
            future = executor.submit(coordination_point_slash, chunk)  # 直接传递数据块
            futures.append(future)
        concurrent.futures.wait(futures)


def coordination_point_slash_num_no(df):
    df = df.fillna('/')
    db_lock = threading.Lock()
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df[['point_standard', 'coordination']] = df.apply(
        lambda x: GD.gaode().coordination_point(GD.gaode().geo_data(x['point_slash_num_no'])), axis=1
    )
    df[['longitude', 'latitude']] = df['coordination'].str.split(',', expand=True)
    df['point_confirmed'] = '/'
    df = df[['point', 'point_standard', 'point_confirmed', 'longitude', 'latitude']]
    with db_lock:
        df.to_sql('revenue_address', con=engine, if_exists='append', index=False,
                  )
    print(df)


def Thread_to_sql_slash_num_no(data):
    df_merge = data
    df_chunks = np.array_split(df_merge, 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for chunk in df_chunks:
            future = executor.submit(coordination_point_slash_num_no, chunk)  # 直接传递数据块
            futures.append(future)
        concurrent.futures.wait(futures)


def coordination_point_space_num_no(df):
    df = df.fillna('/')
    db_lock = threading.Lock()
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df[['point_standard', 'coordination']] = df.apply(
        lambda x: GD.gaode().coordination_point(GD.gaode().geo_data(x['point_space_num_no'])), axis=1
    )
    df[['longitude', 'latitude']] = df['coordination'].str.split(',', expand=True)
    df['point_confirmed'] = '/'
    df = df[['point', 'point_standard', 'point_confirmed', 'longitude', 'latitude']]
    with db_lock:
        df.to_sql('revenue_address', con=engine, if_exists='append', index=False,
                  )
    print(df)


def Thread_to_sql_space_num_no(data):
    df_merge = data
    df_chunks = np.array_split(df_merge, 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for chunk in df_chunks:
            future = executor.submit(coordination_point_space_num_no, chunk)  # 直接传递数据块
            futures.append(future)
        concurrent.futures.wait(futures)


def coordination_point_slash_space_num_no(df):
    df = df.fillna('/')
    db_lock = threading.Lock()
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df[['point_standard', 'coordination']] = df.apply(
        lambda x: GD.gaode().coordination_point(GD.gaode().geo_data(x['point_slash_space_num_no'])), axis=1
    )
    df[['longitude', 'latitude']] = df['coordination'].str.split(',', expand=True)
    df['point_confirmed'] = '/'
    df = df[['point', 'point_standard', 'point_confirmed', 'longitude', 'latitude']]
    with db_lock:
        df.to_sql('revenue_address', con=engine, if_exists='append', index=False,
                  )
    print(df)


def Thread_to_sql_slash_space_num_no(data):
    df_merge = data
    df_chunks = np.array_split(df_merge, 16)
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        futures = []
        for chunk in df_chunks:
            future = executor.submit(coordination_point_slash_space_num_no, chunk)  # 直接传递数据块
            futures.append(future)
        concurrent.futures.wait(futures)


def deleto():
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')

    with engine.connect() as connection:
        delete_query = text("""
        DELETE FROM revenue_address 
        WHERE longitude LIKE :pattern
        """)
        result = connection.execute(delete_query, {"pattern": "%未%"})
        # result = connection.execute(delete_query, {"pattern": "/"})
        print(f"Deleted rows with '未' in longitude: {result.rowcount}")
        connection.commit()

        remove_duplicates_query = text("""

        DELETE FROM revenue_address 
        WHERE id NOT IN (
        SELECT id FROM (  -- 增加这一层子查询包装
            SELECT MAX(id) as id
            FROM revenue_address
            GROUP BY point  -- 按你的分组条件
        ) AS temp_table
        )
        """)

        result = connection.execute(remove_duplicates_query)
        connection.commit()
        print(f"Deleted duplicate rows based on point: {result.rowcount}")


def address_manual():
    df_address = pd.read_excel(r'E:\MK\数据源\地址高德\新建 XLSX 工作表.xlsx')
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    with engine.connect() as connection:
        connection.execute(text('DELETE FROM revenue_address_no_gis'))
        connection.commit()
    df_address.to_sql('revenue_address_no_gis', con=engine, if_exists='append', index=False,
                      )
    # 5. 删除临时表（可选）
    # engine.execute(f"DROP TABLE IF EXISTS {temp_table_name}")
    print("revenue_address_no_gis更新完成")


def address_problem_delete(path):
    try:
        # 创建数据库连接
        engine = create_engine(
            f'mysql+pymysql://{user}:{password}@{host}:{port}/{name}'
        )

        # 读取Excel文件中的point_standard_y列
        df_problem = pd.read_excel(path)
        # 去除空值和重复值
        points_to_delete = df_problem['point_y'].dropna().unique()

        if len(points_to_delete) == 0:
            print("没有找到需要删除的地址数据")
            return

        # 生成与points_to_delete数量匹配的命名占位符
        placeholders = ", ".join([f":param{i}" for i in range(len(points_to_delete))])
        delete_query = text(f"""
            DELETE FROM revenue_address 
            WHERE point IN ({placeholders})
        """)

        # 创建参数字典，键为占位符名称，值为对应的数据
        params = {f"param{i}": point for i, point in enumerate(points_to_delete)}

        with engine.connect() as conn:
            trans = conn.begin()
            try:
                # 使用字典传递参数
                result = conn.execute(delete_query, params)
                trans.commit()
                print(f"成功删除了 {result.rowcount} 条记录")
            except Exception as e:
                trans.rollback()
                print(f"删除操作失败，已回滚: {str(e)}")

    except Exception as e:
        print(f"操作出错: {str(e)}")


def all_to_half():
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    df_address = pd.read_sql('SELECT * FROM revenue_address', con=engine)
    df_address['point'] = df_address['point'].apply(DP.full_to_half)
    df_address.to_excel(r'e:/1.xlsx')
    print(df_address['point'])



