import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from DATABASE import to_sql as s
import FUNCS.FUNCS as FK
import FUNCS.DATA_PROCESS as DP
import os

user = os.getenv("DB_USER")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
password = os.getenv("DB_PASSWORD")
name = os.getenv("DB_NAME")


BOUNDS = {
    'min_lon': 73.5,
    'max_lon': 135.0,
    'min_lat': 18.0,
    'max_lat': 53.5
}
GRID_SIZE = 1 # 10x10=100个网格
MAX_DISTANCE_KM = 50


# --------------------------
# 核心功能实现
# --------------------------

def haversine_vectorized(df):
    """向量化Haversine距离计算"""
    R = 6371  # 地球半径（千米）

    # 将角度转换为弧度
    lat1 = np.radians(df['latitude_a'])
    lon1 = np.radians(df['longitude_a'])
    lat2 = np.radians(df['latitude_b'])
    lon2 = np.radians(df['longitude_b'])

    # 计算差值
    dlon = lon2 - lon1
    dlat = lat2 - lat1

    # Haversine公式向量化计算
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return R * c


def generate_grids(df):
    """生成网格索引"""
    # 计算网格间隔
    lon_interval = (BOUNDS['max_lon'] - BOUNDS['min_lon']) / GRID_SIZE
    lat_interval = (BOUNDS['max_lat'] - BOUNDS['min_lat']) / GRID_SIZE

    # 计算网格索引
    df['grid_i'] = ((df['longitude'] - BOUNDS['min_lon']) // lon_interval).clip(0, GRID_SIZE - 1).astype(int)
    df['grid_j'] = ((df['latitude'] - BOUNDS['min_lat']) // lat_interval).clip(0, GRID_SIZE - 1).astype(int)
    return df


def find_neighbors(grid_i, grid_j):
    """生成相邻网格索引"""
    neighbors = []
    for di in [-1, 0, 1]:
        for dj in [-1, 0, 1]:
            ni = grid_i + di
            nj = grid_j + dj
            if 0 <= ni <= GRID_SIZE and 0 <= nj <= GRID_SIZE or grid_i == grid_j:
                neighbors.append((ni, nj))
    # df_neighbors = pd.DataFrame(neighbors)
    # df_neighbors.to_excel(r'E:\nei.xlsx')
    return neighbors



def find_nearby_pairs(df):
    """主处理函数"""
    # 生成网格索引
    df = generate_grids(df)

    # 为每个点生成相邻网格
    df['neighbors'] = df.apply(lambda x: find_neighbors(x.grid_i, x.grid_j), axis=1)
    print(df['neighbors'])
    # 展开相邻网格
    exploded = df.explode('neighbors')
    exploded[['neighbor_i', 'neighbor_j']] = pd.DataFrame(
        exploded['neighbors'].tolist(), index=exploded.index
    )
    print(df.columns)
    # 自连接查询相邻网格中的点
    merged = pd.merge(
        df,
        exploded,
        left_on=['grid_i', 'grid_j'],
        right_on=['neighbor_i', 'neighbor_j'],
        suffixes=('_a', '_b')
    )
    # 过滤相同点的比较和重复对
    merged = merged[merged['id_a'] < merged['id_b']]

    merged['distance'] = haversine_vectorized(merged)

    result = merged[(merged['distance'] <= MAX_DISTANCE_KM)]

    return result[['id_a', 'id_b', 'distance']]


# --------------------------
# 使用示例
# --------------------------

# 生成测试数据
data = {
    'id': range(1, 12),
    'lon': [116.4074,116.4074, 121.4737, 113.2644, 114.3055, 108.9386,
            103.8342, 126.5497, 112.9388, 120.1551, 117.2008],
    'lat': [39.9042,39.9042, 31.2304, 23.1291, 30.5928, 34.2623,
            36.0608, 45.8022, 28.2282, 30.2741, 39.0841]
}

def data_select(year,months):
    from sqlalchemy import create_engine
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{name}')
    df_point = pd.read_sql('SELECT id, point, point_standard, longitude, latitude FROM revenue_address',con=engine)
    df_x_y = pd.read_sql(f'SELECT 发运单号, 发货地点, 到货地点, 公司, 业务类型, 业务日期 FROM revenue_old WHERE 业务日期 LIKE "%%{year}%%"',con=engine).drop_duplicates()
    df_car = pd.read_sql(f'SELECT 发运单号, 省, 市, 县区 FROM car WHERE 日期 LIKE "%%{year}%%"',con=engine)
    df_car['省市县'] = df_car['省'] + df_car['市'] + df_car['县区']
    df_x_y = pd.merge(df_x_y,df_car,on='发运单号',how='left')
    df_x_y['业务日期'] = pd.to_datetime(df_x_y['业务日期'],format='%Y%m%d')
    df_x_y = df_x_y[df_x_y['业务日期'].dt.month.isin(months)]
    # df_x_y = df_x_y[df_x_y['业务日期'].dt.day.isin([1,2,3,4,5,6,7])]
    print('数据行数：')
    print(df_x_y.shape[0])
    # df_x_y['point_match'] = df_x_y.apply(lambda x: DP.point_match(x['省'], x['省市县'], x['到货地点']), axis=1)
    df_x_y = pd.merge(df_x_y, df_point, left_on='发货地点', right_on='point', how='left')
    df_x_y = pd.merge(df_x_y, df_point, left_on='到货地点', right_on='point', how='left')
    df_start = df_x_y[['发货地点']].drop_duplicates()
    df_end = df_x_y[['到货地点']].drop_duplicates()
    df_start = df_start.rename(columns={'发货地点': 'point'})
    df_end = df_end.rename(columns={'到货地点': 'point'})
    df_point_transported = pd.concat([df_start, df_end], ignore_index=True)
    df_point = df_point[df_point['point'].isin(df_point_transported['point'])]
    df_point = df_point[df_point['latitude']!='/']
    df_point['latitude'] = df_point['latitude'].astype(float)
    df_point['longitude'] = df_point['longitude'].astype(float)
    print(df_point.columns)
    print('地址数量:')
    print(df_point.shape[0])
    return df_point

def haversine_all(df):
    1

def calculate(df):
    df_address = df
    # df_address = df_address[df_address['point'].str.contains('钱塘')|df_address['point'].str.contains('下沙')]
    # df_address = df_address[(df_address['id']==3428)|(df_address['id']==47939)]
    # df_address = df_address[0:100000]
    # df = df.drop_duplicates(['latitude','longitude'])
    df_address_nodup = df_address.drop_duplicates(['point'])
    # df_address_nodup = df_address.drop_duplicates(['latitude', 'longitude'])
    print(df_address_nodup.columns)
    # 执行查询
    result = find_nearby_pairs(df_address_nodup)
    print(result.columns)
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{name}')

    # df_merge = pd.merge(result,df,left_on='id_a',right_on='id',how='left')
    # df_merge = pd.merge(df_merge, df, left_on='id_b', right_on='id', how='left')
    s.tosql().drop_s(['near','near_latlon'])
    # df['neighbors'] = df['neighbors'].astype(str)
    # df_address = df_address[['id','latitude','longitude']]
    # merge = pd.merge(df_address,result,left_on='id',right_on='id_a',how='right')
    # merge = pd.merge(merge, result, left_on='id', right_on='id_b', how='right')
    # merge_2 = pd.merge(df_address,merge,on=['latitude','longitude'],how='left')
    # merge_2.to_sql('near1',con=engine,if_exists='append')
    print(result)
    print(df_address_nodup)
    df_address_nodup = df_address.drop_duplicates(['latitude', 'longitude'])
    print(df_address_nodup)
    result = pd.merge(result,df_address_nodup,left_on='id_a',right_on='id',how='left')
    result = pd.merge(result, df_address_nodup, left_on='id_b', right_on='id', how='left')
    # metadata = MetaData()
    # locations = Table('near_latlon', metadata, autoload_with=engine)
    # # 反射表结构（如果事先不知道表结构）
    # metadata.reflect(bind=engine)
    # delete_stmt = locations.delete()
    # # 执行删除操作
    # with engine.connect() as connection:
    #     result = connection.execute(delete_stmt)
    #     print(f"已删除 {result.rowcount} 条记录")
    result.to_sql('near_latlon',con=engine,if_exists='append')
    print(result.columns)
    # 结果展示
    print(f"找到 {len(result)} 对邻近坐标点：")
    print(result.head())

def data_merge():
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{name}')
    df_point = pd.read_sql('SELECT point, point_standard, longitude, latitude FROM revenue_address limit 10000',
                           con=engine)
    df_near = pd.read_sql('SELECT * FROM near',con=engine)

#
# calculate(data_select(2025,[5]))
# data_select(2025,[5])

def haversine(lon1, lat1, lon2, lat2):
    """计算两个经纬度坐标之间的Haversine距离（单位：公里）"""
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = np.sin(dlat / 2) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2) ** 2
    c = 2 * np.arcsin(np.sqrt(a))
    return 6371 * c  # 地球半径6371km


def generate_point_pairs(df, MAX_DISTANCE_KM=50):
    """
    生成两点关系DataFrame，包含所有距离<=MAX_DISTANCE_KM的点对

    返回字段：
    Index(['id_a', 'id_b', 'distance',
           'id_x', 'point_x', 'point_standard_x', 'longitude_x', 'latitude_x',
           'id_y', 'point_y', 'point_standard_y', 'longitude_y', 'latitude_y'])
    """
    # 创建空列表存储结果
    engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{name}')
    results = []

    # 获取所有点ID列表
    ids = df['id'].values
    n = len(ids)

    # 双重循环计算点对距离
    for i in range(n):
        for j in range(i + 1, n):  # 避免重复计算和自比较
            row1 = df.iloc[i]
            row2 = df.iloc[j]

            distance = haversine(
                row1['longitude'], row1['latitude'],
                row2['longitude'], row2['latitude']
            )

            if distance <= MAX_DISTANCE_KM:
                # 构造结果记录
                record = {
                    'id_a': row1['id'],
                    'id_b': row2['id'],
                    'distance': distance,
                    # x系列字段来自第一个点
                    'id_x': row1['id'],
                    'point_x': row1['point'],
                    'point_standard_x': row1['point_standard'],
                    'longitude_x': row1['longitude'],
                    'latitude_x': row1['latitude'],
                    # y系列字段来自第二个点
                    'id_y': row2['id'],
                    'point_y': row2['point'],
                    'point_standard_y': row2['point_standard'],
                    'longitude_y': row2['longitude'],
                    'latitude_y': row2['latitude']
                }
                results.append(record)

    # 转换为DataFrame
    result_df = pd.DataFrame(results)

    # 确保字段顺序一致
    column_order = [
        'id_a', 'id_b', 'distance',
        'id_x', 'point_x', 'point_standard_x', 'longitude_x', 'latitude_x',
        'id_y', 'point_y', 'point_standard_y', 'longitude_y', 'latitude_y'
    ]
    s.tosql().drop_s(['near_latlon'])
    result_df.to_sql('near_latlon', con=engine, if_exists='append')

    return df


print(generate_point_pairs(data_select(2025,[9])))