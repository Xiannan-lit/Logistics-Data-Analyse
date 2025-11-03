import math
import geohash  # 使用python-geohash库，安装命令：pip install python-geohash
import pandas as pd
from sqlalchemy import create_engine
import FUNCS.FUNCS as FK
import FUNCS.DATA_PROCESS as DP
import os

user = os.getenv("DB_USER")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
password = os.getenv("DB_PASSWORD")
name = os.getenv("DB_NAME")



def haversine(lon1, lat1, lon2, lat2):
    """Haversine算法计算两点距离（公里）"""
    R = 6371.0  # 地球半径（公里）
    lon1_rad, lat1_rad = math.radians(lon1), math.radians(lat1)
    lon2_rad, lat2_rad = math.radians(lon2), math.radians(lat2)

    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def get_nearby_geohash(geohash_str):
    """获取当前GeoHash的相邻网格（8个方向）+自身，共9个候选网格"""
    # 有些版本的neighbors返回列表（8个相邻网格）
    nearby_list = geohash.neighbors(geohash_str)
    nearby = set(nearby_list)  # 直接将列表转为集合
    nearby.add(geohash_str)    # 加入自身网格
    return nearby


def find_pairs_within_distance(points, max_distance, geohash_precision=4):
    # 1. 构建GeoHash到点的映射
    geohash_map = {}
    for pid, lon, lat in points:
        # 修正：参数顺序为(lat, lon)
        gh = geohash.encode(lat, lon, precision=geohash_precision)
        if gh not in geohash_map:
            geohash_map[gh] = []
        geohash_map[gh].append((pid, lon, lat))

    # 2. 筛选候选点对并精确计算距离
    result = []
    processed = set()

    for pid1, lon1, lat1 in points:
        # 修正：参数顺序为(lat1, lon1)
        gh1 = geohash.encode(lat1, lon1, precision=geohash_precision)
        candidate_ghs = get_nearby_geohash(gh1)

        # 后续逻辑不变...
        for gh in candidate_ghs:
            if gh not in geohash_map:
                continue
            for pid2, lon2, lat2 in geohash_map[gh]:
                if pid1 >= pid2:
                    continue
                pair = (pid1, pid2)
                if pair in processed:
                    continue
                dist = haversine(lon1, lat1, lon2, lat2)
                if dist < max_distance and dist > 0:
                    result.append((pid1, pid2, round(dist, 2)))
                processed.add(pair)

    return result


# 示例用法
if __name__ == "__main__":
    # 示例数据：(id, 经度, 纬度)
    points = [
        (1, 116.4074, 39.9042),  # 北京
        (2, 121.4737, 31.2304),  # 上海
        (3, 116.3912, 39.9075),  # 北京附近（距离约1.5公里）
        (4, 116.4100, 39.9100),  # 北京附近（距离约1公里）
        (5, 120.1551, 30.2741),  # 杭州
    ]

    # 查找距离小于2公里的点对
    pairs = find_pairs_within_distance(points, max_distance=2)
    print("距离小于2公里的地址对：")
    for p in pairs:
        print(f"点{p[0]}与点{p[1]}：{p[2]}公里")

def distance_database():
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{name}')
    df_point = pd.read_sql('SELECT id,longitude, latitude FROM revenue_address WHERE longitude not like "/" and longitude not like ""',
                           con=engine)

    df_point['longitude'] = df_point['longitude'].astype(float)
    df_point['latitude'] = df_point['latitude'].astype(float)
    return df_point

def data_select(year,months):
    from sqlalchemy import create_engine
    engine = create_engine(F'mysql+pymysql://{user}:{password}@{host}:{port}/{name}')
    df_point = pd.read_sql('SELECT id, point, point_standard, longitude, latitude FROM revenue_address',con=engine)
    df_x_y = pd.read_sql(f'SELECT 发运单号, 发货地点, 到货地点, 客户, 公司, 业务类型, 业务日期 FROM revenue_old WHERE 业务日期 LIKE "%%{year}%%"',con=engine)
    print('df_xy:'+df_x_y)
    df_x_y['业务日期'] = pd.to_datetime(df_x_y['业务日期'],format='%Y%m%d')
    df_x_y = df_x_y[df_x_y['业务日期'].dt.month.isin(months)]
    df_point = df_point[(df_point['point'].isin(df_x_y['发货地点']))|(df_point['point'].isin(df_x_y['到货地点']))]
    df_point['latitude'] = df_point['latitude'].astype(float)
    df_point['longitude'] = df_point['longitude'].astype(float)
    df_point = df_point[['id','longitude','latitude']]
    return df_point

def test():
    df_point = data_select([2025],[1,2,3])
    print(df_point)
    points = list(df_point.itertuples(index=False, name=None))
    pairs = find_pairs_within_distance(points, max_distance=50)
    print("距离小于50公里的地址对：")
    print(pairs)
