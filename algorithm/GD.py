import pandas as pd
import requests

class gaode():
    def geo_data(self, address):
        gaode_key = 'XXX'
        url = f'https://restapi.amap.com/v3/geocode/geo?address={address}&key={gaode_key}'
        response = requests.get(url)
        data = response.json()
        if data['status'] == '1':
            # print('传回源数据:'+data)
            print(address)
            return (data)
        else:
            print(f'未获取到相关路径_geodata{address}')
            return '未获取到相关路径'

    def geo_data_standard(self, coordinate):
        gaode_key = 'XXX'
        url = f'https://restapi.amap.com/v4/geocode/geo?location={coordinate}&key={gaode_key}'
        response = requests.get(url)
        data = response.json()
        if data:
            print(data)
            return (data)
        else:
            print('未获取到相关路径')
            return '未获取到相关路径'

    def coordination_point(self,GDdata_geo):
        import pandas as pd
        if GDdata_geo and 'geocodes' in GDdata_geo and GDdata_geo['geocodes']:
            data_la_lo = GDdata_geo['geocodes'][0]['location']
            geocode = GDdata_geo['geocodes'][0]
            province = geocode.get('province', '')
            city = geocode.get('city', '')
            district = geocode.get('district', '')
            result = f"{province},{city},{district}"
            print(pd.Series([result, data_la_lo], index=['point_standard', 'coordination']))
            return pd.Series([result, data_la_lo], index=['point_standard', 'coordination'])
        else:
        # 返回 pd.Series, 默认值也按列返回
            print(pd.Series(['未获取到相关路径', '未获取到坐标'], index=['point_standard', 'coordination']))
            return pd.Series(['未获取到相关路径', '未获取到坐标'], index=['point_standard', 'coordination'])

    def routine_truck(self,origin_coordinate,end_coordinate):
        gaode_key = 'XXX'
        url = f'https://restapi.amap.com/v4/direction/truck?origin={origin_coordinate}&destination={end_coordinate}&strategy=1&key={gaode_key}'
        response = requests.get(url)
        data = response.json()
        if data:
            print(data)
            return (data)
        else:
            print('未获取到相关路径')
            return '未获取到相关路径'

    def routine_car(self,origin_coordinate,end_coordinate):
        gaode_key = 'XXX'
        url = f'https://restapi.amap.com/v3/direction/driving?origin={origin_coordinate}&destination={end_coordinate}&strategy=1&key={gaode_key}'
        response = requests.get(url)
        data = response.json()
        if data['status'] == '1':
            print(str(data)[:100])
            return (data)
        else:
            print('未获取到相关路径')
            return '未获取到相关路径'

    # def distance(self,route):


    def toll(self, GDdata_routine):
        if GDdata_routine and 'route' in GDdata_routine and GDdata_routine['route'] and 'paths' in GDdata_routine[
            'route']:
            # Extract paths, since it's a list, we access the first path
            paths = GDdata_routine['route']['paths']
            if paths:
                first_path = paths[0]  # Access the first path
                # Access toll and toll_distance
                data_tolls = first_path.get('tolls', 'No tolls information')
                data_toll_distance = first_path.get('toll_distance', 'No toll distance information')
                # Format and return the result
                data_t = f"Tolls: {data_tolls}, Toll Distance: {data_toll_distance}"
                print(data_t)
                return data_t
            else:
                print('No paths found in the data.')
                return '未获取到相关路径'
        else:
            print('未获取到相关路径')
            return '未获取到相关路径'



    def standard(self, GDdata_geo):
        if GDdata_geo and 'geocodes' in GDdata_geo and GDdata_geo['geocodes']:
            # 获取 geocodes 中的第一个地址信息
            geocode = GDdata_geo['geocodes'][0]
            # 提取省、市、区信息
            province = geocode.get('province', '')
            city = geocode.get('city', '')
            district = geocode.get('district', '')

            # 用逗号隔开
            result = f"{province},{city},{district}"
            print(result)
            return result
        else:
            return '未获取到相关路径'

    def distance_duration(self,data):
        if data and data['status']=='1':
            route = data['route']['paths'][0]
            distance = route.get('distance','')
            duration = route.get('duration','')
            result = f"{distance},{duration}"
            print(result)
            distance, duration = result.split(',')
            return pd.Series([distance,duration])
        else:
            return ['无路线','无路线']