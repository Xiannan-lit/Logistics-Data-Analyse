from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import create_engine

engine_ccd = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
engine_gd = create_engine('mysql+pymysql://root:xiannan@localhost:3306/gd')



def MYB_read_and_concatenate(folder_path):
    all_dfs = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.xls') or filename.endswith('.xlsx'):
            try:
                df = pd.read_excel(os.path.join(folder_path, filename), header=1)
                all_dfs.append(df)
                print(f"成功读取 {filename}。")
            except Exception as e:
                print(f"读取 {filename} 时出错：{e}")
    if all_dfs:
        return pd.concat(all_dfs)
    else:
        print("未找到有效的 Excel 文件。")
        return None

def commission(folder_path):
    all_dfs = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.xls') or filename.endswith('.xlsx'):
            try:
                df = pd.read_excel(os.path.join(folder_path, filename), header=1,dtype={'订单号': str,'运单号': str})
                df['来源文件'] = filename
                all_dfs.append(df)
                print(f"成功读取 {filename}。")
            except Exception as e:
                print(f"读取 {filename} 时出错：{e}")
    if all_dfs:
        return pd.concat(all_dfs)
    else:
        print("未找到有效的 Excel 文件。")
        return None




def MYB_read_and_concatenate_oil_0(folder_path):
    all_dfs = []

    # 获取文件列表并按修改时间排序
    files = [f for f in os.listdir(folder_path) if f.endswith('.xls') or f.endswith('.xlsx')]
    files = sorted(files, key=lambda x: os.path.getmtime(os.path.join(folder_path, x)))

    for filename in files:
        try:
            df = pd.read_excel(os.path.join(folder_path, filename), header=0, dtype={'平台单号': str})
            df['文件名'] = filename
            all_dfs.append(df)
            print(f"成功读取 {filename}。")
        except Exception as e:
            print(f"读取 {filename} 时出错：{e}")

    if all_dfs:
        all = pd.concat(all_dfs, ignore_index=True)
        # all = all.astype(str)
        return all
    else:
        print("未找到有效的 Excel 文件。")
        return None

def MYB_read_and_concatenate_oil(folder_path):
    all_dfs = []

    # 获取文件列表并按修改时间排序
    files = [f for f in os.listdir(folder_path) if f.endswith('.xls') or f.endswith('.xlsx')]
    files = sorted(files, key=lambda x: os.path.getmtime(os.path.join(folder_path, x)))

    for filename in files:
        try:
            df = pd.read_excel(os.path.join(folder_path, filename), header=1)
            df['文件名'] = filename
            all_dfs.append(df)
            print(f"成功读取 {filename}。")
        except Exception as e:
            print(f"读取 {filename} 时出错：{e}")

    if all_dfs:
        return pd.concat(all_dfs, ignore_index=True)
    else:
        print("未找到有效的 Excel 文件。")
        return None

def MYB_read_and_concatenate_2(folder_path):
    all_dfs = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.xls') or filename.endswith('.xlsx'):
            try:
                # 读取 Excel 文件的所有工作表
                xls = pd.ExcelFile(os.path.join(folder_path, filename))
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name, header=2)
                    all_dfs.append(df)
                    print(f"成功读取 {filename} 的 {sheet_name} 工作表。")
            except Exception as e:
                print(f"读取 {filename} 时出错：{e}")
    if all_dfs:
        return pd.concat(all_dfs)
    else:
        print("未找到有效的 Excel 文件。")
        return None


def MYB_read_and_concatenate_header(folder_path,header):
    all_dfs = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.xls') or filename.endswith('.xlsx'):
            try:
                # 读取 Excel 文件的所有工作表
                xls = pd.ExcelFile(os.path.join(folder_path, filename))
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name, header=header,dtype=str)
                    all_dfs.append(df)
                    print(f"成功读取 {filename} 的 {sheet_name} 工作表。")
            except Exception as e:
                print(f"读取 {filename} 时出错：{e}")
    if all_dfs:
        return pd.concat(all_dfs)
    else:
        print("未找到有效的 Excel 文件。")
        return None
def data_astype(data,indexes,type):
    for i in indexes:
        data[i] = data[i].astype(type)

def car_revnue():
    engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
    df_car = pd.read_sql('SELECT 发运单号, 省, 市, 县区, 结算点, 距离, 运输费用, 油卡费用, 指导价, 承担运费类型, 承担运费, 线路名称, 调度员, 卸货点数, 简要地址 FROM car',con=engine)
    data_astype(df_car,['运输费用','油卡费用','承担运费'],float)
    df_car['发运单号'] = df_car['发运单号'].str.replace('-1','')
    df_car = df_car.groupby(['发运单号']).agg({
        '结算点':'first',
        '省':'first',
        '市':'first',
        '县区':'first',
        '距离':'first',
        '运输费用':'sum',
        '油卡费用':'sum',
        '指导价':'sum',
        '承担运费类型':'first',
        '承担运费':'sum',
        '线路名称':'first',
        '调度员':'first',
        '卸货点数':'first'
    })
    df_revenue = pd.read_sql('SELECT * FROM revenue WHERE 发运单号 NOT LIKE "%%CCD%%"',con=engine)
    data_astype(df_revenue,['采购价','结算价'],float)
    df_revenue['发运单号'] = df_revenue['发运单号'].str.replace('-1','')
    df_revenue['业务日期'] = pd.to_datetime(df_revenue['业务日期'],format='%Y%m%d')
    df_revenue['年'] = df_revenue['业务日期'].dt.year
    df_revenue['月'] = df_revenue['业务日期'].dt.month
    df_revenue['季'] = df_revenue['业务日期'].dt.quarter
    df_merge = pd.merge(df_revenue,df_car,on='发运单号',how='left')
    df_merge['司机运费'] = df_merge['运输费用'] + df_merge['油卡费用']
    df_merge.loc[df_merge['承担运费类型'] == '承担运费现结', '司机运费'] = df_merge.loc[df_merge['承担运费类型'] == '承担运费现结','司机运费'] + df_merge.loc[df_merge['承担运费类型'] == '承担运费现结', '承担运费']
    df_merge.loc[df_merge['承担运费类型'] == '承担运费现结', '采购价'] = df_merge.loc[df_merge[
                                                                                            '承担运费类型'] == '承担运费现结', '采购价'] + \
                                                                           df_merge.loc[df_merge[
                                                                                            '承担运费类型'] == '承担运费现结', '承担运费']/1.09
    return df_merge

def car_revnue_tax():
    engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
    df_car = pd.read_sql('SELECT 发运单号, 省, 市, 县区, 结算点, 距离, 运输费用, 实际运费, 油卡费用, 成本总金额, 指导价, 承担运费类型, 承担运费, 线路名称, 调度员, 卸货点数, 简要地址 FROM car',con=engine)
    data_astype(df_car,['运输费用','油卡费用','承担运费','成本总金额'],float)
    df_car['发运单号'] = df_car['发运单号'].str.replace('-1','')
    df_car = df_car.groupby(['发运单号']).agg({
        '结算点':'first',
        '省':'first',
        '市':'first',
        '县区':'first',
        '距离':'first',
        '运输费用':'sum',
        '油卡费用':'sum',
        '指导价':'sum',
        '承担运费类型':'first',
        '承担运费':'sum',
        '线路名称':'first',
        '调度员':'first',
        '卸货点数':'first',
        '成本总金额':'sum',
        '实际运费':'sum'
    })
    df_revenue = pd.read_sql('SELECT * FROM revenue WHERE 发运单号 NOT LIKE "%%CCD%%"',con=engine)
    df_revenue['业务日期'] = pd.to_datetime(df_revenue['业务日期'],format='%Y%m%d')
    df_revenue['年'] = df_revenue['业务日期'].dt.year
    df_revenue['月'] = df_revenue['业务日期'].dt.month
    df_revenue['季'] = df_revenue['业务日期'].dt.quarter
    df_merge = pd.merge(df_revenue,df_car,on='发运单号',how='left')
    df_merge.loc[df_merge['承担运费类型'] == '承担运费现结', '采购价'] = df_merge.loc[df_merge[
                                                                                            '承担运费类型'] == '承担运费现结', '成本总金额'] + \
                                                                           df_merge.loc[df_merge[
                                                                                            '承担运费类型'] == '承担运费现结', '承担运费']
    df_merge.loc[df_merge['承担运费类型'] != '承担运费现结', '采购价'] = df_merge.loc[df_merge['承担运费类型'] != '承担运费现结', '成本总金额']
    return df_merge

def match_2in_1out(row, dict,data1,data2):
    for a1, a2 in dict.items():
        for b1, b2 in a2.items():
            if row[data1] == b1 and row[data2] == b2:
                return a1
    return None

def extract_tonnage_from_remark(remark):
    match = re.search(r'(\d+(\.\d+)?)\s*T', remark)
    if match:
        return float(match.group(1))
    else:
        return None


def standardize_truck_size(size_str):
    size_str = size_str.replace("米", "").strip()
    try:
        size = float(size_str)
    except ValueError:
        return "未知"

    if size <= 5.5:
        return "4.2米"
    elif size <= 8.2:
        return "6.8米"
    elif size <= 11.1:
        return "9.6米"
    elif size <= 15:
        return "12.5米"
    elif size <= 20:
        return "17.5米"
    else:
        return "未知"
def get_quarter(business_date):
    quarter = (business_date.month - 1) // 3 + 1
    return quarter


import re

def extract_fund(text):
    if isinstance(text, float):
        return 0
    # 使用正则表达式搜索金额
    amounts = re.findall(r'(\d+元)', str(text))

    # 如果找到了金额
    if amounts:
        # 提取第一个金额
        amount = amounts[0]
        # 判断 '元' 前面是否有数字
        if text.split(amount)[0].isdigit():
            return amount
        else:
            # 提取 '到付' 前面的数字
            payment = re.search(r'(\d+)到付', text)
            if payment:
                return payment.group(1) + '元'
            else:
                return None
    else:
        return '0'

def multi(address) :
    amount = 1
    count_one = address.count('+')
    count_two = address.count('、')
    count_point_one = address.count('两地')
    count_point_two = address.count('三地')
    count_point_three = address.count('四地')
    amount = amount + count_one + count_two + count_point_one*2 + count_point_two*3 + count_point_three*4
    return amount


def find_closest_date(orders_df, price_adjustments_df):
    closest_dates = []
    for order_date in orders_df['业务日期']:
        # 筛选出小于等于订单日期的最近日期
        closest_date_df = price_adjustments_df[price_adjustments_df['调整日期'] <= order_date]
        if closest_date_df.empty:
            closest_dates.append({'调整日期': 0, '索引': None})
        else:
            closest_date = closest_date_df.iloc[-1]
            closest_date_index = closest_date_df.index[-1]
            closest_dates.append({'调整日期': closest_date, '索引': closest_date_index})
    return pd.DataFrame(closest_dates)


def guanxing(guan,zong):
    if guan == zong:
        return '纯管道'
    elif guan == 0:
        return '纯型材'
    else:
        return '管型'

def plate_number(driver_info):
    parts = driver_info.split('/')
    if len(parts) > 2:
        return parts[2]
    else:
        return None

def plate_clean(plate):
    plate = plate.replace(' ','')
    plate = plate.replace('?','')
    return plate

def extract_chinese_characters(s):
    chinese_characters = re.findall(r'[\u4e00-\u9fff]+', s)
    return ''.join(chinese_characters)

def driver_region(plate):
    license_plate_mapping = {
        '京': '北京市',
        '津': '天津市',
        '沪': '上海市',
        '渝': '重庆市',
        '冀': '河北省',
        '晋': '山西省',
        '辽': '辽宁省',
        '吉': '吉林省',
        '黑': '黑龙江省',
        '苏': '江苏省',
        '浙': '浙江省',
        '皖': '安徽省',
        '闽': '福建省',
        '赣': '江西省',
        '鲁': '山东省',
        '豫': '河南省',
        '鄂': '湖北省',
        '湘': '湖南省',
        '粤': '广东省',
        '琼': '海南省',
        '川': '四川省',
        '贵': '贵州省',
        '云': '云南省',
        '陕': '陕西省',
        '甘': '甘肃省',
        '青': '青海省',
        '藏': '西藏自治区',
        '桂': '广西壮族自治区',
        '宁': '宁夏回族自治区',
        '新': '新疆维吾尔自治区',
        '台': '台湾省',
        '港': '香港特别行政区',
        '澳': '澳门特别行政区'
    }
    region = plate.str.slice(stop=1).map(license_plate_mapping)
    return region


import os
import pandas as pd

def inventory(address):
    """
    Reads all CSV and Excel files (including all sheets) from the specified directory
    and combines them into a single DataFrame.

    Parameters:
        address (str): The directory containing the CSV and Excel files.

    Returns:
        DataFrame: A combined DataFrame of all the files, or None if no files were found.
    """
    # Initialize an empty list to store the dataframes
    dataframes = []

    # Check if the directory exists
    if os.path.exists(address):
        # List all files in the directory
        file_list = os.listdir(address)

        if file_list:
            print("Files found in the directory:")
            for filename in file_list:
                print(filename)
                # Check if the file is a CSV or Excel file
                file_path = os.path.join(address, filename)
                if filename.endswith('.csv'):
                    df = pd.read_csv(file_path)
                    dataframes.append(df)
                elif filename.endswith('.xls') or filename.endswith('.xlsx'):
                    excel_data = pd.ExcelFile(file_path)
                    for sheet_name in excel_data.sheet_names:
                        df = pd.read_excel(file_path, sheet_name=sheet_name)
                        dataframes.append(df)

            # Combine all dataframes into one if there are any
            if dataframes:
                combined_df = pd.concat(dataframes, ignore_index=True)
                # Display the first few rows of the combined dataframe
                print("Combined DataFrame:")
                print(combined_df.head())
                return combined_df
            else:
                print("No CSV or Excel files found in the directory.")
                return None
        else:
            print("The directory is empty.")
            return None
    else:
        print("The directory does not exist.")
        return None


def trivil_read_and_concatenate_header(folder_path,header):
    all_dfs = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.xls') or filename.endswith('.xlsx'):
            try:
                # 读取 Excel 文件的所有工作表
                xls = pd.ExcelFile(os.path.join(folder_path, filename))
                for sheet_name in xls.sheet_names:
                    df = pd.read_excel(xls, sheet_name, header=header)
                    df['承运商'] = sheet_name
                    all_dfs.append(df)
                    print(f"成功读取 {filename} 的 {sheet_name} 工作表。")
            except Exception as e:
                print(f"读取 {filename} 时出错：{e}")
    if all_dfs:
        return pd.concat(all_dfs)
    else:
        print("未找到有效的 Excel 文件。")
        return None

def cost_year(df_merge,year,car):
    df_merge = df_merge[df_merge['年']==year]
    df_merge = df_merge[df_merge['车型']==car]
    df_agg = df_merge.groupby(['结算点','公司']).agg(采购均价=('采购价','mean'),总车次=('发运单号','count'),总成本=('采购价','sum')).reset_index()
    df_agg['采购均价'] = df_agg['采购均价'].round(0)
    df_agg.rename(columns={'采购均价':f'{year}_{car}采购价','总车次':f'{year}_{car}车次','总成本':f'{year}_{car}总成本'},inplace=True)
    return df_agg

def start(df):
    base_address = {'广东营业部': ('Y', '广东省,茂名市,高州市'),
                    '福建营业部': ('M', '福建省,漳州市,漳浦县'),
                    '吉林营业部': ('L', '吉林省,长春市,九台区'),
                    '新疆营业部': ('J', '新疆维吾尔自治区,乌鲁木齐市,沙依巴克区'),
                    '四川营业部': ('D', '四川省,成都市,龙泉驿区'),
                    '天津营业部': ('T', '天津市,天津市,滨海新区'),
                    '湖南营业部': ('H', '湖南省,长沙市,宁乡市'),
                    '六安营业部': ('W', '安徽省,六安市,金安区'),
                    '下沙营业部': ('S', '浙江省,杭州市,钱塘区'),
                    '西安营业部': ('A', '陕西省,西安市,雁塔区'),
                    '新昌营业部': ('C', '浙江省,绍兴市,新昌县'),
                    '衢州营业部': ('Q', '浙江省,衢州市,衢江区')
                    }
    # df['首字母'] = df['发运单号'].str[0]
    for i in base_address.keys():
        df.loc[(df['公司'] == i)  & (df['业务类型'] == '产品发货整车业务') & (
            df['发货地点'].isnull()), '发货地点'] = base_address[i][1]
        # df.loc[(df['公司'] == i) & (df['首字母']==base_address[i][0])&(df['业务类型']=='产品发货整车业务')&(df['发货地点'].isnull()), '发货地点'] = base_address[i][1]
        df.loc[df['发货地点'].str.contains('下沙基地'),'发货地点'] ='浙江省杭州市钱塘区下沙'
        df.loc[df['到货地点'].str.contains('下沙基地'), '到货地点'] = '浙江省杭州市钱塘区下沙'
    return df


#👆👆👆👆👆👆👆👆👆👆👆👆基础

#👇👇👇👇👇👇👇👇👇👇👇👇衍生

def start(df):
    base_address = {'广东营业部': ('Y', '广东省,茂名市,高州市'),
                    '福建营业部': ('M', '福建省,漳州市,漳浦县'),
                    '吉林营业部': ('L', '吉林省,长春市,九台区'),
                    '新疆营业部': ('J', '新疆维吾尔自治区,乌鲁木齐市,沙依巴克区'),
                    '四川营业部': ('D', '四川省,成都市,龙泉驿区'),
                    '天津营业部': ('T', '天津市,天津市,滨海新区'),
                    '湖南营业部': ('H', '湖南省,长沙市,宁乡市'),
                    '六安营业部': ('W', '安徽省,六安市,金安区'),
                    '下沙营业部': ('S', '浙江省,杭州市,钱塘区'),
                    '西安营业部': ('A', '陕西省,西安市,雁塔区'),
                    '新昌营业部': ('C', '浙江省,绍兴市,新昌县'),
                    '衢州营业部': ('Q', '浙江省,衢州市,衢江区'),
                    '河南分部': ('H', '河南省,焦作市,温县')
                    }
    df['首字母'] = df['发运单号'].str[0]
    df['发货地点'] = df['发货地点'].replace(' ','/')
    for i in base_address.keys():
        df.loc[(df['公司'] == i) &(df['业务类型']=='产品发货整车业务')&(df['发货地点']=='/')&(~df['发运单号'].str.contains('CCD')), '发货地点'] = base_address[i][1]
    df = df[df['业务类型'].isin([
                                '产品发货整车业务',
                                '公路整车-其他货物整车业务',
                                '国际货代（美元）-出口',
                                '公路整车-产品发货整车业务',
                                '国际货代（人民币）-出口',
                                '国际货代（人民币）-进口',
                                ])]
    df_out = company_out(r'E:\MK\数据源\毛利报表-内部')
    df_out['客户类型'] = '内部客户'
    df = pd.merge(df,df_out,on='客户',how='left')
    df['客户类型'] = df['客户类型'].fillna('外部客户')
    return df

def company_out(path_out):
    df = MYB_read_and_concatenate(path_out)
    df = df[['客户']].drop_duplicates()
    return df


def standardize_truck_size(size_str):
    size_str = size_str.replace("米", "").strip()
    try:
        size = float(size_str)
    except ValueError:
        return "未知"

    if size <= 5.5:
        return "4.2米"
    elif size <= 8.2:
        return "6.8米"
    elif size <= 11.1:
        return "9.6米"
    elif size <= 15:
        return "12.5米"
    elif size <= 20:
        return "17.5米"
    else:
        return "未知"

def standardize_truck_size_2(size_str):
    size_str0 = size_str
    size_str = size_str.replace("米", "").strip()
    try:
        size = float(size_str)
    except ValueError:
        return "未知"
    if size <= 8.2:
        return size_str0
    elif size <= 11.1:
        return "9.6米"
    elif size <= 15:
        return "12.5米"
    elif size <= 20:
        return "17.5米"
    else:
        return "未知"

def point_match(province,point,point_raw):
    if province=='/':
        return point_raw
    else:
        return point

def data_select():
    df_revenue = pd.read_sql('SELECT * FROM revenue_old WHERE 采购价 > 50 AND 结算价 > 10 AND 业务类型 NOT LIKE "%%零担%%"',con=engine_ccd)
    df_revenue = start(df_revenue)
    df_revenue = df_revenue[df_revenue['车型'].notnull()]
    df_address = pd.read_sql('SELECT * FROM revenue_address',con=engine_ccd)
    df_address = df_address.drop_duplicates('point')
    df_car = pd.read_sql('SELECT 发运单号, 承运商, 卸货点数, 指导价, 结算点, 省, 市, 县区, 外部单号 FROM car',con=engine_ccd)
    df_car['省市县'] = df_car['省'] + df_car['市'] + df_car['县区']
    df_revenue = pd.merge(df_revenue, df_car, on='发运单号', how='left')
    df_revenue = df_revenue.fillna('/')
    df_revenue = df_revenue.replace(' ','/')
    df_revenue['point_match'] = df_revenue.apply(lambda x:point_match(x['省'],x['省市县'],x['到货地点']),axis=1)
    df_revenue = pd.merge(df_revenue,df_address,left_on='发货地点',right_on='point',how='left')
    df_revenue = pd.merge(df_revenue,df_address,left_on='point_match',right_on='point',how='left')
    df_revenue['routine_all'] = df_revenue['longitude_x'] + ',' + df_revenue['latitude_x'] + ';' + df_revenue['latitude_y'] + ',' + \
                             df_revenue['longitude_y']
    df_cheapest = pd.read_sql('SELECT * FROM routine_distance_cheapest', con=engine_ccd)
    df_cheapest['routine_all'] = df_cheapest['longitude_x'] + ',' + df_cheapest['latitude_x'] + ';' + df_cheapest[
        'latitude_y'] + ',' + df_cheapest['longitude_y']
    df_cheapest = df_cheapest.drop_duplicates(['routine_all'])
    df_cheapest = df_cheapest[['routine_all','distance','duration']]
    df_cheapest['distance'] = df_cheapest['distance'].astype(float).fillna(-1)
    df_cheapest['distance'] = df_cheapest['distance']/1000
    df_revenue = pd.merge(df_revenue,df_cheapest,on='routine_all',how='left')
    df_revenue['线路'] = df_revenue['point_standard_x'] + '-' + df_revenue['point_standard_y']
    df_revenue['业务日期'] = pd.to_datetime(df_revenue['业务日期'],format='%Y%m%d')
    df_revenue['年'] = df_revenue['业务日期'].dt.year
    df_revenue['月'] = df_revenue['业务日期'].dt.month
    df_revenue['标准化车型'] = df_revenue.apply(lambda x:standardize_truck_size(x['车型']),axis=1)
    # df_agg = df_revenue.groupby(['公司','线路','年','标准化车型']).agg(采购均价=('采购价','mean'),车次=('发运单号','count')).reset_index()
    # print(df_agg)
    return df_revenue

def data_select_raw_start():
    df_revenue = pd.read_sql('SELECT 发货地点, 到货地点, 发运单号, 公司, 业务类型, 客户, 车型, 业务日期, 备注 FROM revenue_old WHERE 采购价 > 50 AND 结算价 > 10 AND 业务类型 NOT LIKE "%%零担%%" AND 业务类型 NOT LIKE "%%到付%%" AND 业务类型 NOT LIKE "%%扫码付%%" AND 业务类型 NOT LIKE "%%调整%%"',con=engine_ccd)
    df_revenue = start(df_revenue)
    df_revenue = df_revenue[df_revenue['车型'].notnull()]
    df_address = pd.read_sql('SELECT * FROM revenue_address',con=engine_ccd)
    df_address = df_address.drop_duplicates('point')
    df_car = pd.read_sql('SELECT 发运单号, 成本总金额, 承担运费, 承担运费类型, 卸货点数, 指导价, 结算点, 省, 市, 县区, 外部单号 FROM car',con=engine_ccd)
    df_car['省市县'] = df_car['省'] + df_car['市'] + df_car['县区']
    df_car['采购价'] = df_car['成本总金额']
    df_car.loc[df_car['承担运费类型']=='承担运费现结','采购价'] = df_car.loc[df_car['承担运费类型']=='承担运费现结','成本总金额'] + df_car.loc[df_car['承担运费类型']=='承担运费现结','承担运费']
    df_revenue = pd.merge(df_revenue, df_car, on='发运单号', how='left')
    df_revenue = df_revenue.fillna('/')
    df_revenue = df_revenue.replace(' ','/')
    # df_revenue['point_match'] = df_revenue.apply(lambda x:point_match(x['省'],x['省市县'],x['到货地点']),axis=1)
    df_revenue = pd.merge(df_revenue,df_address,left_on='发货地点',right_on='point',how='left')
    df_revenue = pd.merge(df_revenue,df_address,left_on='到货地点',right_on='point',how='left')
    df_revenue['线路'] = df_revenue['point_standard_x'] + '-' + df_revenue['point_standard_y']
    df_revenue['业务日期'] = pd.to_datetime(df_revenue['业务日期'],format='%Y%m%d')
    df_revenue['年'] = df_revenue['业务日期'].dt.year
    df_revenue['月'] = df_revenue['业务日期'].dt.month
    df_revenue['标准化车型'] = df_revenue.apply(lambda x:standardize_truck_size(x['车型']),axis=1)
    # df_agg = df_revenue.groupby(['公司','线路','年','标准化车型']).agg(采购均价=('采购价','mean'),车次=('发运单号','count')).reset_index()
    # print(df_agg)
    return df_revenue

def full_to_half(text):
    """
    将全角字符转换为半角字符
    :param text: 包含全角字符的字符串
    :return: 转换后的半角字符串
    """
    if not isinstance(text, str):
        return text  # 非字符串类型直接返回

    result = []

    for char in text:
        # 获取字符的Unicode编码
        code = ord(char)
        # 全角空格转换为半角空格
        if not char:
            return text
        if code == 0x3000:
            result.append(chr(0x20))
        # 全角字符（除空格外）转换为半角
        elif 0xFF01 <= code <= 0xFF5E:
            result.append(chr(code - 0xFEE0))
        # 其他字符不转换
        else:
            result.append(char)
    print('已替换成半角')
    return ''.join(result)