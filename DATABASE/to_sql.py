import os
import pandas as pd
from sqlalchemy import create_engine, text,TEXT,VARCHAR,DECIMAL,INT, MetaData, Table, Column, String,Integer
from sqlalchemy.types import DECIMAL


class create_table():
    def business(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        metadata = MetaData()
        business = Table('business', metadata,
                         Column('公司', String(100)),
                         Column('运单号', String(50)),
                         Column('日期', String(50)),
                         Column('客户', String(100)),
                         Column('客户名称', String(100)),
                         Column('承运商', String(100)),
                         Column('发货地点', String(500)),
                         Column('到货地点', String(500)),
                         Column('车型', String(50)),
                         Column('类型', String(50)),
                         Column('类型新', String(50)),  # 新增字段
                         Column('车牌号', String(50)),
                         Column('司机标记', String(50)),
                         Column('司机姓名', String(50)),
                         Column('联系方式', String(50)),
                         Column('运费收入', String(50)),
                         Column('特殊运费收入', String(50)),
                         Column('装卸费收入', String(50)),
                         Column('其他收入6', String(50)),
                         Column('其他收入0', String(50)),
                         Column('收入总金额', String(50)),
                         Column('支付类型', String(50)),
                         Column('运输费用', String(50)),
                         Column('油卡费用', String(50)),
                         Column('运输服务费', String(50)),
                         Column('装卸费成本', String(50)),
                         Column('装卸费支付类型', String(50)),
                         Column('装卸费服务费', String(50)),
                         Column('总服务费', String(50)),
                         Column('运营费用', String(50)),
                         Column('成本总金额', String(50)),
                         Column('备注', String(500)),
                         Column('收货联系人', String(50)),
                         Column('收货联系人电话', String(50)),
                         Column('发货联系人', String(50)),
                         Column('发货联系人电话', String(50)),
                         Column('外部单号', String(200)),
                         Column('支付方式', String(50)),
                         Column('收入对账', String(50)),
                         Column('收入对账人', String(50)),
                         Column('收入对账备注', String(255)),
                         Column('成本对账', String(50)),
                         Column('成本对账人', String(50)),
                         Column('成本对账备注', String(255)),
                         Column('业务开发员', String(50)),
                         Column('起点终点距离', String(50)),
                         Column('吨位', String(50)),
                         Column('回单是否回收', String(50)),
                         Column('回单快递单号', String(50)),
                         Column('港区代理', String(50)),
                         Column('报关', String(50)),
                         Column('文件', String(50)),
                         Column('操作费', String(50)),
                         Column('代收代垫', String(50)),
                         Column('其他异常费0', String(50)),
                         Column('其他费6', String(50)),
                         Column('提单号', String(100)),
                         Column('火车皮号', String(50)),
                         Column('调度员', String(50)),  # 新增字段
                         Column('创建人', String(50)),  # 新增字段
                         Column('项目名称', String(100)),  # 新增字段
                         Column('关联运单号', String(100)),  # 新增字段
                         Column('货物类型', String(100)),  # 新增字段
                         Column('货物信息', String(500))  # 新增字段
                         )

        # 使用engine创建表
        metadata.create_all(engine)
    def revenue_old(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        metadata = MetaData()
        revenue_old = Table('revenue_old', metadata,
                            Column('序号', Integer),  # Corresponds to SQL INT
                            Column('发运单号', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('业务日期', String(255)),
                            Column('实际车型', String(255)), # Corresponds to SQL VARCHAR(255)
                            Column('车型', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('车牌号', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('客户', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('发货地点', String(500)),  # Corresponds to SQL VARCHAR(500)
                            Column('到货地点', String(500)),  # Corresponds to SQL VARCHAR(500)
                            Column('简要地址', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('承运商', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('公司', String(200)),  # Corresponds to SQL VARCHAR(200)
                            Column('结算价', DECIMAL(20, 2)),  # Corresponds to SQL DECIMAL(20, 2)
                            Column('采购价', DECIMAL(20, 2)),  # Corresponds to SQL DECIMAL(20, 2)
                            Column('毛利额', DECIMAL(20, 2)),  # Corresponds to SQL DECIMAL(20, 2)
                            Column('毛利率', String(20)),  # Corresponds to SQL VARCHAR(20)
                            Column('运营费用', DECIMAL(20, 2)),  # Corresponds to SQL DECIMAL(20, 2)
                            Column('备注', String(500)),  # Corresponds to SQL VARCHAR(500)
                            Column('创建日期', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('业务类型', String(255)),  # Corresponds to SQL VARCHAR(255)
                            Column('是否找车', String(255)),  # New field for "是否找车" (Car Sourcing)
                            Column('支付类型', String(255)),  # New field for "支付类型" (Payment Type)
                            Column('原结算价', DECIMAL(20, 2)),  # Corresponds to SQL DECIMAL(20, 2)
                            Column('原毛利额', DECIMAL(20, 2)),  # Corresponds to SQL DECIMAL(20, 2)
                            Column('原毛利率', String(20))  # Corresponds to SQL VARCHAR(20)
                            )
        metadata.create_all(engine)
class tosql():
    from sqlalchemy import create_engine, String, TEXT
    import pandas as pd
    import os
    engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
    def address_change(self,car,business,revenue):
        car_address = fr"{car}"
        business = fr"{business}"
        revenue = fr"{revenue}"
        return [car_address,business,revenue]

    def car_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\\MK\\数据源\\车辆确认'  # 使用双反斜线或原始字符串
        for file_name in os.listdir(folder_path):
            if file_name.endswith('.xls') or file_name.endswith('.xlsx'):
                file_path = os.path.join(folder_path, file_name)
                df = pd.read_excel(file_path)
                df.to_sql('car',con=engine, if_exists='append', index=False, dtype={
                    '公司': String(100),
                    '发运单号': String(50),
                    '状态': String(50),
                    '日期': String(50),
                    '类型': String(50),
                    '到货地址': String(500),
                    '承运商': String(100),
                    '实际车型': String(50),
                    '车型': String(50),
                    '支付开票路径': String(50),
                    '运输费用': DECIMAL(10,2),
                    '油卡费用': DECIMAL(10,2),
                    '服务费': DECIMAL(10,2),
                    '成本总金额': DECIMAL(10,2),
                    '权责运费收入': DECIMAL(10,2),
                    '卸货点数': INT,
                    '卸货补贴': DECIMAL(10,2),
                    '其他收入': DECIMAL(10,2),
                    '其他收入说明': String(255),
                    '收入总额': DECIMAL(10,2),
                    '调度员': String(100),
                    '距离': DECIMAL(10,2),
                    '外部单号': String(300),
                    '车牌号': String(50),
                    '司机标记': String(50),
                    '司机姓名': String(50),
                    '联系方式': String(50),
                    '实际运费': DECIMAL(10,2),
                    '特殊运费': DECIMAL(10,2),
                    '运费收入装卸费': DECIMAL(10,2),
                    '备注': TEXT,
                    '提交时间': String(50),
                    '超价审核状态名称': String(50),
                    '超价金额': DECIMAL(10,2),
                    '超价比例': String(50),
                    '客户': String(50),
                    '简要地址': String(50),
                    '结算点': String(50),
                    '省': String(50),
                    '市': String(50),
                    '县区': String(50),
                    '发运备注': TEXT,
                    '特殊运费说明': TEXT,
                    '总重量': DECIMAL(10,2),
                    '管道重量': DECIMAL(10,2),
                    '型材金额': DECIMAL(10,2),
                    '线路名称': String(50),
                    '结算标准价': DECIMAL(10,2),
                    '指导价': DECIMAL(10,2),
                    '不变价金额': DECIMAL(10,2),
                    '调度员名称': String(50),
                    '承担运费类型': String(30),
                    '承担运费': DECIMAL(10,2),
                    '收入完结': String(255),
                    '收入完结人': String(255),
                    '成本完结': String(255),
                    '成本完结人': String(255),
                })
                print(f'{file_name} has been imported into table car.')
    def drop(self,table):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        with engine.connect() as connection:
            sql_command = text(f"DROP TABLE IF EXISTS `{table}`;")
            connection.execute(sql_command)
            print(f"表 `{table}` 已被删除。")

    def drop_all(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        tables = pd.read_sql("SHOW TABLES;", engine)
        table_names = tables.iloc[:, 0]  # 假定表名在第一列
        for table_name in table_names:
            self.drop(table_name)
        print("所有表已从数据库中删除。")

    def drop(self,table_name):
        engine = self.engine
        with engine.connect() as connection:
            sql_command = text(f"DROP TABLE IF EXISTS `{table_name}`;")
            connection.execute(sql_command)
            print(f"表 `{table_name}` 已被删除。")

    def revenue_tosql(self):
        engine = self.engine
        folder_path = r'E:\\MK\\数据源\\毛利报表'
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path, header=1)
                df['营销机构'] = df['营销机构'].str[0:160]
                df.fillna('/')
                df.to_sql(
                    'revenue',
                    con=engine,
                    if_exists='append',
                    index=False,
                    dtype={
                        '序号': INT(),  # 对应 SQL 的 INT
                        '发运单号': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '业务日期': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '车型': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '车牌号': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '客户': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '发货地点': String(500),  # 对应 SQL 的 VARCHAR(500)
                        '到货地点': String(500),  # 对应 SQL 的 VARCHAR(500)
                        '简要地址': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '承运商': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '公司': String(200),  # 对应 SQL 的 VARCHAR(200)
                        '结算价': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '采购价': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '毛利额': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '毛利率': String(20),  # 对应 SQL 的 VARCHAR(20)
                        '运营费用': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '备注': String(500),  # 对应 SQL 的 VARCHAR(500)
                        '创建日期': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '业务类型': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '营销机构': String(500),  # 对应 SQL 的 VARCHAR(255)
                        '起运地': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '到达地': String(255),  # 对应 SQL 的 VARCHAR(255)
                        '原合同结算价': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '指导价成本': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '成本偏差': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '原结算价': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '原毛利额': DECIMAL(20, 2),  # 对应 SQL 的 DECIMAL(20,2)
                        '原毛利率': String(20)  # 对应 SQL 的 VARCHAR(20)
                    }
                )
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    def revenue_old_tosql(self):
        folder_path = r'E:\\MK\\数据源\\毛利报表old'  # Your folder path
        df_all = []
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)

                # Read the Excel file starting from row 2 (header=1)
                df = pd.read_excel(file_path, header=1)
                # Fill NA values with '/'
                # df.fillna('/', inplace=True)
                # Insert the data into the 'revenue' table
                df.to_sql(
                    'revenue_old',  # Table name
                    con=self.engine,  # Database engine
                    if_exists='append',  # Append data if table already exists
                    index=False,  # Do not write row numbers
                    dtype={  # Column data types
                            '序号': Integer(),  # Corresponds to SQL INT
                            '发运单号': String(255),  # Corresponds to SQL VARCHAR(255)
                            '业务日期': String(255),  # Corresponds to SQL VARCHAR(255)
                            '车型': String(255),  # Corresponds to SQL VARCHAR(255)
                            '车牌号': String(255),  # Corresponds to SQL VARCHAR(255)
                            '客户': String(255),  # Corresponds to SQL VARCHAR(255)
                            '发货地点': String(500),  # Corresponds to SQL VARCHAR(500)
                            '到货地点': String(500),  # Corresponds to SQL VARCHAR(500)
                            '简要地址': String(255),  # Corresponds to SQL VARCHAR(255)
                            '承运商': String(255),  # Corresponds to SQL VARCHAR(255)
                            '公司': String(200),  # Corresponds to SQL VARCHAR(200)
                            '结算价': DECIMAL(20, 2),  # Corresponds to SQL DECIMAL(20, 2)
                            '采购价': DECIMAL(20, 2),  # Corresponds to SQL DECIMAL(20, 2)
                            '毛利额': DECIMAL(20, 2),  # Corresponds to SQL DECIMAL(20, 2)
                            '毛利率': String(20),  # Corresponds to SQL VARCHAR(20)
                            '运营费用': DECIMAL(20, 2),  # Corresponds to SQL DECIMAL(20, 2)
                            '备注': String(500),  # Corresponds to SQL VARCHAR(500)
                            '创建日期': String(255),  # Corresponds to SQL VARCHAR(255)
                            '业务类型': String(255),  # Corresponds to SQL VARCHAR(255)
                            '是否找车': String(255),  # New field for "是否找车" (Car Sourcing)
                            '支付类型': String(255),  # New field for "支付类型" (Payment Type)
                            '原结算价': DECIMAL(20, 2),  # Corresponds to SQL DECIMAL(20, 2)
                            '原毛利额': DECIMAL(20, 2),  # Corresponds to SQL DECIMAL(20, 2)
                            '原毛利率': String(20)
                        }
                    )
            print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    def business_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\\MK\\数据源\\业务开发'
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path)
                # df = df[['公司', '运单号', '日期', '客户', '客户名称', '承运商', '发货地点', '到货地点', '车型', '类型', '车牌号', '司机标记', '司机姓名', '联系方式', '运费收入', '特殊运费收入', '装卸费收入', '其他收入6', '其他收入0', '收入总金额', '支付类型', '运输费用', '油卡费用', '运输服务费', '装卸费成本', '装卸费支付类型', '装卸费服务费', '总服务费', '运营费用', '成本总金额', '备注', '收货联系人', '收货联系人电话', '发货联系人', '发货联系人电话', '外部单号', '支付方式', '收入对账', '收入对账人', '收入对账备注', '成本对账', '成本对账人', '成本对账备注', '业务开发员', '起点终点距离', '吨位', '回单是否回收', '回单快递单号', '港区代理', '报关', '文件', '操作费', '代收代垫', '其他异常费0', '其他费6', '提单号', '火车皮号']]
                df.to_sql('business', con=engine, if_exists='append', index=False, dtype={
                    '公司': String(100),
                    '运单号': String(50),
                    '日期': String(50),
                    '客户': String(100),
                    '客户名称': String(100),
                    '承运商': String(100),
                    '发货地点': String(500),
                    '到货地点': String(500),
                    '车型': String(50),
                    '类型': String(50),
                    '类型新': String(50),  # 新增字段
                    '车牌号': String(50),
                    '司机标记': String(50),
                    '司机姓名': String(50),
                    '联系方式': String(50),
                    '运费收入': String(50),
                    '特殊运费收入': String(50),
                    '装卸费收入': String(50),
                    '其他收入6': String(50),
                    '其他收入0': String(50),
                    '收入总金额': String(50),
                    '支付类型': String(50),
                    '运输费用': String(50),
                    '油卡费用': String(50),
                    '运输服务费': String(50),
                    '装卸费成本': String(50),
                    '装卸费支付类型': String(50),
                    '装卸费服务费': String(50),
                    '总服务费': String(50),
                    '运营费用': String(50),
                    '成本总金额': String(50),
                    '备注': String(500),
                    '收货联系人': String(50),
                    '收货联系人电话': String(50),
                    '发货联系人': String(50),
                    '发货联系人电话': String(50),
                    '外部单号': String(200),
                    '支付方式': String(50),
                    '收入对账': String(50),
                    '收入对账人': String(50),
                    '收入对账备注': String(255),
                    '成本对账': String(50),
                    '成本对账人': String(50),
                    '成本对账备注': String(255),
                    '业务开发员': String(50),
                    '起点终点距离': String(50),
                    '吨位': String(50),
                    '回单是否回收': String(50),
                    '回单快递单号': String(50),
                    '港区代理': String(50),
                    '报关': String(50),
                    '文件': String(50),
                    '操作费': String(50),
                    '代收代垫': String(50),
                    '其他异常费0': String(50),
                    '其他费6': String(50),
                    '提单号': String(100),
                    '火车皮号': String(50),
                    '调度员': String(50),  # 新增字段
                    '创建人': String(50),  # 新增字段
                    '项目名称': String(100),  # 新增字段
                    '关联运单号': String(100),  # 新增字段
                    '货物类型': String(100),  # 新增字段
                    '货物信息': String(500),
                    '收入完结': String(255),
                    '收入完结人': String(255),
                    '成本完结' : String(255),
                    '成本完结人' : String(255),
                })
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    def examine_tosql(self):
       engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
       folder_path = r'E:\\MK\\数据源\\运费考核明细'

       for root, dirs, files in os.walk(folder_path):
           for filename in files:
               if filename.endswith('.xlsx') or filename.endswith('.xls') or filename.endswith('.csv'):
                   file_path = os.path.join(root, filename)
                   try:
                       if filename.endswith('.xlsx'):
                           first_row = pd.read_excel(file_path, header=None, nrows=1, engine='openpyxl')
                       elif filename.endswith('.xls'):
                           first_row = pd.read_excel(file_path, header=None, nrows=1, engine='xlrd')
                       elif filename.endswith('.csv'):
                           try:
                               first_row = pd.read_csv(file_path, header=None, nrows=1, encoding='utf-8')
                           except UnicodeDecodeError:
                               try:
                                   first_row = pd.read_csv(file_path, header=None, nrows=1, encoding='gbk')
                               except UnicodeDecodeError:
                                   first_row = pd.read_csv(file_path, header=None, nrows=1, encoding='iso-8859-1')
                       else:
                           continue

                       if '运费考核明细查询' in first_row.iloc[0].values:
                           if filename.endswith('.xlsx'):
                               df = pd.read_excel(file_path, header=1, engine='openpyxl')
                           elif filename.endswith('.xls'):
                               df = pd.read_excel(file_path, header=1, engine='xlrd')
                           elif filename.endswith('.csv'):
                               try:
                                   df = pd.read_csv(file_path, header=1, encoding='utf-8', low_memory=False)
                               except UnicodeDecodeError:
                                   try:
                                       df = pd.read_csv(file_path, header=1, encoding='gbk', low_memory=False)
                                   except UnicodeDecodeError:
                                       df = pd.read_csv(file_path, header=1, encoding='iso-8859-1', low_memory=False)
                       else:
                           if filename.endswith('.xlsx'):
                               df = pd.read_excel(file_path, header=0, engine='openpyxl')
                           elif filename.endswith('.xls'):
                               df = pd.read_excel(file_path, header=0, engine='xlrd')
                           elif filename.endswith('.csv'):
                               try:
                                   df = pd.read_csv(file_path, header=0, encoding='utf-8', low_memory=False)
                               except UnicodeDecodeError:
                                   try:
                                       df = pd.read_csv(file_path, header=0, encoding='gbk', low_memory=False)
                                   except UnicodeDecodeError:
                                       df = pd.read_csv(file_path, header=0, encoding='iso-8859-1', low_memory=False)

                       if '产品大类.1' in df.columns :
                           df = df.drop(columns=['产品大类.1'])
                       if '表头 备案编号' in df.columns:
                           df = df.drop(columns=['表头 备案编号'])

                       df.to_sql('examine', con=engine, if_exists='append', index=False, dtype={
                           '机构名称': String(100),
                           '发运单号': String(50),
                           '总金额': String(50),
                           '不变价金额': String(50),
                           '运费': String(50),
                           '车型': String(50),
                           '发运单类型': String(50),
                           '序号': String(50),
                           '类型': String(50),
                           '单据编号': String(50),
                           '机构代码': String(50),
                           '客户代码': String(50),
                           '客户名称': String(100),
                           '到货地址': String(500),
                           '类别编号': String(50),
                           '类别名称': String(50),
                           '物料编号': String(50),
                           '物料名称': String(150),
                           '规格型号': String(50),
                           '总数量': String(50),
                           '总重量': String(50),
                           '承运商': String(100),
                           '产品大类': String(100),
                           '库存金额': String(50),
                           '车牌号': String(50),
                           '新增列': String(255),
                           '特殊运费说明': String(255),
                           '区域类型': String(100),
                           '参照机构代码': String(255),
                           '承担运费类型': String(50),
                           '承担运费': String(50)
                       })
                       print(f"File {filename} has been imported successfully.")
                   except Exception as e:
                       print(f"Error reading {file_path}: {e}")

    def g7_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\MK\数据源\平台\G7\订单'

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if filename.endswith('.xlsx'):
                # .xlsx格式使用openpyxl引擎
                df = pd.read_excel(file_path, engine='openpyxl')
                df.to_sql('g7', con=engine, if_exists='append', index=False, dtype={
                    '序号': String(50),
                    '所属组织': String(100),
                    '运单号': String(50),
                    '车线名称': VARCHAR(500),
                    '车线里程': String(50),
                    '运单状态': String(50),
                    '车牌号': String(50),
                    '姓名': String(50),
                    '联系电话': String(50),
                    '开单时间': String(50),
                    '发车时间': String(50),
                    '到达时间': String(50),
                    '客户名称': String(100),
                    '货物名称': String(100),
                    '总件数': String(50),
                    '总重量': String(50),
                    '应付现付': String(50),
                    '应付到付': String(50),
                    '应付回付': String(50),
                    '应付运输费合计': String(50),
                    '挂车牌号': String(50),
                    '票据要求': String(50),
                    '副驾司机': String(50),
                    '副驾司机电话': String(50),
                    '在途时长(h)': String(50),
                    '实际里程': String(50),
                    '经办人': String(50),
                    '运单备注': String(300),
                    '创建时间': String(50),
                    '创建人': String(50),
                    '需求车型': String(50),
                    '需求车长': String(50),
                    '实际车型': String(50),
                    '实际车长': String(50),
                    '运单标识': String(50),
                    '到站区县': String(50),
                    '到站省': String(50),
                    '到站市': String(50),
                    '到站': String(50),
                    '运单来源': String(50),
                    '点位数': String(50),
                    '已结应付运输费合计': String(50),
                    '未结应付运输费合计': String(50),
                    '车线时效(h)': String(50),
                    '理论油耗': String(50),
                    '装车时长': String(50),
                    '发站省': String(50),
                    '发站市': String(50),
                    '发站区县': String(50)
                })
                print(f"File {filename} has been imported successfully.")
            elif filename.endswith('.xls'):
                # .xls格式使用xlrd引擎
                df = pd.read_excel(file_path, engine='xlrd')
                df.to_sql('g7', con=engine, if_exists='append', index=False, dtype={
                    '序号': String(50),
                    '所属组织': String(100),
                    '运单号': String(50),
                    '车线名称': VARCHAR(500),
                    '车线里程': String(50),
                    '运单状态': String(50),
                    '车牌号': String(50),
                    '姓名': String(50),
                    '联系电话': String(50),
                    '开单时间': String(50),
                    '发车时间': String(50),
                    '到达时间': String(50),
                    '客户名称': String(100),
                    '货物名称': String(100),
                    '总件数': String(50),
                    '总重量': String(50),
                    '应付现付': String(50),
                    '应付到付': String(50),
                    '应付回付': String(50),
                    '应付运输费合计': String(50),
                    '挂车牌号': String(50),
                    '票据要求': String(50),
                    '副驾司机': String(50),
                    '副驾司机电话': String(50),
                    '在途时长(h)': String(50),
                    '实际里程': String(50),
                    '经办人': String(50),
                    '运单备注': String(300),
                    '创建时间': String(50),
                    '创建人': String(50),
                    '需求车型': String(50),
                    '需求车长': String(50),
                    '实际车型': String(50),
                    '实际车长': String(50),
                    '运单标识': String(50),
                    '到站区县': String(50),
                    '到站省': String(50),
                    '到站市': String(50),
                    '到站': String(50),
                    '运单来源': String(50),
                    '点位数': String(50),
                    '已结应付运输费合计': String(50),
                    '未结应付运输费合计': String(50),
                    '车线时效(h)': String(50),
                    '理论油耗': String(50),
                    '装车时长': String(50),
                    '发站省': String(50),
                    '发站市': String(50),
                    '发站区县': String(50)
                })
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    def lyt_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\MK\数据源\平台\陆运通'

        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path)
                df.to_sql('lyt', con=engine, if_exists='append', index=False, dtype={
                    '订单号': String(50),
                    '外部订单号': String(50),
                    '客户单号': String(500),
                    '装货地': String(255),
                    '装货单位': String(255),
                    '卸货地': String(255),
                    '卸货单位': String(255),
                    '货物名称': String(255),
                    '规格型号': String(50),
                    '货物包装': String(50),
                    '承运司机': String(50),
                    '车牌号': String(50),
                    '收款人': String(50),
                    '总运费(元)': String(50),
                    '结算单价(元)': String(50),
                    '结算单位': String(50),
                    '结算数量': String(50),
                    '重量(吨)': String(50),
                    '浮动费用(元)': String(50),
                    '赔付金额(元)': String(50),
                    '附加费(元)': String(50),
                    '挂车车牌号': String(50),
                    '用车要求': String(50),
                    '出厂单价(元)': String(50),
                    '出厂数量': String(50),
                    '原发数量': String(50),
                    '方案名称': String(50),
                    '货物价值(万元)': String(50),
                    '平台投保费用(元)': String(50),
                    '司机保费(元)': String(50),
                    '创建时间': String(50),
                    '订单状态': String(50),
                    '银行卡变更状态': String(50),
                    '平台审核状态': String(50),
                    '退款状态': String(50),
                    '异常原因': String(50),
                    '部门': String(100),
                    '操作人': String(50),
                    '总评分': String(50),
                    '标记状态': String(50),
                    '标记备注': String(255),
                    '车型车长': String(50),
                    '预计公里数(km)': String(50),
                    '剩余支付时间': String(50),
                    '关闭原因': String(50),
                    '支付状态': String(50),
                    '首付款(元)': String(50),
                    '到付款(元)': String(50),
                    '尾款(元)': String(50),
                    '运费(元)': String(50),
                    '开票状态': String(50),
                    '公司主体': String(100),
                    '发货方': String(100),
                    '备注': String(255),
                    '合同运费(元)': String(50),
                    '到货数量': String(50),
                    '接单时间': String(50),
                    '运输到达时间': String(50),
                    '订单完成时间': String(50),
                    '平台报价状态': String(50),
                    '货主报价状态': String(50),
                    '司机报价状态': String(50),
                    '司机在途异常': String(50),
                    '车辆在途异常': String(50)
                })
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")


    def g7_paid_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\MK\数据源\平台\G7-已付'

        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path)
                df.to_sql('g7_paid', con=engine, if_exists='append', index=False, dtype={
                })
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    def myb_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\MK\数据源\平台\满运宝'

        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path, header=1)
                df.to_sql('myb', con=engine, if_exists='append', index=False, dtype={
                    '订单号': String(50),
                    '计划单号': String(50),
                    '内部订单号': String(50),
                    '内部备注': String(255),
                    '订单类型': String(50),
                    '订单状态': String(50),
                    '创建时间': String(50),
                    '路线': String(50),
                    '总运费(元)': String(50),
                    '实付总运费(元)': String(50),
                    '订金(元)': String(50),
                    '预付金额': String(50),
                    '预付油费': String(50),
                    '实际预付金额': String(50),
                    '预付金额付款时间': String(50),
                    '到付金额(元)': String(50),
                    '实际到付金额(元)': String(50),
                    '到付金额付款时间': String(50),
                    '回单金额(元)': String(50),
                    '实际回单金额(元)': String(50),
                    '回单金额付款时间': String(50),
                    '附加运费': String(50),
                    '扣减运费金额': String(50),
                    '额外费用金额': String(50),
                    '录单员': String(50),
                    '调度': String(50),
                    '司机信息': String(50),
                    '收款人信息': String(50),
                    '车型车长': String(50),
                    '重量（吨）/体积（方）': String(50),
                    '用车类型': String(50),
                    '货物名称': String(255),
                    '装卸方式': String(50),
                    '装货时间': String(50),
                    '卸货时间': String(50),
                    '补充约定': String(255),
                    '订单完结时间': String(50),
                    '回单状态': String(50),
                    '开票状态': String(50),
                    '协议/运输单状态': String(50),
                    '公司': String(100),
                    '客户名称': String(100),
                    '保额': String(50),
                    '保险状态': String(50),
                    '是否找车': String(50)
                })
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    # def driver_tosql(self):
    #     engine = self.engine
    #     folder_path = r'E:\MK\数据源\司机'
    #     all = []
    #     for filename in os.listdir(folder_path):
    #         if filename.endswith('.xlsx') or filename.endswith('.xls'):
    #             file_path = os.path.join(folder_path, filename)
    #             df = pd.read_excel(file_path, header=0)
    #             df = df.groupby(['车牌号']).agg({
    #             df = df.groupby(['车牌号']).agg({
    #                 '实际车长': 'first',
    #                 '司机姓名': 'first',
    #                 '司机电话': 'first'
    #             }
    #              ).reset_index()
    #             df = df.drop_duplicates()
    #             all.append(df)
    #     combined_df = pd.concat(all, ignore_index=True)
    #     combined_df = combined_df.drop_duplicates()
    #     combined_df['司机电话'] = combined_df['司机电话'].str.replace(' ','')
    #     combined_df.to_sql('driver', con=engine, if_exists='append', index=False, dtype={
    #                         '车牌号': String(15),
    #                         '实际车长': String(10),
    #                         '司机姓名': String(20),
    #                         '司机电话': String(30)
    #                         })
    #     print(f"Files has been imported successfully.")

    def fee_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\\MK\数据源\运费管理'
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path)
                df.to_sql('fee', con=engine, if_exists='append', index=False, dtype={
                    '选择': VARCHAR(20),
                    '发运单编号': VARCHAR(20),
                    '发运类型': VARCHAR(20),
                    '到货地址': VARCHAR(500),
                    '简要地址': VARCHAR(50),
                    '业务日期': VARCHAR(20),
                    '运费状态': VARCHAR(20),
                    '车牌号': VARCHAR(50),
                    '司机姓名': VARCHAR(50),
                    '司机联系方式': VARCHAR(50),
                    '车型名称': VARCHAR(20),
                    '承运商': VARCHAR(50),
                    '标准运费': VARCHAR(20),
                    '实际运费': VARCHAR(20),
                    '特殊运费': VARCHAR(20),
                    '特殊运费类型': VARCHAR(50),
                    '特殊运费说明': VARCHAR(500),
                    '随车资料': VARCHAR(500),
                    '备注': VARCHAR(500),
                    '管道金额': VARCHAR(20),
                    '管道重量': VARCHAR(20),
                    '型材数量': VARCHAR(20),
                    '整车金额': VARCHAR(20),
                    '管道出厂金额': VARCHAR(20),
                    '创建人': VARCHAR(20),
                    '创建时间': VARCHAR(20),
                    '发货公司': VARCHAR(50),
                    '发出时间': VARCHAR(20),
                    '装卸组名称': VARCHAR(50),
                    '营销机构': VARCHAR(1000),
                    '是否转单': VARCHAR(20),
                    '提交时间': VARCHAR(20),
                    '区域类型': VARCHAR(20),
                    '费率参照机构代码': VARCHAR(20),
                    '承担运费类型': VARCHAR(20),
                    '承担运费': VARCHAR(20)
                })
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    def delivery_tosql(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        folder_path = r'E:\\MK\数据源\发运单管理'
        for filename in os.listdir(folder_path):
            if filename.endswith('.xlsx') or filename.endswith('.xls'):
                file_path = os.path.join(folder_path, filename)
                df = pd.read_excel(file_path)
                df.to_sql('delivery', con=engine, if_exists='append', index=False, dtype={
                          '选择': String(20),
                          '发运单编号': String(20),
                          '发运类型': String(20),
                          '到货地址': String(500),
                          '简要地址': String(100),
                          '业务日期': String(20),
                          '状态': String(20),
                          '车牌号': String(50),
                          '司机姓名': String(50),
                          '司机联系方式': String(50),
                          '车型名称': String(20),
                          '承运商': String(100),
                          '其它要求': String(500),
                          '装货点': String(500),
                          '要求到达时间': String(20),
                          '随车资料': String(500),
                          '备注': String(500),
                          '管道金额': String(20),
                          '管道重量': String(20),
                          '型材金额': String(20),
                          '整车金额': String(20),
                          '管道出厂金额': String(20),
                          '排单日期': String(20),
                          '排单时间': String(20),
                          '排单情况': String(500),
                          '提交时间': String(20),
                          '接收时间': String(20),
                          '到厂时间': String(20),
                          '开始装车时间': String(20),
                          '发出时间': String(20),
                          '创建人': String(20),
                          '创建时间': String(20),
                          '发货公司': String(50),
                          'IC卡': String(20),
                          '装卸组名称': String(50),
                          '营销机构': String(800),
                          '是否转单': String(20),
                          '转单说明': String(500),
                          '发货单据条数': String(20),
                          '提交调车日期': String(20),
                          '区域类型': String(20),
                          '费率参照机构代码': String(20),
                          '承担运费类型': String(20),
                          '承担运费': String(20),
                          '空载重量': String(20),
                          '满载重量': String(20)
                })
                print(f"File {filename} has been imported successfully.")
        print("All files have been imported.")

    def inventory(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        df_exam = pd.read_sql('SELECT 发运单号,类别名称,总重量 FROM examine',con=engine)
        df_revenue = pd.read_sql('SELECT 发运单号,业务类型,公司,车型,业务日期 FROM revenue',con=engine)
        df_revenue['业务日期'] = pd.to_datetime(df_revenue['业务日期'],format='%Y%m%d')
        df_revenue['年'] = df_revenue['业务日期'].dt.year
        df_revenue['月'] = df_revenue['业务日期'].dt.month
        df_merge = pd.merge(df_exam,df_revenue,on='发运单号',how='left')
        df_merge['发运单号'] = df_merge['发运单号'].replace('-1','')
        df_merge['总重量'] = df_merge['总重量'].astype('float')
        df_agg = df_merge.groupby(['公司','类别名称','年','车型']).agg(总重量=('总重量','sum'),订单均重量=('总重量','mean')).reset_index()
        df_agg_2 = df_merge.groupby(['公司','年','车型','发运单号']).agg(整车数量=('总重量','count')).reset_index()
        df_agg_2['整车数量'] = 1
        df_agg_3 = df_agg_2.groupby(['公司','年', '车型']).agg(整车数量=('整车数量', 'sum')).reset_index()
        df_agg_4 = pd.merge(df_agg,df_agg_3,on=['公司','年','车型'],how='left')
        df_agg_4['车均装载'] = df_agg_4['总重量']/df_agg_4['整车数量']
        df_agg_4['排序'] = df_agg_4.groupby(['公司', '车型', '年'])['车均装载'].rank(ascending=False, method='dense')
        df_agg_4 = df_agg_4.sort_values(by=['公司','车型','年','车均装载'],ascending=[True,True,True,False])
        df_agg_4 = df_agg_4[df_agg_4['排序'].isin([1,2,3,4,5])]
        df_agg_4_2023 = df_agg_4[df_agg_4['年'] == 2023]
        df_agg_4_2024 = df_agg_4[df_agg_4['年'] == 2024]
        df_agg_4_2324 = pd.merge(df_agg_4_2024,df_agg_4_2023,on=['公司','车型','排序'],how='left')
        print(df_agg)
        df_agg.to_excel(r'E:\MK\数分\大数据分析\货物分析\配载\1.xlsx')
        df_agg_4.to_excel(r'E:\MK\数分\大数据分析\货物分析\配载\2.xlsx')
        df_agg_4_2324.to_excel(r'E:\MK\数分\大数据分析\货物分析\配载\2324.xlsx')
        return df_agg

    def to_sql_order_manage(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/ccd')
        path = fr"E:\MK\数据源\发运单管理"
        for filename in os.listdir(path):
            if filename.endswith('xls') or filename.endswith('xlsx'):
                df = pd.read_excel(fr"{path}\{filename}")
                df.to_sql('order_manage', con=engine, if_exists='append', index=False)


    def drop_s(self,charts):
        for i in charts:
            self.drop(i)
        print(f'{charts} has been dropped')

    def to_sql(self,charts):
        # Mapping table names to functions
        function_map = {
            'revenue': self.revenue_tosql,
            'business': self.business_tosql,
            'examine': self.examine_tosql,
            'g7': self.g7_tosql,
            'lyt': self.lyt_tosql,
            'myb': self.myb_tosql,
            'car': self.car_tosql,
            'fee':self.fee_tosql,
            'delivery':self.delivery_tosql,
            'revenue_old':self.revenue_old_tosql,
            'order_manage':self.to_sql_order_manage,
            'g7_paid':self.g7_paid_tosql,
        }



        # Iterate over the charts and call corresponding functions
        for chart in charts:
            if chart in function_map:
                function_map[chart]()
            else:
                print(f"No function defined for {chart}")



class GD():
    def GD_database(self):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306')
        with engine.connect() as conn:
            conn.execute(text("CREATE DATABASE IF NOT EXISTS GD"))
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/GD')
        with engine.connect() as conn:
            conn.execute(text("DROP TABLE IF EXISTS revenue_address"))
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/GD')
        with engine.connect() as conn:
            conn.execute(text("""
        CREATE TABLE IF NOT EXISTS revenue_address (
            point VARCHAR(500),
            point_standard VARCHAR(100),
            point_confirmed VARCHAR(100),
            longitude VARCHAR(30),
            latitude VARCHAR(30)
        );
        """))
    def to_sql_revenue_address(self,path):
        engine = create_engine('mysql+pymysql://root:xiannan@localhost:3306/GD')
        for filename in os.listdir(path):
            if filename.endswith('xls') or filename.endswith('xlsx'):
                df = pd.read_excel(fr"{path}\{filename}")
                df.rename(columns={'地点':'point','标准化地点':'point_standard','最终地址':'point_confirmed'},inplace=True)
                df.to_sql('revenue_address', con=engine, if_exists='append', index=False,
                          dtype={
                         'point': String(500),
                         'point_standard': String(100),
                         'point_confirmed': String(100),
                         'longitude': String(30),
                         'latitude': String(30)}
                          )