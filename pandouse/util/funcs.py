import os
from datetime import datetime
import pandas as pd

class HandleCH:
    @staticmethod
    def create_engine(db_kw='100_ark'):
        
        if db_kw == '101_ark':
            db_info = {
                'db_name':'ark', 
                'user':'bxz', 
                'pwd':'123456', 
                'ip':'192.168.123.101'}
        else:
            raise ValueError(f'database: {db_kw} is invalid!')
        # 创建engine
        from clickhouse_driver import Client
        engine = Client(
            host=db_info['ip'], 
            database=db_info['db_name'],
            user=db_info['user'] ,
            password=db_info['pwd'])
        
        return engine

    @staticmethod
    def exe_cmd(cmd):
        ret = os.system(cmd)
        if ret != 0: raise Exception(f'"{cmd}" failed!')
        else: print(f'"{cmd}" success!')

    @staticmethod
    def drop_db(db_name='ark'):
        cmd = f"clickhouse-client --query 'drop database if exists {db_name}'"
        HandleCH.exe_cmd(cmd)

    @staticmethod
    def create_db(db_name='ark'):
        HandleCH.drop_db(db_name)
        cmd = f"clickhouse-client --query 'create database {db_name}'"
        HandleCH.exe_cmd(cmd)

    @staticmethod
    def drop_tb(tb_name, db_name='ark'):
        sql = f'DROP TABLE IF EXISTS {db_name}.{tb_name}'
        cmd = f"clickhouse-client --query '{sql}'"
        HandleCH.exe_cmd(cmd)

    @staticmethod
    def create_tb(sql):
        cmd = f"clickhouse-client --query '{sql}'"
        HandleCH.exe_cmd(cmd)

    @staticmethod
    def load_csv(tb_name, csv_path, db_name='ark'):
        sql = f'INSERT INTO {db_name}.{tb_name} FORMAT CSVWithNames'
        cmd = f"""clickhouse-client -t --query '{sql}' --max_insert_block_size=1000000 < {csv_path}"""
        HandleCH.exe_cmd(cmd)

    @staticmethod
    def read_sql(sql, db_kw='101_ark', _format='df'):
        engine = HandleCH.create_engine(db_kw)
        if _format == 'list':
            data, columns = engine.execute(
                sql, columnar=True, 
                with_column_types=True)
        elif _format == 'df':
            data, columns = engine.execute(
                sql, columnar=True, 
                with_column_types=True)
            data = pd.DataFrame({re.sub(r'\W', '_', col[0]): d for d, col in zip(data, columns)})
        elif _format == 'str':
            cmd = f'clickhouse-client -t --query "{sql}"'
            data = subprocess.getoutput(cmd)

        return data

    @staticmethod
    def get_type_dict(tb_name, db_kw='101_ark'):
        sql = f"select name, type from system.columns where table='{tb_name}';"
        df = HandleCH.read_sql(sql, db_kw, 'df')
        df = df.set_index('name')
        type_dict = df.to_dict('dict')['type']

        return type_dict

    @staticmethod
    def to_sql(df, tb_name, db_name='ark', db_kw='101_ark'):
        '''DataFrame数据存入ClickHouse'''
        # 处理数据类型
        type_dict = HandleCH.get_type_dict(tb_name, db_kw)
        int_cols = [k for k, v in type_dict.items() if 'Int' in v]
        date_cols = [k for k, v in type_dict.items() if 'Date' in v]
        for col in int_cols:
            df[col] = df[col].convert_dtypes('int')
        for col in date_cols:
            df[col] = pd.to_datetime(df[col].astype('str')).astype('int')/10**9
        # 生成临时CSV文件路径
        import hashlib
        args = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        csv_name = hashlib.md5(args.encode('utf-8')).hexdigest() + '.csv'
        csv_path = os.path.join(PATH_DATA_TMP, 'save2ch', csv_name)
        # 将df存入CSV
        df.to_csv(csv_path, index=False, date_format='%Y-%m-%d %H:%M:%S.%f')
        # 将CSV导入ClickHouse
        HandleCH.load_csv(tb_name, csv_path, db_name)
        # 删除临时CSV文件
        os.remove(csv_path)

    @staticmethod
    def log(sql, client='101_ark'):
        engine = HandleCH.create_engine(client)
        cmd = f'clickhouse-client --send_logs_level=debug <<< "{sql}" > /dev/null'
        ret = subprocess.getoutput(cmd)
        log_path = os.path.expanduser('~/projects/clickhouse/src/log/query.log')
        with open(log_path, 'a') as f:
            f.write('\n')
            f.write(ret)
            f.write('\n') 
