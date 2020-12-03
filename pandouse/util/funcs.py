import os
from datetime import datetime
import pandas as pd
from loguru import logger

class HandleCH:
    @staticmethod
    def create_engine(db_kw='100_ark'):
        "自动生成数据库引擎"
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
        "插入数据库中"
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


class Logger:
    '''日志记录'''
    @staticmethod
    def log_path(log_name):
        '''获取日志路径'''
        file_path = os.path.join(PATH_LOG, log_name + '_{time:YYYYMMDD}.log')

        return file_path

    @staticmethod
    def add(log_name, retention='10 days', rotation='1 day'):
        '''创建日志文件, 每天创建一个日志文件, 最多停留10天
        --------------
        1. logger.add('runtime_{time}.log', rotation="500 MB")   每 500MB 存储一个文件
        2. logger.add('runtime_{time}.log', rotation='00:00')    每天 0 点新创建一个 log 文件
        3. logger.add('runtime_{time}.log', rotation='1 week')   每隔一周创建一个 log 文件
        4. logger.add('runtime.log', retention='10 days')        日志文件最长保留 10 天
        '''
        log_path = Logger.log_path(log_name)
        log = logger.add(sink=log_path, format="{time} {level} {message}", retention=retention, rotation=rotation)
        return log

    @staticmethod
    def logger(log_name):
        '''添加日志的装饰器'''
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kw):
                func_name = func.__name__
                try:
                    log = Logger.add(log_name)
                    stdout = HandleSYS.get_stdout(func, *args, **kw)
                    for msg in stdout:
                        print(msg)
                        logger.info(f'{func_name} - {msg}')
                    f = func(*args, **kw)
                except Exception as e:
                    logger.error(e)
                finally:
                    logger.remove(log)
                return f
            return wrapper
        return decorator


class HandleDB:
    '''操作数据库'''
    @staticmethod
    def get_connect(engine):
        # engine.raw_connection().cursor()
        return engine.raw_connection()

    @staticmethod
    def run_sql(sql, engine):
        engine.execute(sql)
        # engine.dispose()

    @staticmethod
    def cx_oracle_execute(sql, conn):
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()    
        conn.close()

    @staticmethod
    def cx_oracle_conn(user='ark', pwd='E2718281828', ip='192.168.123.100'):
        conn = cx_Oracle.connect(f"c##{user}/{pwd}@{ip}/ORCLCDB")

        return conn

    @staticmethod
    def truncate_table(tb_name, engine):
        sql = 'truncate table {}'.format(tb_name)
        engine.execute(sql)
        # engine.dispose()

    @staticmethod
    def del_table(tb_name, engine):
        sql = 'delete from {}'.format(tb_name)
        engine.execute(sql)

    @staticmethod
    def del_date(tb_name, engine, t_date, flag='curr_after'):
        t_date = HandleDate.date_format(str(t_date), '%Y%m%d')
        if flag == 'curr_after':   # 删除指定日期及其后的数据
            sql = "delete from {} where t_date>=to_date('{}','yyyymmdd')".format(tb_name, t_date)
        elif flag == 'curr':       # 删除指定日期数据
            sql = "delete from {} where t_date=to_date('{}','yyyymmdd')".format(tb_name, t_date)
        else:
            raise ValueError('flag:{} error !!!'.format(flag))
        print(sql)
        engine.execute(sql)
        # engine.dispose()

    @staticmethod
    def del_single_date(tb_name, engine, t_date):
        '''删除指定表的单个日期的所有数据'''
        t_date = HandleDate.date_format(str(t_date), '%Y%m%d')
        sql = "delete from {} where t_date=to_date('{}','yyyymmdd')".format(tb_name, t_date)
        print(sql)
        engine.execute(sql)

    @staticmethod
    def to_sql(df, tb_name, engine, if_exists='append'):
        type_dict = HandleDB.get_type_dict(tb_name, engine)
        if 'create_date' in type_dict.keys():
            df['create_date'] = datetime.now()
        else:
            if 'create_date' in df.columns:
                df = df.drop('create_date', axis=1)
        df.to_sql(
                    tb_name,
                    con = engine,
                    if_exists = if_exists,
                    index = False,
                    chunksize = 2000000,
                    dtype = type_dict)
        

    @staticmethod
    def read_sql(sql, engine):
        df = pd.read_sql(sql,engine)
        engine.dispose()
        return df

    @staticmethod
    def get_type_dict(tb_name, engine=ENGINE_RUNTIME):
        '''
        反射获取数据库中各字段的数据类型
        '''
        metadata = MetaData(engine)
        type_dict = {}
        columns = list(Table(tb_name, metadata, autoload=True).columns)
        for col in columns:
            type_dict[col.name] = col.type
        return type_dict

    @staticmethod
    def get_symbol_ls(t_date=None, engine=ENGINE_RUNTIME, _type='stock'):
        '''获取股票/指数/基金/金融期货等的代码'''
        if t_date==None: # 默认取全部
            if _type == 'stock':
                sql = 'select distinct symbol from b_stk_ipo_date'
            elif _type == 'index':
                sql = 'select distinct idx_code symbol from b_idx_bar_1day'
            elif _type == 'fund':
                sql = 'select distinct fnd_code symbol from b_fnd_info'
        elif t_date != None:  # 获取日期的代码列表
            t_date = HandleDate.date_format(str(t_date), '%Y%m%d')
            if _type == 'stock':     
                sql = f"""
                    select distinct symbol 
                    from b_stk_ipo_date 
                    where ipo_date <= to_date('{t_date}', 'yyyymmdd') 
                    and delist_date>to_date('{t_date}','yyyymmdd')
                """
            elif _type == 'index':
                sql = f"""
                    select distinct idx_code symbol
                    from b_idx_bar_1day
                    where t_date = to_date('{t_date}', 'yyyymmdd') 
                """
            elif _type == 'fund':
                sql = f"""
                    select distinct fnd_code symbol
                    from b_fnd_info
                    where list_date <= to_date('{t_date}', 'yyyymmdd') 
                    and delist_date>to_date('{t_date}','yyyymmdd')
                """
        symbols = pd.read_sql(sql, engine).symbol.tolist()
        if '990018.SH' in symbols:
            symbols.remove('990018.SH')

        return symbols

    @staticmethod
    def get_all_symbols():
        sql = 'select distinct symbol from b_stk_ipo_date'
        df = pd.read_sql(sql, con=ENGINE_RUNTIME)

        return df.symbol.tolist()

    @staticmethod
    def get_trading_dates(s_date, e_date, engine=ENGINE_RUNTIME):
        sql = """select t_date 
                from b_cal_trading_dates 
                where t_date>=to_date('{s_date}', 'yyyymmdd') 
                and t_date<=to_date('{e_date}', 'yyyymmdd') 
                order by t_date
        """.format(
            s_date = pd.to_datetime(s_date).strftime('%Y%m%d'),
            e_date = pd.to_datetime(e_date).strftime('%Y%m%d'))
        df = pd.read_sql(sql, engine)
        date_ls = df.t_date.tolist()
        return date_ls

    @staticmethod
    def is_trading_date(date, engine=ENGINE_RUNTIME):
        date = pd.to_datetime(date).strftime('%Y%m%d')
        sql = "select count(*) c from b_cal_trading_dates where t_date=to_date('{}','yyyymmdd')".format(date)
        df = pd.read_sql(sql, con=engine)

        return df.c[0] == 1

    @staticmethod
    def get_ipo_date(engine=ENGINE_RUNTIME):
        '''获取IPO日期数据'''
        sql = 'select symbol, ipo_date from b_stk_ipo_date'
        return pd.read_sql(sql, engine)

    @staticmethod
    def get_tables(engine):
        '''获取指定数据库中的表列表'''
        sql = 'select table_name from user_tables order by table_name'

        return pd.read_sql(sql, engine).table_name.to_list()

    @staticmethod
    def fallback(tb_name, t_date, engine, has_tz=False):
        '''数据库回退到指定日期的前一日结束的状态
        :params:    tb_name    要回退的数据库表
        :params:    t_date     要回退到的日期(的后一日)
        :params:    engine     数据库engine
        :params:    has_tz     是否含有时间拉链
        :return:
        '''
        t_date = HandleDate.date_format(str(t_date), '%Y%m%d')
        # 数据不含有时间拉链时
        if has_tz ==False:   # 删除t_date所有数据
            sql = """
                delete 
                from {tb_name}
                where t_date = to_date('{t_date}', 'yyyymmdd')
            """.format(tb_name=tb_name, t_date=t_date)

            with engine.begin() as conn:
                conn.execute(sql)
        # 数据含有时间拉链时
        else:
            # 删除t_date及其后日期的所有数据
            sql1 = """
                delete 
                from {tb_name}
                where t_date >= to_date('{t_date}', 'yyyymmdd')
            """.format(tb_name=tb_name, t_date=t_date)
            # 在拉链断开的地方重新设置拉链
            sql2 = """
                update {tb_name}
                set e_date = to_date('{inf_date}', 'yyyymmdd')
                where e_date >= to_date('{t_date}', 'yyyymmdd')
            """.format(tb_name=tb_name, 
                inf_date=INF_DATE, t_date=t_date)
            
            with engine.begin() as conn:
                conn.execute(sql1)
                conn.execute(sql2)

    @staticmethod
    def login_oracle():
        '''
        sudo su - oracle
        sqlplus / as sysdba
        '''
        print('sudo su - oracle')
        print('sqlplus / as sysdba')

    @staticmethod
    def kill_session():
        '''
        方法一
        ---------------
        --查询被锁的会话ID
        1. 获取sid        select session_id from v$locked_object;
        2. 获取serial#    SELECT sid, serial#, username, osuser FROM v$session where sid = 9;
        3. 杀死进程        ALTER SYSTEM KILL SESSION '9,99';
        '''
        '''
        方法二
        -----------
        1. 获取sid       select session_id from v$locked_object;
        2. 获取spid      select spid, osuser, s.program
                        from v$session s,v$process p
                        where s.paddr=p.addr and s.sid=24
        3. 杀死进程      sudo kill -9 12345
        '''
        pass

    @staticmethod
    def sqlldr(ctl_file_name, infile=None):
        '''使用sqlldr导入数据'''
        path_sqlldr = os.path.join(PATH_ROOT, 'src/sql/sqlldr')
        ctlfile = os.path.join(path_sqlldr, 'ctlfiles', ctl_file_name)
        badfile = os.path.join(path_sqlldr, 'badfile.bad')
        logfile = os.path.join(path_sqlldr, 'logfile.log')

        ip = HandleSYS.get_ip()
        if ip == '10.254.254.111':
            db_user = 'c##ark/E2718281828@10.254.254.111:1521/ORCLCDB'
        elif ip == '10.254.254.110':
            db_user = 'c##ark/31415926@10.254.254.110:1521/ORCLCDB'
        elif ip == '192.168.123.100':
            db_user = 'c##ark/E2718281828@192.168.123.100:1521/ORCLCDB'
        else:
            raise ValueError(f'IP: {ip} is invalid!')

        if infile == None:   # infile 已写入 ctl 文件
            cmd = f'''
                sqlldr \
                {db_user} \
                control={ctlfile} \
                bad={badfile} \
                log={logfile} \
                rows=10000000 \
                bindsize=2000000000 \
                readsize=2000000000 \
                direct=true'''
        else:                 # infile 作为变量输入
            cmd = f'''
                sqlldr \
                {db_user} \
                control={ctlfile} \
                data={infile} \
                bad={badfile} \
                log={logfile} \
                rows=10000000 \
                bindsize=2000000000 \
                readsize=2000000000 \
                direct=true'''
        # 执行命令
        # print(cmd)
        ret = os.system(cmd)
        if ret == 0: print('Success!')
        else: print('Failed')

