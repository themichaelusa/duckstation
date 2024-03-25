import time
import copy
import duckdb
from utils import with_duckdb_session
from utils import prep_sql_as_query
from utils import merge_n_tables_query
from utils import directory_exists_and_not_empty, rm_directory_and_contents, get_files_in_directory

## UTILS
NEW_DD_CON = lambda db, ro: duckdb.connect(database=db, read_only=ro)
RM_TRAIL_CHAR = lambda s, c: s[:-1] if s[-1] == c else s

## MAIN
class WrappedDuckDB:
    def __init__(self, con=None, sql_path=None) -> None:
        self.ddcon = con if con else NEW_DD_CON(':memory:', False)
        self.table_info = {}
        self.source_count = 0
        self.sql_path = sql_path
        self.macros_set = False
        self.use_session = True
        self.setup_macros()

    def setup(self, sql_path='./sql'):
        old_sql_path = self.sql_path
        self.update_sql_path(sql_path)
        self.cmd('setup', out=False)
        self.update_sql_path(old_sql_path)

    def setup_macros(self, macros_path='./sql/shared/macros'):
        if self.macros_set:
            return
        # get all files with paths
        files = get_files_in_directory(macros_path)
        base_sql_path = copy.deepcopy(self.sql_path)
        print(f'setup_macros::base_sql_path: ', base_sql_path)
        self.update_sql_path(macros_path)
        for file in files:
            print(f'setup_macros::file: ', file)
            if file.endswith('.sql'):
                cmd_name = file.split('.')[0] if '.' in file else None
                if not cmd_name:
                    raise Exception(f'invalid shared macro filename: {file}')
                self.cmd(cmd_name, out=False, debug=False)
        self.macros_set = True
        # get macro names
        sql = f"""
        SELECT * FROM duckdb_functions() 
        WHERE schema_name = 'main' and internal = false
        ORDER BY function_oid
        """
        self.update_sql_path(base_sql_path)
        #print(self.cmd('setup_macros', inject=sql, as_pl=True))

    def add_checkpoint(self, tag):
        sql = f"INSERT INTO checkpoints(label) VALUES ('{tag}');"
        self.cmd('add_checkpoint', inject=sql, out=False)

    def has_checkpoint(self, tag, debug=False):
        sql = f"SELECT * FROM checkpoints WHERE label='{tag}';"
        has_chp = self.cmd('has_checkpoint', inject=sql, as_pl=True)
        if debug:
            print(f'has_checkpoint::{tag}::has_chp: ', has_chp)
        has_rows = has_chp.shape[0] > 0
        return has_rows

    def update_sql_path(self, sql_path):
        self.sql_path = sql_path if sql_path else self.sql_path

    @with_duckdb_session
    def mount(self, session, path, flush=False):
        valid = directory_exists_and_not_empty(path)
        if flush and valid:
            rm_directory_and_contents(path)
            print(f'flushed DB at: {path}')
            return False
        if not valid:
            return False
        sql = f"IMPORT DATABASE '{path}';"
        time_start = time.time()
        session.execute(sql)
        print(f'mount::cmd_time: {time.time() - time_start}')
        print(f'mounted DB from: {path}')
        return True

    @with_duckdb_session
    def table_exists(self, session, table):
        sql = f"SELECT * FROM {table} LIMIT 1;"
        try:
            session.execute(sql)
            return True
        except Exception as e:
            return False

    @with_duckdb_session
    def cmd(self, session, tag=None, replace={}, debug=False, out=True, as_pl=False, inject=None):
        if inject:
            sql = inject
        else:
            sql = prep_sql_as_query(tag, replace=replace, path=self.sql_path, debug=debug)
        # if debug:
        #     print(f'{tag}::sql: ', sql)
        cmd_out = session.execute(sql) if sql else None
        if out and cmd_out:
            return cmd_out.pl() if as_pl else cmd_out.pl().to_pandas(
                use_pyarrow_extension_array=True
            )
        return True if cmd_out else False
        
    def load(self, source, table, tag='load', replace={}, debug=False):
        # modified to support BYOD via passing in a duckdb connection
        if self.table_exists(table):
            print(f'{table}::already loaded')
            return
        replace.update({
            'source': source, 
            'table': table, 
            'source_id' : self.source_count
        })
        success = self.cmd(tag=tag, replace=replace, debug=debug, out=False)
        if not success:
            raise Exception(f'no sql found for tag: {tag}')
        self.table_info[table] = { 
            'source_path': source,
            'source_id' : self.source_count,
            #'count': self.count(table),
        }
        self.source_count += 1
    
    def load_from_file(self, source, table, replace={}, debug=False):
        source = f"'{source}'"
        self.load(source, table, replace=replace, debug=debug)
    
    def load_from_table(self, source, table, replace={}, debug=False):
        self.load(source, table, replace=replace, debug=debug)

    def duplicate_table(self, table, new_table, columns=[], sample_pct=None, debug=True):
        columns = ', '.join(columns) if columns else '*'
        sql = f"CREATE TABLE {new_table} AS SELECT {columns} FROM {table}"
        sql += f" USING SAMPLE {sample_pct}%" if sample_pct else ''
        if debug:
            print(f'duplicate_table::sql: ', sql)
        self.cmd('duplicate_table', inject=sql, out=False, debug=debug)
        self.table_info[new_table] = {'source_path': table}
        #self.table_info[new_table] = self.table_info[table]

    def raw_load_from_file(self, path, table, sample_pct=None):
        sql = f"CREATE TABLE {table} AS SELECT * FROM '{path}'";
        sql += f" USING SAMPLE {sample_pct}%" if sample_pct else ''
        print(f'raw_load_from_file::sql: ', sql)
        time_start = time.time()
        self.cmd('raw_load_from_file', inject=sql, out=False)
        print(f'raw_load_from_file::cmd_time: {time.time() - time_start}')
        print(f'raw_load_from_file::loaded: {table} from: {path}')

    def count(self, table, source_id=None, column='*'):
        # check if column exists if not star
        if column != '*':
            sql = f"SELECT {column} FROM {table} LIMIT 1;"
            try:
                self.cmd('count', inject=sql, out=False)
            except Exception as e:
                #print(f'count::column: {column} does not exist in table: {table}')
                return 0
        # get count of column or rows
        where_source = f"source_id={source_id}" if source_id else 'TRUE'
        sql = f'SELECT COUNT({column}) FROM {table} WHERE {where_source}'
        if column != '*':
            sql += f' AND {column} IS NOT NULL'
        #print(f'count::sql: ', sql)
        count_rows = self.cmd('count', inject=sql, as_pl=True)
        count = count_rows.to_dicts()[0]
        count_key = list(count.keys())[0]
        return int(count[count_key])
    
    def df_pl(self, table):
        return self.ddcon.table(table).pl()
    
    def drop(self, table):
        sql = f'DROP TABLE IF EXISTS {table};'
        self.cmd('drop', inject=sql, out=False)
        self.table_info.pop(table, None)

    def merge(self, tables=[], merge_table='merged', debug=False):
        sql = merge_n_tables_query(tables, merge_table)
        if debug:
            print(f'merge::sql: ', sql)
        self.cmd('merge', inject=sql, out=False, debug=debug)
        for table in tables:
            self.drop(table)

    def columns(self, table):
        sql = f"SELECT * FROM {table} LIMIT 1;"
        return list(self.cmd('columns', inject=sql, as_pl=True).columns)
    
    def sql(self, cmd):
        time_start = time.time()
        out = self.cmd('custom_sql', inject=cmd, out=True, as_pl=True, debug=True)
        print(f'sql::cmd_time: {time.time() - time_start}')
        return out
    
    def repl(self, tag='repl'):
        while True:
            cmd = input(f'duckdb::{tag}> (type exit to quit)')
            if cmd == 'exit':
                break
            elif cmd == 'tables':
                print(self.tables())
            else:
                try:
                    print(self.sql(cmd))
                except Exception as e:
                    print(e)
    
    def tables(self):
        sql = f"SELECT table_name FROM duckdb_tables WHERE schema_name = 'main';"
        _tables = self.cmd('tables', inject=sql, as_pl=True)
        return _tables['table_name'].to_list()

    @with_duckdb_session
    def save(self, session, path, tag=''):
        dir_path = f'{path}/{tag}' if tag else path
        sql = f"EXPORT DATABASE '{dir_path}' (FORMAT PARQUET, COMPRESSION ZSTD);"
        session.execute(sql)
        print(f'saved DB to: {dir_path}')

    def export_table(self, table, path):
        #path = RM_TRAIL_CHAR(path, '/')
        sql = f"COPY (SELECT * FROM {table}) TO '{path}' (COMPRESSION ZSTD);"
        self.cmd('export_table', inject=sql, out=False)
        print(f"saved table: {table} to: {path}/{table}.parquet")