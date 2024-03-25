import os
import re
import time
import shutil
from functools import wraps

## OS UTILS
def directory_exists_and_not_empty(path):
    try:
        if not path:
            return False
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            return False
        return len(os.listdir(path)) > 0
    except Exception as e:
        #print(f'error checking if directory exists and not empty: {e}')
        return False
    
def directory_exists(path):
    try:
        if not path:
            return False
        path = os.path.expanduser(path)
        return os.path.exists(path)
    except Exception as e:
        #print(f'error checking if directory exists: {e}')
        return False

def rm_directory_and_contents(path):
    try:
        shutil.rmtree(path)
        return True
    except Exception as e:
        #print(f'error removing directory and contents: {e}')
        return False
    
def create_directory(path):
    try:
        os.makedirs(path)
        return True
    except Exception as e:
        #print(f'error creating directory: {e}')
        return False
    
def file_exists_and_not_empty(path):
    try:
        if not path:
            return False
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            return False
        return os.path.getsize(path) > 0
    except Exception as e:
        #print(f'error checking if file exists and not empty: {e}')
        return False
    
def get_files_in_directory(path):
    try:
        if not path:
            return []
        path = os.path.expanduser(path)
        if not os.path.exists(path):
            return []
        return os.listdir(path)
    except Exception as e:
        #print(f'error getting files in directory: {e}')
        return []
    
## DUCKDB UTILS (for session management)
def with_duckdb_session(func):
    @wraps(func)
    def session_manager(self, *args, **kwargs):
        if self.use_session:
            session = self.ddcon.cursor()
            try:
                return func(self, session, *args, **kwargs)
            finally:
                session.close()
        else:
            return func(self, self.ddcon, *args, **kwargs)
    return session_manager

def timed(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        time_start = time.time()
        res = func(*args, **kwargs)
        print(f'{func.__name__}::time: {time.time() - time_start}')
        return res
    return wrapper

### SQL UTILS
def prep_sql_as_query(query, replace={}, path='./sql/jaccard_sieve', debug=False):
    if not query or not path:
        print(f'maybe query null: {query}')
        print(f'maybe path null: {path}')
        print('prep_sql_as_query::query or path is None')
        return None
    with open(f'{path}/{query}.sql', 'r') as f:
        sql = f.read()
        # Sort the keys by length in descending order to replace the longer keys first
        for k in sorted(replace.keys(), key=len, reverse=True):
            # We create a regex pattern that matches the $ symbol, the key, 
            # and ensures that it is not followed by any word characters.
            pattern = r'\$' + re.escape(k) + r'(?!\w)'
            value = str(replace[k])
            sql = re.sub(pattern, value, sql)
        # if any $ symbols remain, replace them with empty strings
        sql = re.sub(r'\$\w+', '', sql)
    if debug:
        print(f'load_sql_as_query::{query}::sql: ', sql)
    return sql

def merge_n_tables_query(tables, merge_table):
    if type(tables) != list or len(tables) == 0 or not merge_table:
        return None
    sql = f"CREATE TABLE {merge_table} AS ("
    if len(tables) == 1:
        return sql + f"SELECT * FROM {tables[0]}" + " );"
    for i, table in enumerate(tables):
        sql += f" SELECT * FROM {table}"
        if i != len(tables) - 1:
            sql += " UNION ALL BY NAME"
    sql += " );"
    return sql

def context_expr_expand(context_exprs: list[tuple[str, str]], group_by : list[str]) -> str:
    expr = lambda func, col: f'{func}({col}) AS {col}'
    final_expr = ',\n '.join([expr(func, col) for func, col in context_exprs])
    columns = ', '.join([col for _, col in context_exprs])
    group_by_clause = ', '.join(group_by)
    return { 
        'context_expr': final_expr,
        'context_cols': columns,
        'group_by_clause': group_by_clause,
    }
    
## ITER UTILS
def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]