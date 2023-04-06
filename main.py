from uuid import uuid4
import psycopg2
import uvicorn as uvicorn
from fastapi import FastAPI, HTTPException
from conf import host, user, password, database
from sql import SQL_CREATE_DB, SQL_GET_ALL_DB, SQL_CREATE_TABLE, SQL_GET_TABLE_INFO, \
    SQL_DELETE_TABLE, SQL_INSERT_QUERY, SQL_DELETE_QUERY_BY_ID, SQL_GET_QUERY_BY_ID, \
    SQL_GET_QUERY_BY_TABLE, SQL_GET_TABLE_QUERIES, SQL_GET_SINGLE_QUERIES, \
    SQL_CREATE_USER, SQL_GET_USER_BY_ID, SQL_GET_ALL_USERS, SQL_CREATE_TABLE_IN_DB, \
    SQL_CREATE_SCHEMA, SQL_GET_PRI_KEY

app = FastAPI()


def create_db_connection(host_name, user_name, user_password, db_name):
    pgparams = {
        'dbname': db_name,
        'user': user_name,
        'password': user_password,
        'host': host_name
    }
    conn = psycopg2.connect(**pgparams)
    conn.autocommit = True
    return conn


def close_connection(conn):
    conn.close()


@app.post('/api/user/create-user')
async def create_user(name: str):
    conn = create_db_connection(host, user, password, 'user_info')
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_GET_ALL_USERS)
        all_users = [item[0] for item in cursor.fetchall()]
        if name in all_users:
            raise HTTPException(420, "Пользователь с таким именем уже существует")
        current_user_id = str(uuid4())
        app.state.user_name = name
        cursor.execute(SQL_CREATE_USER, (current_user_id, name))

        conn3 = create_db_connection(host, user, password, database)
        cursor3 = conn3.cursor()
        cursor3.execute(SQL_CREATE_DB % {"db_name": name})

        conn2 = create_db_connection(host, user, password, name)
        cursor2 = conn2.cursor()
        cursor2.execute(SQL_CREATE_TABLE % {"table_name": "queries",
                                            "columns": "id VARCHAR(36) PRIMARY KEY,"
                                                       " query VARCHAR(40) NOT NULL,"
                                                       " table_name VARCHAR(40),"
                                                       " db_name VARCHAR(40)"})

        close_connection(conn2)
        close_connection(conn3)

    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.post('/api/user/set-user')
async def set_user(user_name: str):
    app.state.user_name = user_name


@app.post('/api/db/create-db')
async def create_db(name: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_CREATE_SCHEMA % {"schema_name": name})
        app.state.db_name = name
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.post('/api/db/get-all-db')
async def get_all_db():
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    db_list = []
    try:
        cursor.execute(SQL_GET_ALL_DB)
        db_list = cursor.fetchall()
        to_delete = ["information_schema", "pg_catalog", "public", "pg_toast"]
        db_list = [x[0] for x in db_list if x[0] not in to_delete]
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return db_list


@app.post('/api/db/select-db')
async def select_db(db_name: str):
    app.state.db_name = db_name


@app.post('/api/table/create-table')
async def create_table(table_name: str, columns_amount: int, primary_key: str, columns_names: list[str],
                       columns_types: list[str]):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        columns = ""
        for i in range(columns_amount):
            if columns_names[i] == primary_key:
                columns += f"{columns_names[i]} {columns_types[i]} PRIMARY KEY,"
            else:
                columns += f"{columns_names[i]} {columns_types[i]},"
            if i == columns_amount - 1:
                columns = columns[:-1]
        a = SQL_CREATE_TABLE % {'table_name': app.state.db_name + "." + table_name,
                                'columns': columns}
        cursor.execute(a)
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.get('/api/table/get-table-by-name')
async def get_table_by_name(name: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    res = {}
    try:
        cursor.execute(SQL_GET_TABLE_INFO, {'table_name': name})
        info = cursor.fetchall()
        res["table_name"] = name
        res["columns_amount"] = len(info)
        columns_infos = []
        for column in info:
            columns_infos.append([column[1], column[2]])
        res["column_infos"] = columns_infos
        cursor.execute(SQL_GET_PRI_KEY, {'table_name': name})
        res["primary_key"] = cursor.fetchone()[0]
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return res


@app.delete('/api/table/drop-table-by-name')
async def delete_table_by_name(name: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_TABLE % {'table_name': app.state.db_name + '.' + name})
        conn.commit()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.post('/api/table-query/add-new-query-to-table')
async def add_query_to_table(query_id: str, table_name: str, query: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_INSERT_QUERY, (query_id, query, table_name, app.state.db_name))
        conn.commit()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.put('/api/table-query/modify-query-in-table')
async def modify_query_in_table(query_id: str, table_name: str, query: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_QUERY_BY_ID, {'query_id': query_id})
        cursor.execute(SQL_INSERT_QUERY, (query_id, query, table_name, app.state.db_name))
        conn.commit()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.delete('/api/table-query/delete-table-query-by-id')
async def delete_query_in_table(id: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_QUERY_BY_ID, {'query_id': id})
        conn.commit()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.get('/api/table-query/execute-table-query-by-id')
async def execute_query_in_table(id: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    res = ""
    try:
        cursor.execute(SQL_GET_QUERY_BY_ID, {'query_id': id})
        (id, query, table_name, db_name) = cursor.fetchone()
        query = str(query).replace(table_name, db_name + "." + table_name)
        cursor.execute(query)
        res = cursor.fetchall()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return res


@app.get('/api/table-query/get-all-queries-by-table-name')
async def get_queries_by_table(name: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    res = []
    try:
        cursor.execute(SQL_GET_QUERY_BY_TABLE, {'table_name': name})
        res = cursor.fetchall()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return res


@app.get('/api/table-query/get-table-query-by-id')
async def get_query_by_id(id: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    res = []
    try:
        cursor.execute(SQL_GET_QUERY_BY_ID, {'query_id': id})
        res = cursor.fetchall()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return res


@app.get('/api/table-query/get-all-table-queries')
async def get_table_queries():
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    res = []
    try:
        cursor.execute(SQL_GET_TABLE_QUERIES)
        res = cursor.fetchall()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return res


@app.post('/api/single-query/add-new-query')
async def add_query(query_id: str, query: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_INSERT_QUERY, (query_id, query, None, app.state.db_name))
        conn.commit()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.put('api/single-query/modify-single-query')
async def modify_query(query_id: str, query: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_QUERY_BY_ID, {'query_id': query_id})
        cursor.execute(SQL_INSERT_QUERY, (query_id, query, None, app.state.db_name))
        conn.commit()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.delete('/api/single-query/delete-single-query-by-id')
async def delete_query(id: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_QUERY_BY_ID, {'query_id': id})
        conn.commit()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)


@app.get('/api/single-query/execute-single-query-by-id')
async def execute_query(id: str):
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    res = ""
    try:
        cursor.execute(SQL_GET_QUERY_BY_ID, {'query_id': id})
        (id, query, table_name, db_name) = cursor.fetchone()
        cursor.execute(query)
        res = cursor.fetchall()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return res


@app.get('/api/single-query/get-all-single-queries')
async def get_single_queries():
    conn = create_db_connection(host, user, password, app.state.user_name)
    cursor = conn.cursor()
    res = []
    try:
        cursor.execute(SQL_GET_SINGLE_QUERIES)
        res = cursor.fetchall()
    except Exception as err:
        print(f"Error: '{err}'")
    close_connection(conn)
    return res


if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0")
