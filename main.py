import psycopg2
import uvicorn as uvicorn
from fastapi import FastAPI
from conf import host, user, password, database
from sql import SQL_CREATE_DB, SQL_GET_ALL_DB, SQL_CREATE_TABLE, SQL_GET_TABLE_INFO, \
    SQL_DELETE_TABLE, SQL_INSERT_QUERY, SQL_DELETE_QUERY_BY_ID, SQL_GET_QUERY_BY_ID, \
    SQL_GET_QUERY_BY_TABLE, SQL_CREATE_SCHEMA, SQL_GET_TABLE_COLUMNS_NAME, \
    SQL_GET_PRI_KEY, SQL_GET_TABLE_DATA, SQL_INSERT_TABLE_DATA, \
    SQL_DELETE_DATA, SQL_GET_ALL_USERS, SQL_GET_ALL_TABLES
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import sqlvalidator

app = FastAPI()
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


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
    conn = create_db_connection(host, user, password, database)
    cursor = conn.cursor()
    try:
        app.state.user_name = name
        cursor.execute(SQL_GET_ALL_USERS)
        all_users = cursor.fetchall()
        for i in range(0, len(all_users)):
            all_users[i] = all_users[i][0]
        if name in all_users:
            return JSONResponse(content={"message": "Ok"}, status_code=200)
        cursor.execute(SQL_CREATE_DB % {"db_name": name})
        conn2 = create_db_connection(host, user, password, name)
        cursor2 = conn2.cursor()
        cursor2.execute(SQL_CREATE_TABLE % {"table_name": "queries",
                                            "columns": "id VARCHAR(36) PRIMARY KEY,"
                                                       " query VARCHAR(40) NOT NULL,"
                                                       " table_name VARCHAR(40),"
                                                       " db_name VARCHAR(40)"})

        close_connection(conn2)
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


@app.post('/api/db/create-db')
async def create_db(name: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_CREATE_SCHEMA % {"schema_name": name})
        app.state.db_name = name
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


@app.get('/api/db/get-all-db')
async def get_all_db():
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    db_list = []
    try:
        cursor.execute(SQL_GET_ALL_DB)
        db_list = cursor.fetchall()
        to_delete = ["information_schema", "pg_catalog", "public", "pg_toast"]
        db_list = [x[0] for x in db_list if x[0] not in to_delete]
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return db_list


@app.post('/api/table/create-table')
async def create_table(table_name: str, columns_amount: int, primary_key: str, columns_names: list[str],
                       columns_types: list[str]):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
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
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


@app.get('/api/db/get-all-tables_in_db')
async def get_all_tables_in_db(db_name: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    table_list = []
    try:
        app.state.db_name = db_name
        cursor.execute(SQL_GET_ALL_TABLES, {"db_name": db_name})
        table_list = cursor.fetchall()
        for i in range(0, len(table_list)):
            table_list[i] = table_list[i][0]
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return table_list


@app.get('/api/table/get-table-by-name')
async def get_table_by_name(name: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
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
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return res


@app.delete('/api/table/drop-table-by-name')
async def delete_table_by_name(name: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_TABLE % {'table_name': app.state.db_name + '.' + name})
        conn.commit()
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


@app.post('/api/table-query/add-new-query-to-table')
async def add_query_to_table(query_id: str, table_name: str, query: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    sql_query = sqlvalidator.parse(query)
    if not sql_query.is_valid():
        return JSONResponse(content={"message": sql_query.errors}, status_code=406)
    try:
        cursor.execute(SQL_INSERT_QUERY, (query_id, query, table_name, app.state.db_name))
        conn.commit()
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


@app.put('/api/table-query/modify-query-in-table')
async def modify_query_in_table(query_id: str, table_name: str, query: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_QUERY_BY_ID, {'query_id': query_id})
        cursor.execute(SQL_INSERT_QUERY, (query_id, query, table_name, app.state.db_name))
        conn.commit()
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


@app.delete('/api/table-query/delete-table-query-by-id')
async def delete_query_in_table(id: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    try:
        cursor.execute(SQL_DELETE_QUERY_BY_ID, {'query_id': id})
        conn.commit()
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


@app.get('/api/table-query/execute-table-query-by-id')
async def execute_query_in_table(id: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    res = ""
    try:
        cursor.execute(SQL_GET_QUERY_BY_ID, {'query_id': id})
        (id, query, table_name, db_name) = cursor.fetchone()
        query = str(query).replace(table_name, db_name + "." + table_name)
        cursor.execute(query)
        res = cursor.fetchall()
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return res


@app.get('/api/table-query/get-all-queries-by-table-name')
async def get_queries_by_table(name: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    res = []
    try:
        cursor.execute(SQL_GET_QUERY_BY_TABLE, {'table_name': name})
        res = cursor.fetchall()
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return res


@app.get('/api/table-query/get-table-query-by-id')
async def get_query_by_id(id: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    res = []
    try:
        cursor.execute(SQL_GET_QUERY_BY_ID, {'query_id': id})
        res = cursor.fetchall()
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return res


@app.get('/api/table/get-data')
async def get_table_data(table_name: str):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    res = []
    try:
        cursor.execute(SQL_GET_TABLE_DATA % {'table_name': app.state.db_name + '.' + table_name})
        res = cursor.fetchall()
        cursor.execute(SQL_GET_TABLE_COLUMNS_NAME, {'table_name': table_name})
        column_names = cursor.fetchall()
        for i in range(0, len(column_names)):
            column_names[i] = column_names[i][0]
        res.insert(0, column_names)
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return res


@app.post('/api/table/save-data')
async def save_table_data(table_name: str, data: list):
    try:
        conn = create_db_connection(host, user, password, app.state.user_name)
    except Exception:
        return JSONResponse(content={"message": "Set user"}, status_code=406)
    cursor = conn.cursor()
    try:
        for i in range(len(data)):
            data[i] = tuple(map(lambda x: str(x), data[i]))
        records_list_template = ','.join(['%s'] * len(data))
        cursor.execute(SQL_DELETE_DATA.format(table_name=app.state.db_name + '.' + table_name))
        insert_query = SQL_INSERT_TABLE_DATA.format(table_name=app.state.db_name + '.' + table_name,
                                                    data=records_list_template)
        cursor.execute(insert_query, data)
    except Exception as err:
        return JSONResponse(content={"message": str(err)}, status_code=406)
    finally:
        close_connection(conn)
    return JSONResponse(content={"message": "Ok"}, status_code=200)


if __name__ == '__main__':
    uvicorn.run("main:app", host="0.0.0.0")
