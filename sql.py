SQL_CREATE_DB = "CREATE DATABASE \"%(db_name)s\""
SQL_CREATE_SCHEMA = "CREATE SCHEMA %(schema_name)s"
SQL_GET_ALL_DB = "select schema_name from information_schema.schemata;"
SQL_CREATE_TABLE = "CREATE TABLE %(table_name)s (%(columns)s);"
SQL_GET_TABLE_INFO = "SELECT table_name, column_name, data_type FROM information_schema.columns " \
                     "WHERE table_name = %(table_name)s;"
SQL_GET_PRI_KEY = "SELECT c.column_name " \
                  "FROM information_schema.table_constraints tc " \
                  "JOIN information_schema.constraint_column_usage AS ccu USING (constraint_schema, constraint_name) " \
                  "JOIN information_schema.columns AS c ON c.table_schema = tc.constraint_schema " \
                  "AND tc.table_name = c.table_name AND ccu.column_name = c.column_name " \
                  "WHERE constraint_type = 'PRIMARY KEY' and tc.table_name = %(table_name)s;"
SQL_DELETE_TABLE = "DROP TABLE %(table_name)s;"
SQL_INSERT_QUERY = "INSERT INTO queries (id, query, table_name, db_name) VALUES (%s, %s, %s, %s)"
SQL_DELETE_QUERY_BY_ID = "DELETE FROM queries WHERE id = %(query_id)s"
SQL_GET_QUERY_BY_ID = "SELECT * FROM queries WHERE id = %(query_id)s"
SQL_GET_QUERY_BY_TABLE = "SELECT * FROM queries WHERE table_name = %(table_name)s"
SQL_GET_TABLE_DATA = "SELECT * FROM %(table_name)s"
SQL_INSERT_TABLE_DATA = """INSERT INTO {table_name} values {data}"""
SQL_DELETE_DATA = """DELETE FROM {table_name};"""
SQL_GET_ALL_USERS = "SELECT datname FROM pg_database;"
SQL_GET_ALL_TABLES = "SELECT table_name " \
                     "FROM information_schema.tables " \
                     "WHERE table_schema = %(db_name)s;"
