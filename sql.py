SQL_GET_ALL_USERS = "SELECT name FROM user_info.users;"
SQL_CREATE_DB = "CREATE DATABASE %(db_name)s"
SQL_CREATE_SCHEMA = "CREATE SCHEMA %(schema_name)s"
SQL_CREATE_USER = "INSERT INTO user_info.users (id, name) VALUES (%s, %s);"
SQL_CREATE_TABLE_IN_DB = "CREATE %(db_name)s.TABLE %(table_name)s (%(columns)s);"
SQL_GET_ALL_DB = "select schema_name from information_schema.schemata;"
SQL_CREATE_TABLE = "CREATE TABLE %(table_name)s (%(columns)s);"


SQL_SAVE_DB = "INSERT INTO created_db (id, name, user_id) VALUES (%s, %s, %s);"
SQL_GET_USER_BY_ID = "SELECT name FROM users WHERE id = %(user_id)s;"
# SQL_GET_ALL_DB = "SELECT name FROM created_db WHERE user_id = %(user_id)s;"
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
SQL_GET_TABLE_QUERIES = "SELECT * FROM queries WHERE table_name IS NOT NULL"
SQL_GET_SINGLE_QUERIES = "SELECT * FROM queries WHERE table_name IS NULL"
