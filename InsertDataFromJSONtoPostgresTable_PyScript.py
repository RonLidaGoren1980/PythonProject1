import json, sys
import psycopg2
from psycopg2 import connect, Error
import requests
from psycopg2.extras import Json

try:
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="newproject1")

    cursor = connection.cursor()
    # Print PostgreSQL Connection properties
    print(connection.get_dsn_parameters(), "\n")

    # Print PostgreSQL version
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record, "\n")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)

url = 'https://raw.githubusercontent.com/RonLidaGoren1980/PythonProject1/master/AWInternetsalesAnalysis.json'
resp = requests.get(url)
data = json.loads(resp.text)
# record_list = json.dumps(data, indent=2)
# print(record_list)

# if type(data) == list:
#     first_record = data[0]
#     columns = list(first_record.keys())
#     # print("\ncolumn names:", columns)
columns = [list(x.keys()) for x in data][0]
#     # print(columns)
table_name = "internetsalesdw2017_new"
sql_string = 'INSERT INTO {} '.format(table_name)
sql_string += "(" + ', '.join(columns) + ")\nVALUES "
# enumerate over the record
for i, data_dict in enumerate(data):
    values = [list(x.values()) for x in data]
# value string for the SQL string
values_str = ""
# print(values)


# enumerate over the records' values
for i, record in enumerate(values):

    # declare empty list for values
    val_list = []

    # append each value to a new list of values
    for v, val in enumerate(record):
        if type(val) == str:
            val = str(Json(val)).replace('"', '')
        val_list += [str(val)]

    # put parenthesis around each record string
    values_str += "(" + ', '.join(val_list) + "),\n"

# remove the last comma and end SQL with a semicolon
values_str = values_str[:-2] + ";"

# concatenate the SQL string
table_name = "internetsalesdw2017_new"
sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
    table_name,
    ', '.join(columns),
    values_str
)
# print("\nSQL statement:")
# print(sql_string)
if cursor != None:

    try:
        cursor.execute(sql_string)
        connection.commit()

        print('\nfinished INSERT INTO execution')

    except (Exception, Error) as error:
        print("\nexecute_sql() error:", error)
        connection.rollback()

    # close the cursor and connection
    cursor.close()
    connection.close()
