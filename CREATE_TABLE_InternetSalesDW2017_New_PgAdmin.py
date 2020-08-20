import psycopg2
from psycopg2 import Error

try:
    connection = psycopg2.connect(user="postgres",
                                  password="postgres",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="newproject1")
    cursor = connection.cursor()

    create_table_query = '''
                            CREATE TABLE public.InternetSalesDW2017_New
                            (
                                id integer NOT NULL DEFAULT nextval('internetsalesdw2017_id_seq'::regclass),
                                categoryname character varying(50) COLLATE pg_catalog."default",
                                subcategoryname character varying(50) COLLATE pg_catalog."default",
                                productname character varying(50) COLLATE pg_catalog."default",
                                orderdate date,
                                duedate date,
                                shipdate date,
                                unitprice numeric(8,2),
                                discountamount numeric(8,2),
                                totalproductcost numeric(8,2),
                                productstandardcost numeric(8,2),
                                salesamount numeric(8,2),
                                taxamt numeric(8,2),
                                freight numeric(8,2),
                                CONSTRAINT InternetSalesDW2017_New_pkey PRIMARY KEY (id)
                            )
                            WITH (
                                OIDS = FALSE
                            )
                            TABLESPACE pg_default;
                            
                            ALTER TABLE public.InternetSalesDW2017_New
                                OWNER to postgres; '''

    cursor.execute(create_table_query)
    connection.commit()
    print("Table created successfully in PostgreSQL ")

except (Exception, psycopg2.Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
# closing database connection.
        if (connection):
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")