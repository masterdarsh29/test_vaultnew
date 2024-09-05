
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json, avg
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import psycopg2
from pyspark.conf import SparkConf
import time

# Initialize Spark Context
conf = SparkConf() \
    .set("spark.executor.memory", "2g") \
    .set("spark.driver.memory", "2g")
sc = SparkContext(conf=conf)
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Define the schema of Kafka JSON data
json_schema = StructType([
    StructField("operation_type", StringType(), True),
    StructField("changed_at", TimestampType(), True),
    StructField("Year", StringType(), True),
    StructField("Sales_plus", FloatType(), True),
    StructField("Expenses_plus", FloatType(), True),
    StructField("Operating_Profit", FloatType(), True),
    StructField("OPM_percent", FloatType(), True),
    StructField("Other_Income_plus", FloatType(), True),
    StructField("Interest", FloatType(), True),
    StructField("Depreciation", FloatType(), True),
    StructField("Profit_before_tax", FloatType(), True),
    StructField("Tax_percent", FloatType(), True),
    StructField("Net_Profit_plus", FloatType(), True),
    StructField("EPS_in_Rs", FloatType(), True),
    StructField("Dividend_Payout_percent", FloatType(), True),
])

# PostgreSQL connection settings
postgres_host = '192.168.3.45'
postgres_port = '5432'
postgres_db = 'MyTask'
postgres_user = 'Darshan'
postgres_password = 'Darshan123'

# Function to handle PostgreSQL operations
def perform_crud_operation(operation, data):
    conn = psycopg2.connect(
        host=postgres_host,
        port=postgres_port,
        dbname=postgres_db,
        user=postgres_user,
        password=postgres_password
    )
    cur = conn.cursor()

    try:
        if operation == 'INSERT':
            insert_query = """
            INSERT INTO profit_loss_data_sink (Year, Sales_plus, Expenses_plus, Operating_Profit, OPM_Percent,
                Other_Income_plus, Interest, Depreciation, Profit_before_tax, Tax_Percent, Net_Profit_plus, EPS_in_Rs,
                Dividend_Payout_Percent)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (Year) DO NOTHING
            """
            cur.execute(insert_query, (
                data['Year'], data['Sales_plus'], data['Expenses_plus'], data['Operating_Profit'],
                data['OPM_percent'], data['Other_Income_plus'], data['Interest'], data['Depreciation'],
                data['Profit_before_tax'], data['Tax_percent'], data['Net_Profit_plus'],
                data['EPS_in_Rs'], data['Dividend_Payout_percent']
            ))
            conn.commit()

        elif operation == 'UPDATE':
            update_query = """
            UPDATE profit_loss_data_sink
            SET Sales_plus = %s, Expenses_plus = %s, Operating_Profit = %s, OPM_Percent = %s,
                Other_Income_plus = %s, Interest = %s, Depreciation = %s, Profit_before_tax = %s,
                Tax_Percent = %s, Net_Profit_plus = %s, EPS_in_Rs = %s, Dividend_Payout_Percent = %s
            WHERE Year = %s
            """
            cur.execute(update_query, (
                data['Sales_plus'], data['Expenses_plus'], data['Operating_Profit'], data['OPM_percent'],
                data['Other_Income_plus'], data['Interest'], data['Depreciation'], data['Profit_before_tax'],
                data['Tax_percent'], data['Net_Profit_plus'], data['EPS_in_Rs'], data['Dividend_Payout_percent'],
                data['Year']  # Year should be the last argument
            ))
            conn.commit()

        elif operation == 'DELETE':
            delete_query = "DELETE FROM profit_loss_data_sink WHERE Year = %s"
            cur.execute(delete_query, (data['Year'],))
            conn.commit()

        # Update averages after every insert/update
        compute_and_update_average(cur, conn)

    except Exception as e:
        print(f"PostgreSQL Operation Error: {str(e)}")
    finally:
        cur.close()
        conn.close()

# Function to compute and update average row
def compute_and_update_average(cur, conn):
    try:
        avg_query = """
        SELECT
            ROUND(AVG(Sales_plus)::NUMERIC, 2),
            ROUND(AVG(Expenses_plus)::NUMERIC, 2),
            ROUND(AVG(Operating_Profit)::NUMERIC, 2),
            ROUND(AVG(OPM_percent)::NUMERIC, 2),
            ROUND(AVG(Other_Income_plus)::NUMERIC, 2),
            ROUND(AVG(Interest)::NUMERIC, 2),
            ROUND(AVG(Depreciation)::NUMERIC, 2),
            ROUND(AVG(Profit_before_tax)::NUMERIC, 2),
            ROUND(AVG(Tax_percent)::NUMERIC, 2),
            ROUND(AVG(Net_Profit_plus)::NUMERIC, 2),
            ROUND(AVG(EPS_in_Rs)::NUMERIC, 2),
            ROUND(AVG(Dividend_Payout_percent)::NUMERIC, 2)
        FROM profit_loss_data_sink
        WHERE Year NOT LIKE 'TTM'
        """
    
        cur.execute(avg_query)
        avg_values = cur.fetchone()
 
        # Insert or update average row
        avg_insert_query = """
        INSERT INTO profit_loss_data_sink (Year, Sales_plus, Expenses_plus, Operating_Profit, OPM_Percent,
            Other_Income_plus, Interest, Depreciation, Profit_before_tax, Tax_Percent, Net_Profit_plus, EPS_in_Rs,
            Dividend_Payout_Percent)
        VALUES ('AVERAGE', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (Year)
        DO UPDATE SET
            Sales_plus = EXCLUDED.Sales_plus,
            Expenses_plus = EXCLUDED.Expenses_plus,
            Operating_Profit = EXCLUDED.Operating_Profit,
            OPM_Percent = EXCLUDED.OPM_Percent,
            Other_Income_plus = EXCLUDED.Other_Income_plus,
            Interest = EXCLUDED.Interest,
            Depreciation = EXCLUDED.Depreciation,
            Profit_before_tax = EXCLUDED.Profit_before_tax,
            Tax_Percent = EXCLUDED.Tax_Percent,
            Net_Profit_plus = EXCLUDED.Net_Profit_plus,
            EPS_in_Rs = EXCLUDED.EPS_in_Rs,
            Dividend_Payout_Percent = EXCLUDED.Dividend_Payout_Percent
        """
        cur.execute(avg_insert_query, avg_values)
        conn.commit()
       
    except Exception as e:
        print(f"Average Calculation Error: {str(e)}")

# Function to process Kafka data
def process_kafka_data():  
    try:  
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:29092") \
            .option("subscribe", "MyTask7") \
            .load()  

        df = df.selectExpr("CAST(value AS STRING) as json_value") \
            .select(from_json(col("json_value"), json_schema).alias("data")) \
            .select("data.*")  

        # Print Kafka data for debugging  
        df.show(truncate=False)  # Display full data for debugging  

        # Process data  
        for row in df.collect():  
            print("Processing row:", row.asDict())  # Print each row for debugging  

            operation = row["operation_type"]  
            
            # Build data dictionary, considering every operation needs valid parameters  
            data = {  
                'Year': row["Year"]  
            }  

            # Add only required fields based on operation  
            if operation in ['INSERT', 'UPDATE']:  
                data.update({  
                    'Sales_plus': row["Sales_plus"],  
                    'Expenses_plus': row["Expenses_plus"],  
                    'Operating_Profit': row["Operating_Profit"],  
                    'OPM_percent': row["OPM_percent"],  
                    'Other_Income_plus': row["Other_Income_plus"],  
                    'Interest': row["Interest"],  
                    'Depreciation': row["Depreciation"],  
                    'Profit_before_tax': row["Profit_before_tax"],  
                    'Tax_percent': row["Tax_percent"],  
                    'Net_Profit_plus': row["Net_Profit_plus"],  
                    'EPS_in_Rs': row["EPS_in_Rs"],  
                    'Dividend_Payout_percent': row["Dividend_Payout_percent"]  
                })  

            print(f"Data for {operation}: {data}")  # Additional debug logging for operations  
            
            if operation == 'INSERT':  
                perform_crud_operation('INSERT', data)  
            elif operation == 'UPDATE':  
                perform_crud_operation('UPDATE', data)  
            elif operation == 'DELETE':  
                perform_crud_operation('DELETE', data)  

    except Exception as e:  
        print(f"Data Processing Error: {str(e)}")
   
 
# Run the data processing function periodically
while True:
    process_kafka_data()
    time.sleep(10)  # Pause before the next iteration



# select * from profit_loss_data

# delete from profit_loss_data where year in (' Mar 2025', 'Jun 2025')



# select * from profit_loss_data


# SELECT column_name, data_type
# FROM information_schema.columns
# WHERE table_schema = 'public' AND
# table_name ='profit_loss_data';


# CREATE TABLE profit_loss_audit (
#     audit_id SERIAL PRIMARY KEY,
#     operation_type VARCHAR(10) NOT NULL, -- 'INSERT', 'UPDATE', 'DELETE',  
#     changed_at TIMESTAMPTZ default CURRENT_TIMESTAMP,
#     changed_by TEXT DEFAULT CURRENT_USER,
#     Year TEXT,
#     Sales_plus DOUBLE PRECISION,
#     Expenses_plus DOUBLE PRECISION,
#     Operating_Profit DOUBLE PRECISION,
#     OPM_percent DOUBLE PRECISION,
#     Other_Income_plus DOUBLE PRECISION,
#     Interest DOUBLE PRECISION,
#     Depreciation DOUBLE PRECISION,
#     Profit_before_tax DOUBLE PRECISION,
#     Tax_percent DOUBLE PRECISION,
#     Net_Profit_plus DOUBLE PRECISION,
#     EPS_in_Rs DOUBLE PRECISION,
#     Dividend_Payout_percent DOUBLE PRECISION
# );
# -----------------------------------------------------------------------------------------------



# INSERT INTO profit_loss_audit (
#     operation_type,
#     changed_at,
#     changed_by,
#     Year,
#     Sales_plus,
#     Expenses_plus,
#     Operating_Profit,
#     OPM_percent,
#     Other_Income_plus,
#     Interest,
#     Depreciation,
#     Profit_before_tax,
#     Tax_percent,
#     Net_Profit_plus,
#     EPS_in_Rs,
#     Dividend_Payout_percent
# )
# SELECT
#     'INSERT' AS operation_type,  -- Marking as initial load
#     CURRENT_TIMESTAMP AS changed_at,
#     CURRENT_USER AS changed_by,
#     "year",
#     "Sales",
#     "Expenses",
#     "Operating_Profit",
#     "OPM_%",
#     "Other_Income",
#     "Interest",
#     "Depreciation",
#     "Profit_before_tax",
#     "Tax_%",
#     "Net_Profit",
#     "EPS_in_Rs",
#     "Dividend_Payout_%"
# FROM profit_loss_data;

# ---------------------------------------------------------------------------------------------

# drop table profit_loss_audit 
# CREATE OR REPLACE FUNCTION profit_loss_audit_trigger()
# RETURNS TRIGGER AS $$
# BEGIN
#     IF (TG_OP = 'INSERT') THEN
#         INSERT INTO profit_loss_audit (operation_type, Year, Sales_plus, Expenses_plus, Operating_Profit, OPM_percent, Other_Income_plus, Interest, Depreciation, Profit_before_tax, Tax_percent, Net_Profit_plus, EPS_in_Rs, Dividend_Payout_percent)
#         VALUES ('INSERT', NEW."Year", NEW."Sales+", NEW."Expenses+", NEW."Operating Profit", NEW."OPM %", NEW."Other Income+", NEW."Interest", NEW."Depreciation", NEW."Profit before tax", NEW."Tax %", NEW."Net Profit+", NEW."EPS in Rs", NEW."Dividend Payout %");
#         RETURN NEW;
#     ELSIF (TG_OP = 'UPDATE') THEN
#         INSERT INTO profit_loss_audit (operation_type, Year, Sales_plus, Expenses_plus, Operating_Profit, OPM_percent, Other_Income_plus, Interest, Depreciation, Profit_before_tax, Tax_percent, Net_Profit_plus, EPS_in_Rs, Dividend_Payout_percent)
#         VALUES ('UPDATE', NEW."Year", NEW."Sales+", NEW."Expenses+", NEW."Operating Profit", NEW."OPM %", NEW."Other Income+", NEW."Interest", NEW."Depreciation", NEW."Profit before tax", NEW."Tax %", NEW."Net Profit+", NEW."EPS in Rs", NEW."Dividend Payout %");
#         RETURN NEW;
#     ELSIF (TG_OP = 'DELETE') THEN
#         INSERT INTO profit_loss_audit (operation_type, Year, Sales_plus, Expenses_plus, Operating_Profit, OPM_percent, Other_Income_plus, Interest, Depreciation, Profit_before_tax, Tax_percent, Net_Profit_plus, EPS_in_Rs, Dividend_Payout_percent)
#         VALUES ('DELETE', OLD."Year", OLD."Sales+", OLD."Expenses+", OLD."Operating Profit", OLD."OPM %", OLD."Other Income+", OLD."Interest", OLD."Depreciation", OLD."Profit before tax", OLD."Tax %", OLD."Net Profit+", OLD."EPS in Rs", OLD."Dividend Payout %");
#         RETURN OLD;
#     END IF;
# END;
# $$ LANGUAGE plpgsql;
 
# CREATE TRIGGER profit_loss_audit_trigger
# AFTER INSERT OR UPDATE OR DELETE ON profit_loss_data
# FOR EACH ROW EXECUTE FUNCTION profit_loss_audit_trigger();


# CREATE TABLE profit_loss_data_sink(
#     Year TEXT,
#     Sales_plus DOUBLE PRECISION,
#     Expenses_plus DOUBLE PRECISION,
#     Operating_Profit DOUBLE PRECISION,
#     OPM_Percent DOUBLE PRECISION,
#     Other_Income_plus DOUBLE PRECISION,
#     Interest DOUBLE PRECISION,
#     Depreciation DOUBLE PRECISION,
#     Profit_before_tax DOUBLE PRECISION,
#     Tax_Percent DOUBLE PRECISION,
#     Net_Profit_plus DOUBLE PRECISION,
#     EPS_in_Rs DOUBLE PRECISION,
#     Dividend_Payout_Percent DOUBLE PRECISION
# );
 
# select * from profit_loss_data_sink;
 
