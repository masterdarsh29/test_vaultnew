from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType
import psycopg2
from pyspark.conf import SparkConf
import time
import pandas as pd

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
    StructField("Company_id", StringType(), True),
    StructField("total_profit", FloatType(), True),
    StructField("net_profit_margin", FloatType(), True),
    StructField("profit_category", StringType(), True)
])

# PostgreSQL connection settings
postgres_host = '192.168.1.223'
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
            INSERT INTO pharmac_sink (Year, Sales_plus, Expenses_plus, Operating_Profit, OPM_Percent,
                Other_Income_plus, Interest, Depreciation, Profit_before_tax, Tax_Percent, Net_Profit_plus, EPS_in_Rs,
                Dividend_Payout_Percent, Company_id, total_profit, net_profit_margin, profit_category)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cur.execute(insert_query, (
                data['Year'], data['Sales_plus'], data['Expenses_plus'], data['Operating_Profit'],
                data['OPM_percent'], data['Other_Income_plus'], data['Interest'], data['Depreciation'],
                data['Profit_before_tax'], data['Tax_percent'], data['Net_Profit_plus'],
                round(data['EPS_in_Rs'], 2), data['Dividend_Payout_percent'], data['Company_id'],
                data['total_profit'], round(data['net_profit_margin'], 2), data['profit_category']
            ))
            conn.commit()

        elif operation == 'UPDATE':
            update_query = """
            UPDATE pharmac_sink
            SET Sales_plus = %s, Expenses_plus = %s, Operating_Profit = %s, OPM_Percent = %s,
                Other_Income_plus = %s, Interest = %s, Depreciation = %s, Profit_before_tax = %s,
                Tax_Percent = %s, Net_Profit_plus = %s, EPS_in_Rs = %s, Dividend_Payout_Percent = %s,
                total_profit = %s, net_profit_margin = %s, profit_category = %s
            WHERE Year = %s AND Company_id = %s
            """
            cur.execute(update_query, (
                data['Sales_plus'], data['Expenses_plus'], data['Operating_Profit'], data['OPM_percent'],
                data['Other_Income_plus'], data['Interest'], data['Depreciation'], data['Profit_before_tax'],
                data['Tax_percent'], data['Net_Profit_plus'], round(data['EPS_in_Rs'], 2), data['Dividend_Payout_percent'],
                data['total_profit'], round(data['net_profit_margin'], 2), data['profit_category'],
                data['Year'], data['Company_id']
            ))
            conn.commit()

        elif operation == 'DELETE':
            delete_query = "DELETE FROM pharmac_sink WHERE Year = %s AND Company_id = %s"
            cur.execute(delete_query, (data['Year'], data['Company_id']))
            conn.commit()

    except Exception as e:
        print(f"PostgreSQL Operation Error: {str(e)}")
    finally:
        cur.close()
        conn.close()
# Function to process Kafka data
def process_kafka_data():  
    try:  
        df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka1:29092") \
            .option("subscribe", "MyTask16") \
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
            year = row["Year"]  

            # Skip rows where Year is 'TTM'
            if year == 'TTM':
                print("Skipping row with Year 'TTM'")
                continue

            # Build data dictionary, considering every operation needs valid parameters  
            data = {  
                'Year': year,  
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
                'Dividend_Payout_percent': row["Dividend_Payout_percent"],  
                'Company_id': row["Company_id"],
                'total_profit': row["total_profit"],
                'net_profit_margin': row["net_profit_margin"],
                'profit_category': row["profit_category"]
            } 

            # Perform transformations
            data['total_profit'] = data['Operating_Profit'] + data['Other_Income_plus']
            data['net_profit_margin'] = round((data['Net_Profit_plus'] / data['Sales_plus']) * 100, 2)
            data['EPS_in_Rs'] = round(data['EPS_in_Rs'], 2)
            
            
            # Create a new column 'profit_category' based on 'net_profit' values
            if data['Net_Profit_plus'] < 1000:
                data['profit_category'] = 'Low'
            elif 1000 < data['Net_Profit_plus'] < 5000:
                data['profit_category'] = 'Medium'
            else:
                data['profit_category'] = 'High'

            print(f"Data for {operation}: {data}")  # Additional debug logging for operations  
            
            if operation == 'INSERT':  
                perform_crud_operation('INSERT', data)  
            elif operation == 'UPDATE':  
                perform_crud_operation('UPDATE', data)  
            elif operation == 'DELETE':  
                perform_crud_operation('DELETE', data)  

    except Exception as e:  
        print(f"Data Processing Errorc: {str(e)}")
   
 
# Run the data processing function periodically
while True:
    process_kafka_data()
    time.sleep(10)  # Pause before the next iteration