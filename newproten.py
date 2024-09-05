from confluent_kafka import Producer
import json
import psycopg2
import datetime
import time
from dateutil import parser 

db_params = {
    'host': 'localhost',
    'port': '5432',
    'dbname': 'MyTask',
    'user': 'Darshan',
    'password': 'Darshan123'
}

conf = {
    'bootstrap.servers': 'localhost:9092'
}

kafka_topic = 'MyTask16'

timestamp_file = 'last_processed_timestamp.txt'

def read_last_timestamp():
    try:
        with open(timestamp_file, 'r') as f:
            timestamp_str = f.read().strip()
            print(f"Read timestamp: '{timestamp_str}'")  
            if timestamp_str == '0001-01-01T00:00:00':
                print("Detected invalid timestamp. Using minimum valid datetime.")
                return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
            if timestamp_str:
                try:
                    return parser.parse(timestamp_str).astimezone(datetime.timezone.utc)
                except ValueError as ve:
                    print(f"Error parsing timestamp: {ve}")
                    return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
            else:
                print("Timestamp file is empty, using minimum datetime.")
                return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)
    except FileNotFoundError:
        print("Timestamp file not found, using minimum datetime.")
        return datetime.datetime.min.replace(tzinfo=datetime.timezone.utc)

def update_last_timestamp(timestamp):
    with open(timestamp_file, 'w') as f:
        f.write(timestamp.isoformat())

class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)

try:
    conn = psycopg2.connect(**db_params)
    cursor = conn.cursor()        
    print("PostgreSQL connection successful.")
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    exit(1)

producer = Producer(conf)
print("Kafka producer connection successful.")

last_timestamp = read_last_timestamp()

try:
    while True:
        try:
            cursor.execute("""
                SELECT * 
                FROM pharmac_profit_loss_data_audit
                WHERE changed_at > %s 
                ORDER BY changed_at ASC
            """, (last_timestamp,))
            rows = cursor.fetchall()
            for row in rows:
                changed_at = row[2].astimezone(datetime.timezone.utc) if row[2] else None
                message = {
                    'operation_type': row[1],
                    'changed_at': changed_at,  
                    'Year': row[4],
                    'Sales_plus': row[5],
                    'Expenses_plus': row[6],
                    'Operating_Profit': row[7],
                    'OPM_percent': row[8],
                    'Other_Income_plus': row[9],
                    'Interest': row[10],
                    'Depreciation': row[11],
                    'Profit_before_tax': row[12],
                    'Tax_percent': row[13],
                    'Net_Profit_plus': row[14],
                    'EPS_in_Rs': row[15],
                    'Dividend_Payout_percent': row[16],
                    'Company_id': row[17]
                    
                }
                producer.produce(kafka_topic, key=str(row[0]), value=json.dumps(message, cls=DateTimeEncoder).encode('utf-8'))
                print(f"Produced message: {message}")
                if changed_at and changed_at > last_timestamp:
                    last_timestamp = changed_at  
            producer.flush()
            print("All messages sent successfully.")
            update_last_timestamp(last_timestamp)
        except Exception as e:
            print(f"Error producing message: {e}")
        time.sleep(10)
finally:
    cursor.close()
    conn.close()