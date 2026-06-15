import csv
import random
from datetime import datetime, timedelta



OUTPUT_FILE = 'credit_applications.csv'
TARGET_SIZE_MB = 55
ESTIMATED_ROW_BYTES = 120
NUM_ROWS = (TARGET_SIZE_MB * 1024 * 1024) // ESTIMATED_ROW_BYTES

# Справочники
REGIONS = ['DE-HE', 'DE-NW', 'DE-RP', 'DE-BE', 'DE-BB', 'DE-SN', 'DE-HH', 'DE-HB', 'DE-BY', 'DE-BW']
PRODUCT_TYPES = ['cash_loan', 'credit_card', 'mortgage', 'car_loan', 'overdraft']
RISK_LEVELS = ['low', 'medium', 'high', 'critical']
DECISION_STATUSES = ['approved', 'declined', 'manual_review']
CHANNELS = ['mobile', 'web', 'branch', 'call_center']

print(f"Generating {NUM_ROWS:,} rows...")

with open(OUTPUT_FILE, mode='w', newline='') as f:
    writer = csv.writer(f)
    writer.writerow([
        'application_id', 'event_time', 'customer_id', 'region_code',
        'product_type', 'requested_amount', 'term_months', 'credit_score',
        'risk_level', 'decision_status', 'approved_amount', 'channel',
        'employee_review_flag', 'processing_time_sec'
    ])
    
    base_date = datetime(2026, 3, 1)
    
    for i in range(1, NUM_ROWS + 1):
        app_id = f"app_20260501_{i:07d}"
        
        random_days = random.randint(0, 90)
        random_seconds = random.randint(0, 86399)
        event_time = base_date + timedelta(days=random_days, seconds=random_seconds)
        
        cust_id = f"cust_{random.randint(1000, 99999)}"
        region = random.choice(REGIONS)
        product = random.choice(PRODUCT_TYPES)
        requested = random.randint(5000, 500000)
        term = random.choice([6, 12, 24, 36, 48, 60])
        credit_score = random.randint(300, 999)
        risk = random.choice(RISK_LEVELS)
        decision = random.choice(DECISION_STATUSES)
        
        if decision == 'approved':
            approved = requested if random.random() < 0.7 else requested - random.randint(0, requested // 2)
        elif decision == 'declined':
            approved = 0
        else:
            approved = ""
        
        channel = random.choice(CHANNELS)
        review_flag = random.choice(['true', 'false'])
        processing_time = random.randint(1, 300)
        
        writer.writerow([
            app_id,
            event_time.strftime('%Y-%m-%d %H:%M:%S'),
            cust_id,
            region,
            product,
            requested,
            term,
            credit_score,
            risk,
            decision,
            approved,
            channel,
            review_flag,
            processing_time
        ])
        
        if i % 100000 == 0:
            print(f"  Generated {i:,} rows...")

print(f"Operation completed successfuly.\n  File name: {OUTPUT_FILE}.\n  Total rows: {NUM_ROWS:,}")