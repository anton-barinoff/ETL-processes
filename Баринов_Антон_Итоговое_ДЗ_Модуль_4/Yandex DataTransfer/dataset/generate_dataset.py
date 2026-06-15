import csv
import random
from datetime import datetime, timedelta



OUTPUT_FILE = 'transactions_v2.csv'
TARGET_SIZE_MB = 35
ESTIMATED_ROW_BYTES = 100
NUM_ROWS = (TARGET_SIZE_MB * 1024 * 1024) // ESTIMATED_ROW_BYTES

# Справочники
REGIONS = ['DE-HE', 'DE-NW', 'DE-RP', 'DE-BE', 'DE-BB', 'DE-SN', 'DE-HH', 'DE-HB', 'DE-BY', 'DE-BW']
CAMPAIGNS = ['credit_card_offer', 'loan_offer', 'insurance_offer', 'savings_plan']
CALL_STATUSES = ['answered', 'no_answer', 'busy', 'voicemail']
CLIENT_RESPONSES = ['interested', 'not_interested', 'callback_later', '']
FOLLOW_UP = ['true', 'false']


print(f"Generating {NUM_ROWS:,} rows...")

with open(OUTPUT_FILE, mode='w', newline='') as f:
    writer = csv.writer(f)
    
    writer.writerow([
        'call_id', 'call_time', 'client_id', 'region_code',
        'campaign_type', 'call_status', 'client_response',
        'duration_sec', 'follow_up_required'
    ])
    
    base_date = datetime(2026, 3, 1)
    
    for i in range(1, NUM_ROWS + 1):
        call_id = f"call_20260301_{i:07d}"
        
        random_days = random.randint(0, 90)
        random_seconds = random.randint(0, 86399)
        call_time = base_date + timedelta(days=random_days, seconds=random_seconds)
        
        client_id = f"client_{random.randint(1000, 99999)}"
        region = random.choice(REGIONS)
        campaign = random.choice(CAMPAIGNS)
        status = random.choice(CALL_STATUSES)
        
        if status == "answered":
            response = random.choice(CLIENT_RESPONSES)
            duration = random.randint(10, 600)
        else:
            response = ""
            duration = 0
        
        # Логика follow_up_required
        if response == "callback_later":
            follow_up = "true"
        elif response == "not_interested":
            follow_up = "false"
        else:
            follow_up = random.choice(["true", "false"])
        
        call_time_str = call_time.strftime('%Y-%m-%dT%H:%M:%S')
        
        writer.writerow([
            call_id,
            call_time_str,
            client_id,
            region,
            campaign,
            status,
            response,
            duration,
            follow_up
        ])
        
        if i % 100000 == 0:
            print(f"  Generated {i:,} rows...")

print(f"Operation completed successfuly.\n  File name: {OUTPUT_FILE}.\n  Total rows: {NUM_ROWS:,}")