import json
import random
from datetime import datetime, timedelta
import os



def generate_message():
    application_id = f'loan_{random.randint(100000, 999999)}'
    customer_id = f'cust_{random.randint(100, 9999)}'
    regions = ['DE-HE', 'DE-NW', 'DE-RP', 'DE-BE', 'DE-BB', 'DE-SN', 'DE-HH', 'DE-HB', 'DE-BY', 'DE-BW']
    risk_levels = ['low', 'medium', 'high']
    decision_statuses = ['approved', 'rejected', 'manual_review', 'pending']
    doc_types = ['passport', 'id_card', 'driver_license', 'utility_bill']
    doc_statuses = ['verified', 'pending', 'rejected']
    
    num_docs = random.randint(1, 3)
    documents = []
    for _ in range(num_docs):
        documents.append({
            'type': random.choice(doc_types),
            'status': random.choice(doc_statuses)
        })
    
    days_ago = random.randint(1, 30)
    submitted_at = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    return {
        'application_id': application_id,
        'customer': {
            'customer_id': customer_id,
            'region': random.choice(regions)
        },
        'loan': {
            'amount': random.randint(5000, 50000),
            'term_months': random.choice([12, 24, 36, 48, 60])
        },
        'scoring': {
            'score': random.randint(300, 999),
            'risk_level': random.choice(risk_levels)
        },
        'documents': documents,
        'decision_status': random.choice(decision_statuses),
        'submitted_at': submitted_at
    }

def main():
    NUM_MESSAGES = 60000
    OUTPUT_FILE = 'messages.json'
    
    print(f'Generating {NUM_MESSAGES} messages')
    
    messages = [generate_message() for _ in range(NUM_MESSAGES)]
    
    with open(OUTPUT_FILE, 'w') as f:
        for msg in messages:
            f.write(json.dumps(msg) + '\n')
    
    file_size = os.path.getsize(OUTPUT_FILE) / (1024 * 1024)
    print(f'Saved to {OUTPUT_FILE}')
    print(f'File size: {file_size:.2f} MB')
    print(f'Messages: {len(messages)}')
    print('Sample message:')
    print(json.dumps(messages[0], indent=2))

if __name__ == '__main__':
    main()