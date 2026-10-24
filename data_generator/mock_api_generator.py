import json
import time
import random
import uuid
import requests
import os
from faker import Faker
from datetime import datetime

# --- DATABRICKS CONFIGURATION ---
DATABRICKS_INSTANCE = "your_databricks_webpage_link" # Replace if yours is different
DATABRICKS_TOKEN = "you databricks token" 

# IMPORTANT: Change 'main' to 'hive_metastore' or 'workspace' if your catalog is named differently!
VOLUME_PATH = "/Volumes/workspace/default/landing_zone" 

fake = Faker()

def upload_to_volume(local_file_path, file_name):
    """Uploads a local file to a Databricks Unity Catalog Volume via REST API."""
    url = f"{DATABRICKS_INSTANCE}/api/2.0/fs/files{VOLUME_PATH}/{file_name}?overwrite=true"
    
    headers = {
        "Authorization": f"Bearer {DATABRICKS_TOKEN}",
        "Content-Type": "application/octet-stream"
    }
    
    # Read the raw file bytes
    with open(local_file_path, "rb") as f:
        data = f.read()
    
    # Push directly to the volume (no base64 encoding needed for this API)
    # Push directly to the volume
    response = requests.put(url, headers=headers, data=data)
    
    # Change this line:
    if response.ok: 
        print(f"Successfully uploaded {file_name} to Unity Catalog Volume.")
    else:
        print(f"Failed to upload {file_name}: Status {response.status_code} - {response.text}")

def generate_payment_details(status):
    methods = ["credit_card", "paypal", "apple_pay", "google_pay"]
    amount = round(random.uniform(9.99, 99.99), 2)
    
    if status == "failed":
        return {"method": random.choice(methods), "amount": amount, "error_code": random.choice(["NSF", "EXPIRED", "DECLINED"])}
    return {"method": random.choice(methods), "amount": amount, "error_code": None}

def generate_record():
    is_dirty = random.random() < 0.1 

    if is_dirty:
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": fake.name(), 
            "subscription_plan": None, 
            "transaction_status": random.choice(["success", "failed"]),
            "timestamp": datetime.utcnow().isoformat(),
            "payment_details": generate_payment_details(random.choice(["success", "failed"]))
        }
    else:
        status = random.choice(["success", "success", "success", "failed"]) 
        return {
            "event_id": str(uuid.uuid4()),
            "user_id": random.randint(1000, 9999), 
            "subscription_plan": random.choice(["Basic", "Standard", "Premium"]),
            "transaction_status": status,
            "timestamp": datetime.utcnow().isoformat(),
            "payment_details": generate_payment_details(status)
        }

if __name__ == "__main__":
    os.makedirs("landing_zone", exist_ok=True)
    print("Starting continuous data generation and Volume upload... (Press Ctrl+C to stop)")
    batch_count = 1
    
    try:
        while True:
            batch_data = []
            
            for _ in range(100):
                batch_data.append(generate_record())
                time.sleep(0.01) 
            
            file_name = f"events_batch_{batch_count}_{int(time.time())}.json"
            local_path = f"landing_zone/{file_name}"
            
            # Write locally
            with open(local_path, 'w') as f:
                for record in batch_data:
                    f.write(json.dumps(record) + '\n')
                    
            print(f"Generated {local_path} with 100 records.")
            
            # Upload using the new Volume function
            upload_to_volume(local_path, file_name)
            
            batch_count += 1
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nData generation stopped.")