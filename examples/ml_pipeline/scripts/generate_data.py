import csv
import random
import datetime
import uuid
import os

# Configuration
NUM_USERS = 1000
NUM_ACTIVITIES = 5000
NUM_MODELS = 5
NUM_PREDICTIONS = 2000
DATA_DIR = "data/raw"

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def random_date(start, end):
    return start + datetime.timedelta(
        seconds=random.randint(0, int((end - start).total_seconds())),
    )

def generate_users():
    print("Generating users.csv...")
    users = []
    regions = ['US', 'EU', 'APAC', 'LATAM']
    domains = ['gmail.com', 'yahoo.com', 'outlook.com', 'verity.ai']
    
    with open(f"{DATA_DIR}/users.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['user_id', 'email', 'name', 'signup_date', 'region', 'subscription_tier'])
        
        for i in range(NUM_USERS):
            user_id = f"u_{i:05d}"
            name = f"User_{i}"
            email = f"user_{i}@{random.choice(domains)}"
            signup_date = random_date(datetime.datetime(2023, 1, 1), datetime.datetime(2024, 1, 1)).strftime("%Y-%m-%d")
            region = random.choice(regions)
            tier = random.choice(['free', 'basic', 'premium', 'enterprise'])
            
            users.append(user_id)
            writer.writerow([user_id, email, name, signup_date, region, tier])
    
    return users

def generate_activity(user_ids):
    print("Generating user_activity.csv...")
    activities = ['login', 'view_dashboard', 'export_report', 'api_call', 'update_profile']
    
    with open(f"{DATA_DIR}/user_activity.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['activity_id', 'user_id', 'activity_type', 'timestamp', 'duration_sec'])
        
        for i in range(NUM_ACTIVITIES):
            activity_id = str(uuid.uuid4())
            user_id = random.choice(user_ids)
            activity_type = random.choice(activities)
            timestamp = random_date(datetime.datetime(2023, 6, 1), datetime.datetime(2024, 2, 1)).isoformat()
            duration = random.randint(1, 3600)
            
            writer.writerow([activity_id, user_id, activity_type, timestamp, duration])

def generate_models():
    print("Generating model_metadata.csv...")
    models = []
    algos = ['XGBoost', 'RandomForest', 'LogisticRegression', 'LightGBM']
    
    with open(f"{DATA_DIR}/model_metadata.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['model_id', 'version', 'created_by', 'algorithm', 'hyperparameters', 'created_at'])
        
        for i in range(NUM_MODELS):
            model_id = f"churn_model_v{i+1}"
            version = f"1.{i}.0"
            created_by = f"data_scientist_{random.randint(1,3)}@verity.ai"
            algo = random.choice(algos)
            params = f"{{'learning_rate': {random.uniform(0.01, 0.1):.3f}, 'n_estimators': {random.randint(50, 200)}}}"
            created_at = random_date(datetime.datetime(2023, 9, 1), datetime.datetime(2024, 2, 1)).isoformat()
            
            models.append(model_id)
            writer.writerow([model_id, version, created_by, algo, params, created_at])
            
    return models

def generate_predictions(user_ids, model_ids):
    print("Generating predictions.csv...")
    
    with open(f"{DATA_DIR}/predictions.csv", 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['prediction_id', 'model_id', 'user_id', 'churn_probability', 'prediction_date'])
        
        for i in range(NUM_PREDICTIONS):
            pred_id = str(uuid.uuid4())
            model_id = random.choice(model_ids)
            user_id = random.choice(user_ids)
            prob = random.random()
            date = random_date(datetime.datetime(2024, 1, 1), datetime.datetime(2024, 2, 15)).isoformat()
            
            writer.writerow([pred_id, model_id, user_id, f"{prob:.4f}", date])

if __name__ == "__main__":
    ensure_dir(DATA_DIR)
    users = generate_users()
    generate_activity(users)
    models = generate_models()
    generate_predictions(users, models)
    print("Data generation complete!")
