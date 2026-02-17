import csv
import random
import os
from datetime import datetime, timedelta

# Configuration
NUM_CUSTOMERS = 100000
NUM_EMPLOYEES = 1000
NUM_DOCUMENTS = 50000
OUTPUT_DIR = "../data"

def generate_customers():
    print(f"Generating {NUM_CUSTOMERS} customers...")
    filepath = os.path.join(OUTPUT_DIR, "customer/profiles.csv")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(["customer_id", "email", "first_name", "last_name", "segment", "last_login", "signup_date", "account_status", "account_end_date"])
        
        segments = ["STANDARD", "PREMIUM", "VIP", "ENTERPRISE"]
        statuses = ["ACTIVE", "INACTIVE", "SUSPENDED"]
        
        for i in range(1, NUM_CUSTOMERS + 1):
            cid = f"CUST_{i}"
            email = f"user_{i}@example.com"
            # Random fake PII
            fname = f"First{i}"
            lname = f"Last{i}"
            segment = random.choice(segments)
            status = random.choice(statuses)
            
            signup_date = datetime(2025, 1, 1) + timedelta(days=random.randint(0, 365))
            last_login = signup_date + timedelta(days=random.randint(0, 30))
            
            end_date = ""
            if status == "INACTIVE":
                end_date = (last_login + timedelta(days=5)).strftime("%Y-%m-%d")
            
            writer.writerow([
                cid, email, fname, lname, segment, 
                last_login.strftime("%Y-%m-%d %H:%M:%S"), 
                signup_date.strftime("%Y-%m-%d"), 
                status, end_date
            ])

def generate_employees():
    print(f"Generating {NUM_EMPLOYEES} employees...")
    filepath = os.path.join(OUTPUT_DIR, "human_resources/employees.csv")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)
    
    with open(filepath, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(["employee_id", "name", "department", "role", "hire_date", "salary_band"])
        
        depts = ["Engineering", "Marketing", "HR", "Sales", "Finance"]
        roles = ["Junior", "Senior", "Lead", "Manager", "Director"]
        
        for i in range(1, NUM_EMPLOYEES + 1):
            eid = f"EMP_{i:03d}"
            name = f"Employee {i}"
            dept = random.choice(depts)
            role = random.choice(roles)
            band = random.randint(1, 10)
            hire_date = datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1500))
            
            writer.writerow([eid, name, dept, role, hire_date.strftime("%Y-%m-%d"), band])

def generate_documents():
    print(f"Generating {NUM_DOCUMENTS} documents and embeddings...")
    doc_path = os.path.join(OUTPUT_DIR, "digital/documents.csv")
    emb_path = os.path.join(OUTPUT_DIR, "digital/embeddings.csv")
    os.makedirs(os.path.dirname(doc_path), exist_ok=True)
    
    with open(doc_path, 'w') as f_doc, open(emb_path, 'w') as f_emb:
        w_doc = csv.writer(f_doc)
        w_emb = csv.writer(f_emb)
        
        w_doc.writerow(["document_id", "content", "source_url", "author_email", "created_at", "updated_at"])
        w_emb.writerow(["embedding_id", "document_id", "embedding_vector", "model_name", "created_at"])
        
        for i in range(1, NUM_DOCUMENTS + 1):
            # Document
            doc_id = str(i)
            # Generating a reasonably long fake content string
            content = f"This is the content for document {i}. It talks about Verity features and performance. " * 5
            url = f"https://verity.dev/docs/{i}"
            # Randomly link to a customer email to create a join possibility, or internal
            if random.random() > 0.5:
                author_email = f"user_{random.randint(1, NUM_CUSTOMERS)}@example.com"
            else:
                author_email = "internal@verity.dev"
                
            created_at = datetime(2026, 1, 1) + timedelta(minutes=i)
            
            w_doc.writerow([doc_id, content, url, author_email, created_at, created_at])
            
            # Embedding
            vector = f"[{random.random():.4f}, {random.random():.4f}, {random.random():.4f}]"
            w_emb.writerow([i, doc_id, vector, "text-embedding-3-small", created_at])

if __name__ == "__main__":
    generate_customers()
    generate_employees()
    generate_documents()
    print("Done!")
