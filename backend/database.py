import sqlite3
import json
import datetime

DB_NAME = "leads_v2.db"

def init_db():
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            age INTEGER,
            gender TEXT,
            email TEXT,
            phone TEXT,
            city TEXT,
            zip_code TEXT,
            wants_assessment BOOLEAN,
            score INTEGER,
            category TEXT,
            analysis_json TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

def save_lead(lead):
    conn = sqlite3.connect(DB_NAME)
    c = conn.cursor()
    
    # Extract score and category from analysis_data if present, else default
    score = lead.analysis_data.get('suitability_score', 0)
    # Handle nested market_categorization
    market_data = lead.analysis_data.get('market_categorization', {})
    if isinstance(market_data, dict):
        category = market_data.get('primary', 'Unknown')
    else:
        category = str(market_data)

    
    c.execute('''
        INSERT INTO leads (first_name, last_name, age, gender, email, phone, city, zip_code, wants_assessment, score, category, analysis_json)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        lead.first_name, 
        lead.last_name,
        lead.age,
        lead.gender,
        lead.email, 
        lead.phone, 
        lead.city,
        lead.zip_code,
        lead.wants_assessment, 
        score, 
        category, 
        json.dumps(lead.analysis_data)
    ))
    
    lead_id = c.lastrowid
    conn.commit()
    conn.close()
    return lead_id
