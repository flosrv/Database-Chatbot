import streamlit as st
import fastapi
from fastapi import FastAPI
from pydantic import BaseModel
import json
import sqlite3
import psycopg2
import mysql.connector
import pyodbc
from pymongo import MongoClient
import boto3
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import bigquery
import os

# Template for generating API documentation using Llama
template = (
    "Create a concise and exhaustive documentation for the following API: {api_generated} "
    "This should include clear and concise description of how to interact with it for {language_chosen} developers. "
    "Your response should be the relevant query according to the database. "
    "**No Extra Content:** Do not include any additional text, comments, or explanations in your response. "
    "**Direct Data Only:** Your output should contain only the data that is explicitly requested, with no other text."
)

# Initialize FastAPI app
app = FastAPI()

# Streamlit UI components
st.sidebar.title("API Generator App")
st.sidebar.markdown("""
    This application allows you to generate APIs from your database structure.

    1. **Choose a Database**: Select from PostgreSQL, MySQL, Microsoft SQL Server, SQLite, MongoDB, Amazon DynamoDB, Firestore, and BigQuery.
    2. **Provide Database Connection Details**: Enter the host, port, database name, username, and password. For SQLite, the file path is needed.
    3. **Select API Language**: Choose FastAPI (Python) or .NET (C#).
    4. **Analyze Database**: Press the button to analyze the database and generate the structure.

    You can then download the generated structure or interact with the API.
""")

# Link to access the App
st.sidebar.markdown("[Access the API here](http://localhost:8501)")

# Function to connect to different databases
def connect_to_db(db_choice, host, port, db_name, username, password, db_file, aws_access_key, aws_secret_key, region):
    try:
        if db_choice == "PostgreSQL":
            conn = psycopg2.connect(
                host=host, port=port, dbname=db_name, user=username, password=password
            )
        elif db_choice == "MySQL":
            conn = mysql.connector.connect(
                host=host, port=port, database=db_name, user=username, password=password
            )
        elif db_choice == "Microsoft SQL Server":
            conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={host};PORT={port};DATABASE={db_name};UID={username};PWD={password}"
            )
        elif db_choice == "SQLite":
            conn = sqlite3.connect(db_file)
        elif db_choice == "MongoDB":
            client = MongoClient(f"mongodb://{username}:{password}@{host}:{port}/{db_name}")
            conn = client[db_name]
        elif db_choice == "Amazon DynamoDB":
            session = boto3.Session(
                aws_access_key_id=aws_access_key,
                aws_secret_access_key=aws_secret_key,
                region_name=region
            )
            dynamodb = session.resource('dynamodb')
            conn = dynamodb.Table(db_name)
        elif db_choice == "Firestore":
            cred = credentials.Certificate("path_to_your_service_account_file.json")
            firebase_admin.initialize_app(cred)
            conn = firestore.client()
        elif db_choice == "BigQuery":
            client = bigquery.Client.from_service_account_json("path_to_your_service_account_file.json")
            conn = client

        return conn
    except Exception as e:
        st.error(f"Error connecting to {db_choice}: {str(e)}")
        return None


# Function to analyze database structure and return JSON metadata
def analyze_db_structure(conn, db_choice):
    db_structure = {}

    try:
        if db_choice == "PostgreSQL" or db_choice == "MySQL" or db_choice == "Microsoft SQL Server":
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                columns = cursor.fetchall()
                db_structure[table_name] = {"columns": [col[0] for col in columns]}

        elif db_choice == "SQLite":
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = cursor.fetchall()
            for table in tables:
                table_name = table[0]
                cursor.execute(f"PRAGMA table_info({table_name})")
                columns = cursor.fetchall()
                db_structure[table_name] = {"columns": [col[1] for col in columns]}

        elif db_choice == "MongoDB":
            collections = conn.list_collection_names()
            for col in collections:
                collection = conn[col]
                sample = collection.find_one()
                if sample:
                    db_structure[col] = {"keys": list(sample.keys())}

        elif db_choice == "Amazon DynamoDB":
            db_structure[db_choice] = {"columns": ["Primary Key", "Attributes"]}

        elif db_choice == "Firestore":
            collections = conn.collections()
            for col in collections:
                docs = col.stream()
                for doc in docs:
                    db_structure[col.id] = {"keys": list(doc.to_dict().keys())}

        elif db_choice == "BigQuery":
            query = f"SELECT * FROM `{db_choice}` LIMIT 1"
            result = conn.query(query).result()
            db_structure[db_choice] = {"columns": [field.name for field in result.schema]}

    except Exception as e:
        st.error(f"Error analyzing structure for {db_choice}: {str(e)}")

    return db_structure

# Function to generate API based on the analyzed structure (FastAPI)
def generate_fastapi_api(db_structure):
    endpoints = {}
    try:
        for table, metadata in db_structure.items():
            if "columns" in metadata:
                columns = metadata["columns"]
                class TableQuery(BaseModel):
                    query: str
                endpoint = f"/{table}/{{id}}"
                @app.get(endpoint)
                def get_table_data(id: str):
                    # Example logic for fetching the data based on the id
                    query = f"SELECT * FROM {table} WHERE id = {id}"
                    return {"table": table, "columns": columns, "data": query}
                endpoints[table] = endpoint
            if "keys" in metadata:
                keys = metadata["keys"]
                @app.get(f"/{table}")
                def get_collection_data():
                    # Example logic for fetching all keys from the collection
                    return {"collection": table, "keys": keys}
                endpoints[table] = f"/{table}"

    except Exception as e:
        st.error(f"Error generating API for {db_choice}: {str(e)}")

    return endpoints

# Streamlit UI components
st.title("Database to API Generator")

# Database selection drop-down
db_choice = st.selectbox("Choose a database", 
                         ["None", "PostgreSQL", "MySQL", "Microsoft SQL Server", "SQLite", "MongoDB", 
                          "Amazon DynamoDB", "Firestore", "BigQuery"])

# Collect database connection details
host = st.text_input("Host")
port = st.number_input("Port", min_value=1, max_value=65535, value=5432)
db_name = st.text_input("Database Name")
username = st.text_input("Username")
password = st.text_input("Password", type="password")
db_file = st.text_input("Database File (SQLite only)")

aws_access_key = st.text_input("AWS Access Key (if DynamoDB)")
aws_secret_key = st.text_input("AWS Secret Key (if DynamoDB)")
region = st.text_input("AWS Region (if DynamoDB)")

# Upload button for authentication file (for BigQuery or others)
json_file = st.file_uploader("Upload JSON Authentication File (if required)", type=["json"])

# Dropdown for selecting API language
api_language_choice = st.selectbox("Choose the API language", ['None', 'FastAPI (Python)', '.NET (C#)'])

# Button to test connection and retrieve db metadata
if st.button("Analyze DB Details"):
    conn = connect_to_db(db_choice, host, port, db_name, username, password, db_file, aws_access_key, aws_secret_key, region)

    if conn:
        db_structure = analyze_db_structure(conn, db_choice)
        st.write("Database Structure:", json.dumps(db_structure, indent=4))

        # Save structure to a JSON file for API generation
        try:
            with open("db_structure.json", "w") as json_file:
                json.dump(db_structure, json_file)
            st.success("Database structure saved to 'db_structure.json'")
        except Exception as e:
            st.error(f"Error saving database structure: {str(e)}")


