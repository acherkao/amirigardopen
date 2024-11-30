from fastapi import FastAPI, HTTPException
import openai
import psycopg2
from psycopg2 import sql
from typing import Dict
import os
from dotenv import load_dotenv

# Initialize FastAPI app
app = FastAPI()

load_dotenv()  # Load variables from .env

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

if not OPENAI_API_KEY:
    raise Exception("OpenAI API key is not set. Check your .env file or environment variables.")

# CockroachDB connection details
COCKROACHDB_URL = "postgresql://acherkao:e6eTMCnivujYPGJ4S061YQ@raging-crow-5419.j77.aws-ap-south-1.cockroachlabs.cloud:26257/AmiriPoC?sslmode=verify-full"


# Initialize OpenAI API
openai.api_key = OPENAI_API_KEY

# Initialize conversation history (in-memory storage for simplicity)
conversation_history = {}

def detect_language(query):
    """Detect if the query is in Arabic or English."""
    arabic_chars = set("ابتثجحخدذرزسشصضطظعغفقكلمنهوي")
    if any(char in arabic_chars for char in query):
        return "arabic"
    return "english"

def process_openai_response(natural_language_query, conversation):
    """Process OpenAI response with context."""
    prompt = f"""
    You are an intelligent assistant capable of processing both database-related queries and general questions.

    - If the query is related to the database schema below, generate an SQL query that answers the question. 
    Return only the SQL query without any additional text or explanation.
    Ensure that all column names, table names, and string values in the SQL query are in English, even if the input query is in Arabic.
    - Important: Use `current_date` (without parentheses) for date-related queries instead of `CURDATE()` or `NOW()`, as the database is CockroachDB.

    - If the query is NOT related to the database schema, provide a direct and concise natural language response to the query without any additional context, background information, or elaboration.

    Database Schema:
    1. Employees(EmployeeID, FirstName, LastName, Rank, Department, Position, ContactNumber, Email, DateOfJoining, Salary, LeaveBalance, EmergencyContact, CurrentTask)
    2. EmployeeAddresses(AddressID, EmployeeID, AddressLine1, AddressLine2, City, State, PostalCode, Country)
    3. EmployeeTasks(TaskID, EmployeeID, TaskDescription, AssignedDate, DueDate, Status)
    4. EmployeeTrainings(TrainingID, EmployeeID, TrainingName, CompletionDate, CertificationIssued)
    
    Ensure that:
    - All column names match the schema exactly.
    - Only valid SQL syntax for CockroachDB is used.
    - The query ends with a semicolon.
    - The SQL query uses English column and table names even if the question is in Arabic.
    - The result should join tables if needed to show meaningful information.

    Return only the SQL query without any additional text or explanations.

    Query: "{natural_language_query}"
    """
    messages = [
        {"role": "system", "content": prompt},
    ] + conversation + [
        {"role": "user", "content": natural_language_query}
    ]

    response = openai.chat.completions.create(
        model="gpt-4",  # or "gpt-4"
        messages=messages,
        max_tokens=120
    )

    sql_query = response.choices[0].message.content
    
    # Remove SQL markdown (```sql) if present
    if sql_query.startswith("```sql") and sql_query.endswith("```"):
        sql_query = sql_query[6:-3].strip()
    
    return sql_query

def generate_follow_up_sql(last_query_metadata, follow_up_question):
    """
    Generate a follow-up SQL query based on the result of the previous query.
    """
    last_result = last_query_metadata["result"]
    last_sql = last_query_metadata["query"]

    # Use OpenAI to determine how to adapt the follow-up
    prompt = f"""
    The user previously asked a question that resulted in this SQL query: "{last_sql}" 
    and this result: {last_result}.
    
    Now they have asked: "{follow_up_question}"

    The input query may be in Arabic or English, but the SQL query must always:
    - Use English table and column names as specified in the schema.
    - Be valid for CockroachDB.

    If the follow-up relates to employees, ensure the query shows meaningful results like FirstName and LastName. 
    Join tables if necessary to provide complete information.

    Generate an adapted SQL query based on the following database schema:
    1. Employees(EmployeeID, FirstName, LastName, Rank, Department, Position, ContactNumber, Email, DateOfJoining, Salary, LeaveBalance, EmergencyContact, CurrentTask)
    2. EmployeeAddresses(AddressID, EmployeeID, AddressLine1, AddressLine2, City, State, PostalCode, Country)
    3. EmployeeTasks(TaskID, EmployeeID, TaskDescription, AssignedDate, DueDate, Status)
    4. EmployeeTrainings(TrainingID, EmployeeID, TrainingName, CompletionDate, CertificationIssued)

    Ensure that:
    - All column names match the schema exactly.
    - Only valid SQL syntax for CockroachDB is used.
    - The query ends with a semicolon.
    - The SQL query uses English column and table names even if the question is in Arabic.
    - The result should join tables if needed to show meaningful information.

    Return only the SQL query without any additional text or explanations.
    """
    response = openai.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": "You are an SQL expert and intelligent assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=150
    )
    sql_query = response.choices[0].message.content.strip()

    # Ensure the response contains only SQL
    if sql_query.startswith("```sql"):
        sql_query = sql_query[6:].strip()  # Remove markdown syntax
    if sql_query.endswith("```"):
        sql_query = sql_query[:-3].strip()

    # Validate that the query is syntactically correct
    sql_query = validate_sql_query(sql_query)

    # Ensure the query ends with a semicolon
    sql_query = ensure_semicolon(sql_query)
    return sql_query

def validate_sql_query(sql_query):
    """
    Validate that the response is a valid SQL query.
    """
    sql_keywords = ["SELECT", "INSERT", "UPDATE", "DELETE", "WHERE", "FROM", "JOIN", "GROUP BY", "ORDER BY"]
    if not any(keyword in sql_query.upper() for keyword in sql_keywords):
        raise ValueError("Generated query is not a valid SQL statement.")
    if ";" not in sql_query:
        raise ValueError("Generated query does not end with a semicolon.")
    print("=" * 50)
    print("💡 Follow up Request:")
    print(sql_query)
    return sql_query

def ensure_semicolon(sql_query):
    """
    Ensure the SQL query ends with a semicolon.
    """
    sql_query = sql_query.strip()  # Remove any trailing spaces or newlines
    if not sql_query.endswith(";"):
        sql_query += ";"
    return sql_query

def execute_sql_query(sql_query):
    """Executes the SQL query on CockroachDB and returns the result."""
    try:
        # Connect to CockroachDB
        conn = psycopg2.connect(COCKROACHDB_URL)
        cursor = conn.cursor()
        
        # Execute the SQL query
        cursor.execute(sql.SQL(sql_query))
        rows = cursor.fetchall()
        
        # Get column names
        colnames = [desc[0] for desc in cursor.description]
        
        # Convert the result to a list of dictionaries
        result = [dict(zip(colnames, row)) for row in rows]
        
        # Close the connection
        cursor.close()
        conn.close()
        
        return result
    except Exception as e:
        raise Exception(f"Error executing SQL: {str(e)}")

def beautify_response(raw_data, natural_language_query, language, conversation):
    """
    Enriches and beautifies the raw data result using OpenAI GPT and maintains context.
    """
    prompt = f"""
    You are an intelligent assistant tasked with generating contextually enriched and human-like responses. 
    A user has asked the following question: "{natural_language_query}"
    The database returned the following raw data: {raw_data}

    Previous context from the conversation: {conversation}

    Please provide a concise and clear response to the user in {"Arabic" if language == "arabic" else "English"} that:
    - Explains the results in an easy-to-read conversational style.
    - Includes any necessary details without verbosity.
    """
    # Create the OpenAI request
    response = openai.chat.completions.create(
        model="gpt-4",  # or "gpt-4"
        messages=[
            {"role": "system", "content": "You are an SQL expert and intelligent assistant."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=200
    )
    
    return response.choices[0].message.content.strip()

@app.post("/query")
async def query_database(request: Dict[str, str]):
    """
    Endpoint to process natural language queries with context.
    """
    user_id = request.get("user_id", "default")  # Use a unique ID for each user
    query = request.get("query")
    if not query:
        raise HTTPException(status_code=400, detail="Query parameter is required.")
    
     # Initialize user conversation history if not present
    if user_id not in conversation_history:
        conversation_history[user_id] = {"messages": [], "last_query_metadata": None}
    
    try:
                
        # Detect the language of the query
        language = detect_language(query)
        
        
    # Check if the query is a follow-up and handle accordingly
        if conversation_history[user_id]["last_query_metadata"]:
                last_query_metadata = conversation_history[user_id]["last_query_metadata"]
                
                # Use OpenAI to check if the current query is a follow-up
                prompt = f"""
                The user previously asked: "{last_query_metadata['query']}"
                And received the result: {last_query_metadata['result']}
                
                Now they have asked: "{query}"
                
                Is the new question a follow-up to the previous one? Answer "yes" or "no".
                """
                response = openai.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": "You are a language understanding assistant."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=10
                )
                is_follow_up = response.choices[0].message.content.strip().lower() == "yes"

                if is_follow_up:
                    follow_up_sql = generate_follow_up_sql(last_query_metadata, query)
                    query_result = execute_sql_query(follow_up_sql)
                    # Store metadata for follow-ups
                    conversation_history[user_id]["last_query_metadata"] = {
                    "query": follow_up_sql,
                    "result": query_result
                    }
                    print("\nFollow-Up SQL Query Generated:")
                    print(follow_up_sql)
                    print("\nFollow-Up Query Result:")
                    # Beautify the response with context
                    beautified_result = beautify_response(query_result, query, language, conversation_history[user_id])    
                    return beautified_result
                
        # Process the query with OpenAI and conversation history
        response = process_openai_response(query, conversation_history[user_id]["messages"])

             
        # Determine if the response is SQL or a natural language answer
        if response.upper().startswith(("SELECT", "INSERT", "UPDATE", "DELETE")):
                print("\nSQL Query Generated:")
                print(response)
                       
                # Step 2: Execute SQL on Supabase
                query_result = execute_sql_query(response)
                print(f"Raw Query Result:\n{query_result}\n")
                # Store metadata for follow-ups
                conversation_history[user_id]["last_query_metadata"] = {
                "query": response,
                "result": query_result
                }
                
                
                # Beautify the response with context
                beautified_result = beautify_response(query_result, query, language, conversation_history[user_id])
                print("=" * 50)
                print("💡 Beautified Response:")
                print(beautified_result)
                print("=" * 50 + "\n")
                return beautified_result
        else:
                print("\nNatural Language Response:")
                print(response)
                return beautified_result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))