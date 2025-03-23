from fastapi import FastAPI, HTTPException, Depends, Query, Header
from typing import Optional, List, Dict, Any
import sqlite3
from datetime import date, datetime
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Simple Data Pipeline API")

# Database connection
def get_db_connection():
    conn = sqlite3.connect('data_pipeline.db')
    conn.row_factory = sqlite3.Row
    return conn

# API key verification
def verify_api_key(api_key: str = Header(..., alias="X-API-Key")):
    valid_keys = ["test_api_key", "demo_key"]
    if api_key not in valid_keys:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key

@app.get("/data")
async def get_data(
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
    location: Optional[str] = Query(None),
    gender: Optional[str] = Query(None),
    min_age: Optional[int] = Query(None),
    max_age: Optional[int] = Query(None),
    mobile_name: Optional[str] = Query(None),
    cursor: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=100),
    api_key: str = Depends(verify_api_key)
):
    try:
        logger.info(f"API request received with params: cursor={cursor}, limit={limit}")
        
        # Connect to database
        conn = get_db_connection()
        cursor_obj = conn.cursor()
        
        # Build query - include rowid for pagination
        query = "SELECT *, rowid FROM processed_data WHERE 1=1"
        params = []
        
        # Add filters
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        if location:
            query += " AND customer_location LIKE ?"
            params.append(f"%{location}%")
        if gender:
            query += " AND gender = ?"
            params.append(gender)
        if min_age is not None:
            query += " AND age >= ?"
            params.append(min_age)
        if max_age is not None:
            query += " AND age <= ?"
            params.append(max_age)
        if mobile_name:
            query += " AND mobile_name LIKE ?"
            params.append(f"%{mobile_name}%")
        if cursor:
            query += " AND rowid > ?"
            params.append(int(cursor))
        
        # Add sorting and limit
        query += " ORDER BY rowid LIMIT ?"
        params.append(limit + 1)  # +1 to check if there are more results
        
        logger.info(f"Executing query: {query}")
        logger.info(f"Query parameters: {params}")
        
        # Execute query
        cursor_obj.execute(query, params)
        rows = cursor_obj.fetchall()
        
        # Process results
        has_more = len(rows) > limit
        data = rows[:limit]
        
        # Convert rows to dictionaries
        data_dicts = []
        for row in data:
            item = {}
            for key in row.keys():
                item[key] = row[key]
            data_dicts.append(item)
        
        # Generate next cursor
        next_cursor = str(data_dicts[-1]["rowid"]) if has_more and data_dicts else None
        
        # Get total count
        count_query = "SELECT COUNT(*) as count FROM processed_data WHERE 1=1"
        count_params = []
        
        # Add filters to count query
        if start_date:
            count_query += " AND date >= ?"
            count_params.append(start_date)
        if end_date:
            count_query += " AND date <= ?"
            count_params.append(end_date)
        if location:
            count_query += " AND customer_location LIKE ?"
            count_params.append(f"%{location}%")
        if gender:
            count_query += " AND gender = ?"
            count_params.append(gender)
        if min_age is not None:
            count_query += " AND age >= ?"
            count_params.append(min_age)
        if max_age is not None:
            count_query += " AND age <= ?"
            count_params.append(max_age)
        if mobile_name:
            count_query += " AND mobile_name LIKE ?"
            count_params.append(f"%{mobile_name}%")
        
        cursor_obj.execute(count_query, count_params)
        count_row = cursor_obj.fetchone()
        total_count = count_row["count"] if count_row else 0
        
        # Close connection
        conn.close()
        
        return {
            "items": data_dicts,
            "next_cursor": next_cursor,
            "total_count": total_count
        }
    except Exception as e:
        logger.error(f"Error in /data endpoint: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to retrieve data: {str(e)}")

@app.get("/health")
async def health_check():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM processed_data")
        row = cursor.fetchone()
        count = row["count"] if row else 0
        conn.close()
        return {
            "status": "healthy",
            "total_records": count,
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Health check failed: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)