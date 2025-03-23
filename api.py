"""
API Module using FastAPI
"""
from fastapi import FastAPI, Depends, HTTPException, Query, Header
from fastapi.security import APIKeyHeader
from typing import Optional, List, Dict, Any
from datetime import date, datetime
import time
from starlette.requests import Request
import logging

from database import get_data, initialize_database

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Data Pipeline API",
    description="API for accessing processed data from the pipeline",
    version="1.0.0"
)

# API key security
API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME)

# Simple in-memory rate limiting
# in prod, you would use Redis or a similar distributed cache
rate_limits = {}

@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    """Middleware for rate limiting"""
    # Get client IP or API key for rate limiting
    api_key = request.headers.get(API_KEY_NAME, None)
    client_id = api_key or request.client.host
    
    # Check rate limit (100 requests per minute)
    current_time = time.time()
    if client_id in rate_limits:
        # Check if we need to reset the window
        if current_time - rate_limits[client_id]["window_start"] > 60:
            # Reset window
            rate_limits[client_id] = {
                "count": 1,
                "window_start": current_time
            }
        else:
            # Increment count
            rate_limits[client_id]["count"] += 1
            # Check if limit exceeded
            if rate_limits[client_id]["count"] > 100:
                return {
                    "error": "Rate limit exceeded",
                    "detail": "Too many requests. Please try again later."
                }
    else:
        # First request from this client
        rate_limits[client_id] = {
            "count": 1,
            "window_start": current_time
        }
    
    # Process the request
    response = await call_next(request)
    return response

def verify_api_key(api_key: str = Header(..., alias=API_KEY_NAME)):
    """Verify API key"""
    # for simplicity, we're using hardcoded keys
    valid_keys = ["test_api_key", "demo_key"]
    
    if api_key not in valid_keys:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key"
        )
    
    return api_key

@app.get("/data")
async def read_data(
    start_date: Optional[date] = Query(None, description="Start date for filtering data"),
    end_date: Optional[date] = Query(None, description="End date for filtering data"),
    location: Optional[str] = Query(None, description="Filter by customer location"),
    gender: Optional[str] = Query(None, description="Filter by gender"),
    min_age: Optional[int] = Query(None, description="Minimum age filter"),
    max_age: Optional[int] = Query(None, description="Maximum age filter"),
    mobile_name: Optional[str] = Query(None, description="Filter by mobile device name"),
    cursor: Optional[str] = Query(None, description="Cursor for pagination"),
    limit: int = Query(50, ge=1, le=100, description="Number of items to return"),
    api_key: str = Depends(verify_api_key)
):
    """
    Get TechCorner sales data with filtering and pagination.
    
    - Filter by date range with start_date and end_date
    - Filter by customer demographics (location, gender, age range)
    - Filter by product (mobile name)
    - Use cursor-based pagination for efficient retrieval of large datasets
    """
    try:
        logger.info(f"API request received with params: start_date={start_date}, end_date={end_date}, "
                   f"location={location}, gender={gender}, min_age={min_age}, max_age={max_age}, "
                   f"mobile_name={mobile_name}, cursor={cursor}, limit={limit}")
        
        result = get_data(start_date, end_date, location, gender, min_age, max_age, mobile_name, cursor, limit)
        
        if result is None:
            logger.error("get_data returned None")
            raise HTTPException(
                status_code=500,
                detail="Failed to retrieve data"
            )
        
        # Check if we have any items
        if len(result["items"]) == 0:
            logger.info("Query returned no items")
        else:
            logger.info(f"Successfully retrieved {len(result['items'])} records")
        
        return result
    except Exception as e:
        logger.error(f"Error in /data endpoint: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Failed to retrieve data"
        )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Try to initialize the database to make sure it's accessible
        initialize_database()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail="Health check failed"
        )

# Initialize the database when the app starts
@app.on_event("startup")
async def startup_event():
    try:
        initialize_database()
        logger.info("API started and database initialized")
    except Exception as e:
        logger.error(f"Failed to initialize database on startup: {str(e)}")

# For testing
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)