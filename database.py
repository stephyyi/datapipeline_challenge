"""
Database Operations Module
"""
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import pandas as pd
import os
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection string

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data_pipeline.db')

# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)


class ProcessedData(Base):
    """SQLAlchemy model for TechCorner sales data"""
    __tablename__ = 'processed_data'

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, nullable=True)
    date = Column(DateTime, nullable=True)
    customer_location = Column(String, nullable=True)
    age = Column(Integer, nullable=True)
    gender = Column(String, nullable=True)
    mobile_name = Column(String, nullable=True)
    sell_price = Column(Float, nullable=True)
    from_facebook = Column(String, nullable=True)
    followed_page = Column(String, nullable=True)
    previous_purchase = Column(String, nullable=True)
    heard_of_shop = Column(String, nullable=True)
    source_file = Column(String, nullable=False)
    processed_at = Column(DateTime, default=datetime.now)


def initialize_database():
    """Create database tables if they don't exist"""
    try:
        Base.metadata.create_all(engine)
        logging.info("Database initialized successfully")
    except Exception as e:
        logging.error(f"Error initializing database: {str(e)}")
        raise


def store_dataframe(df):
    """Store a pandas DataFrame in the database"""
    try:
        df.to_sql('processed_data', engine, if_exists='replace', index=False)
        logging.info(f"Stored {len(df)} rows in the database")
        return True
    except Exception as e:
        logging.error(f"Error storing data in database: {str(e)}")
        return False


def get_data(start_date=None, end_date=None, location=None, gender=None, 
           min_age=None, max_age=None, mobile_name=None, cursor=None, limit=50):
    """Retrieve TechCorner sales data with optional filtering and pagination"""
    try:
        logging.info(f"get_data called with params: start_date={start_date}, end_date={end_date}, "
                    f"location={location}, gender={gender}, min_age={min_age}, max_age={max_age}, "
                    f"mobile_name={mobile_name}, cursor={cursor}, limit={limit}")

        # Print table columns for debugging
        try:
            from sqlalchemy import inspect
            inspector = inspect(engine)
            columns = inspector.get_columns('processed_data')
            column_names = [col['name'] for col in columns]
            logging.info(f"Database table columns: {column_names}")
        except Exception as e:
            logging.error(f"Error getting column names: {str(e)}")

        session = Session()

        try:
            # Base query
            query = session.query(ProcessedData)

            # Apply date filters
            if start_date:
                query = query.filter(ProcessedData.date >= start_date)
            if end_date:
                query = query.filter(ProcessedData.date <= end_date)

            # Apply customer demographic filters
            if location:
                query = query.filter(ProcessedData.customer_location.ilike(f"%{location}%"))
            if gender:
                query = query.filter(ProcessedData.gender == gender)
            if min_age is not None:
                query = query.filter(ProcessedData.age >= min_age)
            if max_age is not None:
                query = query.filter(ProcessedData.age <= max_age)

            # Apply product filter
            if mobile_name:
                query = query.filter(ProcessedData.mobile_name.ilike(f"%{mobile_name}%"))

            # Apply cursor-based pagination
            if cursor:
                query = query.filter(ProcessedData.id > int(cursor))

            # Apply ordering and limit
            query = query.order_by(ProcessedData.id)
            query = query.limit(limit + 1)  # +1 to check if there are more results

            # Execute query
            results = query.all()

            # Check if there are more results
            has_more = len(results) > limit
            data = results[:limit]

            # Generate next cursor
            next_cursor = str(data[-1].id) if has_more and data else None

            # Convert to dictionaries
            data_dicts = []
            for item in data:
                item_dict = {}
                for column in item.__table__.columns:
                    value = getattr(item, column.name)
                    # Convert datetime objects to ISO format strings
                    if isinstance(value, datetime):
                        value = value.isoformat()
                    item_dict[column.name] = value
                data_dicts.append(item_dict)

            # Count total matching records
            total_count_query = session.query(ProcessedData)
            if start_date:
                total_count_query = total_count_query.filter(ProcessedData.date >= start_date)
            if end_date:
                total_count_query = total_count_query.filter(ProcessedData.date <= end_date)
            if location:
                total_count_query = total_count_query.filter(ProcessedData.customer_location.ilike(f"%{location}%"))
            if gender:
                total_count_query = total_count_query.filter(ProcessedData.gender == gender)
            if min_age is not None:
                total_count_query = total_count_query.filter(ProcessedData.age >= min_age)
            if max_age is not None:
                total_count_query = total_count_query.filter(ProcessedData.age <= max_age)
            if mobile_name:
                total_count_query = total_count_query.filter(ProcessedData.mobile_name.ilike(f"%{mobile_name}%"))

            total_count = total_count_query.count()

            return {
                "items": data_dicts,
                "next_cursor": next_cursor,
                "total_count": total_count
            }
        except Exception as e:
            logging.error(f"Error in query execution: {str(e)}")
            return None
        finally:
            session.close()
    except Exception as e:
        logging.error(f"Error retrieving data from database: {str(e)}")
        return None


# For testing
if __name__ == "__main__":
    # Initialize database
    initialize_database()

    # Create sample dataframe
    df = pd.DataFrame({
        'customer_id': [10245, 10246, 10247],
        'date': [datetime.now(), datetime.now(), datetime.now()],
        'customer_location': ['Rangamati Sadar', 'Inside Rangamati', 'Outside Rangamati'],
        'age': [28, 35, 42],
        'gender': ['Male', 'Female', 'Male'],
        'mobile_name': ['iPhone 13 Pro', 'Samsung Galaxy S21', 'Google Pixel 6'],
        'sell_price': [999.99, 799.99, 699.99],
        'from_facebook': ['Yes', 'No', 'Yes'],
        'followed_page': ['Yes', 'No', 'No'],
        'previous_purchase': ['No', 'Yes', 'No'],
        'heard_of_shop': ['Yes', 'No', 'Yes'],
        'source_file': ['test.csv'] * 3,
        'processed_at': [datetime.now()] * 3
    })

    # Store sample data
    store_dataframe(df)

    # Retrieve data
    result = get_data()
    print("Retrieved data:", result)