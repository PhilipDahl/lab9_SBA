from sqlalchemy import Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class SubmitListingEvent(Base):
    __tablename__ = 'listing_events'
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255), nullable=False)  # Added length
    item_id = Column(String(255), nullable=False)  # Added length
    price = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=func.now(), nullable=False)
    date_created = Column(DateTime, default=func.now(), nullable=False)
    trace_id = Column(String(255), nullable=False)  # Added length

class SubmitTransactionEvent(Base):
    __tablename__ = 'transaction_events'
    id = Column(Integer, primary_key=True)
    user_id = Column(String(255), nullable=False)  # Added length
    transaction_id = Column(String(255), nullable=False)  # Added length
    amount = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=func.now(), nullable=False)
    date_created = Column(DateTime, default=func.now(), nullable=False)
    trace_id = Column(String(255), nullable=False)  # Added length
