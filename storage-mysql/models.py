from sqlalchemy import Integer, Float, String, DateTime, func
from sqlalchemy.orm import DeclarativeBase, mapped_column
from datetime import datetime

class Base(DeclarativeBase):
    pass



class PerformanceReading(Base):
    __tablename__ = "performance_reading"
    
    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(String(100), nullable=False)
    server_id = mapped_column(String(100), nullable=False)
    cpu = mapped_column(Float, nullable=False)
    memory = mapped_column(Float, nullable=False)
    disk_io = mapped_column(Float, nullable=False)
    reporting_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'trace_id': self.trace_id,
            'server_id': self.server_id,
            'reporting_timestamp': self.reporting_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            'cpu': self.cpu,
            'memory': self.memory,
            'disk_io': self.disk_io,
            'date_created': self.date_created.strftime("%Y-%m-%dT%H:%M:%S")
        }


class ErrorReading(Base):
    __tablename__ = "error_reading"
    
    id = mapped_column(Integer, primary_key=True)
    trace_id = mapped_column(String(100), nullable=False)
    server_id = mapped_column(String(100), nullable=False)
    error_code = mapped_column(String(10), nullable=False)
    severity_level = mapped_column(Integer, nullable=False)
    avg_response_time = mapped_column(Float, nullable=False)
    error_message = mapped_column(String(250), nullable=False)
    reporting_timestamp = mapped_column(DateTime, nullable=False)
    date_created = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    
    def to_dict(self):
        """Convert to dictionary for JSON serialization"""
        return {
            'trace_id': self.trace_id,
            'server_id': self.server_id,
            'reporting_timestamp': self.reporting_timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            'error_code': self.error_code,
            'severity_level': self.severity_level,
            'avg_response_time': self.avg_response_time,
            'error_message': self.error_message,
            'date_created': self.date_created.strftime("%Y-%m-%dT%H:%M:%S")
        }

