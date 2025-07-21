"""
Database connection and management utilities
"""
import psycopg2
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from config.settings import DATABASE_CONFIG

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and operations"""
    
    def __init__(self):
        self.config = DATABASE_CONFIG
        self.connection_string = self._build_connection_string()
        self.engine = None
        
    def _build_connection_string(self):
        """Build PostgreSQL connection string"""
        return (f"postgresql://{self.config['user']}:{self.config['password']}"
                f"@{self.config['host']}:{self.config['port']}/{self.config['database']}")
    
    def get_engine(self):
        """Get SQLAlchemy engine"""
        if self.engine is None:
            try:
                self.engine = create_engine(self.connection_string)
                logger.info("Database engine created successfully")
            except SQLAlchemyError as e:
                logger.error(f"Failed to create database engine: {e}")
                raise
        return self.engine
    
    def test_connection(self):
        """Test database connection"""
        try:
            engine = self.get_engine()
            with engine.connect() as conn:
                result = conn.execute(text("SELECT 1"))
                logger.info("Database connection test successful")
                return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def execute_sql_file(self, sql_file_path):
        """Execute SQL commands from a file"""
        try:
            with open(sql_file_path, 'r') as file:
                sql_commands = file.read()
            
            engine = self.get_engine()
            with engine.connect() as conn:
                # Split by semicolon and execute each command
                commands = [cmd.strip() for cmd in sql_commands.split(';') if cmd.strip()]
                for command in commands:
                    if command:
                        conn.execute(text(command))
                conn.commit()
                
            logger.info(f"Successfully executed SQL file: {sql_file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to execute SQL file {sql_file_path}: {e}")
            return False
    
    def get_connection(self):
        """Get raw psycopg2 connection for specific operations"""
        try:
            conn = psycopg2.connect(
                host=self.config['host'],
                port=self.config['port'],
                database=self.config['database'],
                user=self.config['user'],
                password=self.config['password']
            )
            return conn
        except psycopg2.Error as e:
            logger.error(f"Failed to get database connection: {e}")
            raise
    
    def close_engine(self):
        """Close database engine"""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            logger.info("Database engine closed")

# Global database manager instance
db_manager = DatabaseManager()
