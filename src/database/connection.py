"""Database connection and utilities module."""

import os
import psycopg2
from psycopg2.extras import execute_values
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()


class DatabaseConnection:
    """Manages database connections and operations."""

    def __init__(
        self,
        host: str = None,
        port: int = None,
        user: str = None,
        password: str = None,
        database: str = None,
    ):
        """
        Initialize database connection.

        Args:
            host: Database host (default from POSTGRES_HOST env var)
            port: Database port (default from POSTGRES_PORT env var)
            user: Database user (default from POSTGRES_USER env var)
            password: Database password (default from POSTGRES_PASSWORD env var)
            database: Database name (default from POSTGRES_DB env var)
        """
        self.host = host or os.getenv("POSTGRES_HOST", "postgres")
        self.port = port or int(os.getenv("POSTGRES_PORT", 5432))
        self.user = user or os.getenv("POSTGRES_USER", "airflow")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "airflow")
        self.database = database or os.getenv("POSTGRES_DB", "resiliency_db")
        self.connection = None

    def connect(self) -> None:
        """Establish database connection."""
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            print(f"Connected to database: {self.database}")
        except psycopg2.Error as e:
            print(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            print("Disconnected from database")

    def execute_query(self, query: str, params: tuple = None) -> None:
        """
        Execute a query without returning results.

        Args:
            query: SQL query to execute
            params: Parameters for parameterized query
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            self.connection.commit()
            cursor.close()
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Query execution failed: {e}")
            raise

    def fetch_query(self, query: str, params: tuple = None) -> List[tuple]:
        """
        Execute a query and return results.

        Args:
            query: SQL query to execute
            params: Parameters for parameterized query

        Returns:
            List of tuples representing rows
        """
        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params or ())
            results = cursor.fetchall()
            cursor.close()
            return results
        except psycopg2.Error as e:
            print(f"Query execution failed: {e}")
            raise

    def insert_records(
        self, table: str, schema: str = "resiliency", records: List[Dict[str, Any]] = None
    ) -> int:
        """
        Insert multiple records into a table.

        Args:
            table: Table name
            schema: Schema name
            records: List of dictionaries representing records

        Returns:
            Number of records inserted
        """
        if not records:
            return 0

        if not self.connection:
            self.connect()

        try:
            cursor = self.connection.cursor()

            # Get columns from first record
            columns = list(records[0].keys())
            placeholders = ",".join(["%s"] * len(columns))
            column_names = ",".join(columns)

            # Create values list
            values = [tuple(r.get(col) for col in columns) for r in records]

            query = f"INSERT INTO {schema}.{table} ({column_names}) VALUES %s"

            execute_values(cursor, query, values, template=None, fetch=False)
            self.connection.commit()

            rows_inserted = cursor.rowcount
            cursor.close()

            return rows_inserted
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Insert failed: {e}")
            raise

    def upsert_records(
        self, 
        table: str, 
        records: List[Dict[str, Any]], 
        unique_keys: List[str],
        schema: str = "resiliency",
        update_columns: List[str] = None
    ) -> int:
        """
        Upsert (insert or update) multiple records into a table.
        
        Handles idempotent operations - re-running produces same result.
        Uses PostgreSQL ON CONFLICT ... DO UPDATE pattern.

        Args:
            table: Table name
            records: List of dictionaries representing records
            unique_keys: List of column names that form the unique constraint
            schema: Schema name
            update_columns: Columns to update on conflict (default: all except unique_keys)

        Returns:
            Number of rows affected
        """
        if not records:
            return 0

        if not self.connection:
            self.connect()

        try:
            # Get all columns from first record
            all_columns = list(records[0].keys())
            
            # Determine which columns to update (all except unique keys)
            if update_columns is None:
                update_columns = [col for col in all_columns if col not in unique_keys]
            
            # Build column list and placeholders
            column_names = ",".join(all_columns)
            placeholders = ",".join(["%s"] * len(all_columns))
            
            # Build the UPDATE clause (SET col = EXCLUDED.col for each update column)
            update_clause = ",\n    ".join(
                [f"{col} = EXCLUDED.{col}" for col in update_columns]
            )
            
            # Build conflict clause
            conflict_keys = ",".join(unique_keys)
            
            # Build complete upsert SQL
            sql = f"""
            INSERT INTO {schema}.{table} ({column_names})
            VALUES %s
            ON CONFLICT ({conflict_keys})
            DO UPDATE SET
                {update_clause}
            """
            
            # Create values list from records
            values = [tuple(r.get(col) for col in all_columns) for r in records]
            
            cursor = self.connection.cursor()
            execute_values(cursor, sql, values, fetch=False)
            self.connection.commit()
            
            rows_affected = len(records)
            cursor.close()
            
            return rows_affected
        except psycopg2.Error as e:
            self.connection.rollback()
            print(f"Upsert failed: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
