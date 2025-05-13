import sys
import re
from sqlalchemy import create_engine, MetaData
from sqlalchemy.schema import CreateTable
from sqlalchemy.pool import StaticPool
from sqlalchemy.types import (
    Text, Integer, Float, Boolean, String, Date, DateTime, LargeBinary
)
from sqlalchemy.dialects import postgresql

# PostgreSQL to SQLite type mapping
type_overrides = {
    "INTEGER": Integer,
    "BIGINT": Integer,
    "SMALLINT": Integer,
    "FLOAT": Float,
    "REAL": Float,
    "DOUBLE_PRECISION": Float,
    "BOOLEAN": Integer,
    "NUMERIC": Float,
    "DECIMAL": Float,
    "VARCHAR": String,
    "TEXT": Text,
    "CHAR": String,
    "DATE": Date,
    "TIMESTAMP": DateTime,
    "TIME": String,
    "INTERVAL": String,
    "JSON": Text,
    "JSONB": Text,
    "BYTEA": LargeBinary,
    "UUID": String,
    "ARRAY": Text,
    "ENUM": String,
    "TSVECTOR": Text,
    "MONEY": Float,
    "OID": Integer,
    "BIGSERIAL": Integer,
}

def patch_column_types(table):
    """Patch column types for SQLite compatibility and detect AUTOINCREMENT."""
    pk_cols = {col.name for col in table.primary_key.columns}
    for column in list(table.columns):
        coltype = column.type.__class__.__name__.upper()
        if coltype in type_overrides:
            column.type = type_overrides[coltype]()
        else:
            print(f"⚠️ Unknown type '{coltype}' in column '{column.name}' of table '{table.name}' → defaulting to TEXT.")
            column.type = Text()

def clean_postgresql_artifacts(sql: str) -> str:
    # Remove PostgreSQL-style casts like '::character varying'
    sql = re.sub(r"::[a-zA-Z0-9_ ]+", "", sql)
    # Remove schema qualifications from FK references
    sql = re.sub(r"REFERENCES\s+[a-zA-Z0-9_]+\.", "REFERENCES ", sql)
    # Remove DEFAULT nextval(...) expressions
    sql = re.sub(r"DEFAULT\s+\(nextval\('[^']+'\)\)", "", sql)
    return sql

def convert_schemas(postgres_conn_str, sqlite_schema_file):
    """Convert PostgreSQL schema to SQLite-compatible schema."""
    postgres_engine = create_engine(postgres_conn_str)
    metadata = MetaData()
    metadata.reflect(bind=postgres_engine)

    sqlite_engine = create_engine('sqlite://', poolclass=StaticPool)

    with open(sqlite_schema_file, 'w', encoding='utf-8') as f:
        for table in metadata.sorted_tables:
            patch_column_types(table)

            try:
                compiled = CreateTable(table).compile(sqlite_engine)
                stmt = str(compiled)

                cleaned_stmt = clean_postgresql_artifacts(stmt.strip())
                f.write(cleaned_stmt + ';')

            except Exception as e:
                print(f"❌ Error compiling table '{table.name}': {e}")

    print(f"✅ Final schema with AUTOINCREMENT (refined PK detection) saved to '{sqlite_schema_file}'.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python convert-schema.py <postgres_connection_string> <sqlite_schema_output.sql>")
        sys.exit(1)

    postgres_conn_str = sys.argv[1]
    sqlite_schema_file = sys.argv[2]

    convert_schemas(postgres_conn_str, sqlite_schema_file)

