import duckdb
from .settings import ScraperSettings


def query[T](query: str, result: T = dict) -> list[T]:
    settings = ScraperSettings()
    con = duckdb.connect()
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute(f"""
    SET s3_endpoint = '{settings.minio_endpoint}';
    SET s3_access_key_id = '{settings.minio_access_key}';
    SET s3_secret_access_key = '{settings.minio_secret_key}';
    SET s3_url_style = 'path';  -- important for MinIO
    SET s3_use_ssl = 'true';
    """)
    cur = con.execute(query)
    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    result = [result(**dict(zip(columns, row))) for row in rows]
    con.close()
    return result
