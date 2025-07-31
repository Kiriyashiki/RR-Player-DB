from datetime import timezone, datetime


def round_down_to_interval(ts: int, interval_min: int = 5) -> int:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    bucket_min = (dt.minute // interval_min) * interval_min
    rounded = dt.replace(minute=bucket_min, second=0, microsecond=0)
    return int(rounded.timestamp())


def get_last_refresh(conn):
    cur = conn.cursor()
    cur.execute('SELECT last_refresh FROM metadata LIMIT 1')
    row = cur.fetchone()
    return row[0] if row else None
