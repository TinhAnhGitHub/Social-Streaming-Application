from prefect.variables import Variable
from datetime import datetime, timezone

def get_last_timestamp() -> datetime | None:
    try:
        ts = Variable.get("last_reddit_created_utc")
        return datetime.fromisoformat(ts) #type:ignore
    except Exception:
        return None

def set_last_timestamp(ts: datetime):
    Variable.set("last_reddit_created_utc", ts.isoformat())


