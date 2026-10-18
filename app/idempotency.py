import hashlib
import json

from app.schemas import EventCreate


def request_hash(event: EventCreate) -> str:
    canonical = json.dumps(event.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
