import hashlib
import json

from app.schemas import EventCreate


def request_hash(event: EventCreate) -> str:
    # Omitting absent optional fields preserves fingerprints of pre-migration requests.
    canonical = json.dumps(event.model_dump(mode="json", exclude_none=True, exclude={"schedule_generation"}), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
