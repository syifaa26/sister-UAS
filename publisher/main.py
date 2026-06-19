import time
import os
import httpx
import uuid
import random
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TARGET_URL = os.getenv("TARGET_URL", "http://localhost:8080/publish")
TOPICS = ["user_actions", "system_metrics", "billing_events"]

def generate_event(event_id=None):
    if not event_id:
        event_id = str(uuid.uuid4())
        
    return {
        "topic": random.choice(TOPICS),
        "event_id": event_id,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "publisher-simulator-1",
        "payload": {
            "value": random.randint(1, 100),
            "status": "active"
        }
    }

def main():
    logger.info(f"Publisher started. Target: {TARGET_URL}")
    
    # Wait for the aggregator to be fully ready
    time.sleep(5)
    
    with httpx.Client() as client:
        while True:
            try:
                # Generate a batch of events (e.g., 10 unique events)
                base_events = [generate_event() for _ in range(10)]
                
                # Introduce ~30% duplicates as required by the spec
                # Pick 3 random events from the base_events and duplicate them
                duplicates = random.choices(base_events, k=3)
                
                # Combine and shuffle
                batch = base_events + duplicates
                random.shuffle(batch)
                
                payload = {"events": batch}
                
                response = client.post(TARGET_URL, json=payload, timeout=10.0)
                
                if response.status_code == 202:
                    logger.info(f"Successfully published batch of {len(batch)} events (including 3 duplicates).")
                else:
                    logger.error(f"Failed to publish: {response.status_code} - {response.text}")
                    
            except httpx.RequestError as e:
                logger.error(f"Connection error: {e}")
                
            # Wait before sending the next batch
            time.sleep(2)

if __name__ == "__main__":
    main()
