import asyncio
import os
import json
import logging
from datetime import datetime
import redis.asyncio as redis
from sqlalchemy import text
from database import AsyncSessionLocal

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BROKER_URL = os.getenv("BROKER_URL", "redis://localhost:6379/0")
QUEUE_NAME = "event_queue"

redis_client = redis.from_url(BROKER_URL)

async def process_event(event_data: dict):
    # This function handles the idempotency and deduplication transaction
    async with AsyncSessionLocal() as session:
        try:
            # First, increment the received count
            await session.execute(text("UPDATE stats SET received = received + 1 WHERE id = 1"))
            
            # Upsert the event. ON CONFLICT DO NOTHING ensures idempotency.
            # We return id to check if the insertion was successful or ignored.
            stmt = text("""
                INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
                VALUES (:topic, :event_id, :timestamp, :source, :payload)
                ON CONFLICT (topic, event_id) DO NOTHING
                RETURNING id;
            """)
            
            # Parse the string timestamp back into a datetime object for asyncpg
            parsed_timestamp = datetime.fromisoformat(event_data["timestamp"])
            
            result = await session.execute(stmt, {
                "topic": event_data["topic"],
                "event_id": event_data["event_id"],
                "timestamp": parsed_timestamp,
                "source": event_data["source"],
                "payload": json.dumps(event_data["payload"])
            })
            
            inserted = result.fetchone()
            
            if inserted:
                # Event was successfully inserted (unique)
                await session.execute(text("UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1"))
                logger.info(f"Processed unique event: {event_data['topic']} - {event_data['event_id']}")
            else:
                # Event was ignored (duplicate)
                await session.execute(text("UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1"))
                logger.info(f"Dropped duplicate event: {event_data['topic']} - {event_data['event_id']}")
                
            await session.commit()
        except Exception as e:
            await session.rollback()
            logger.error(f"Error processing event: {e}")

async def start_worker():
    logger.info("Worker started, waiting for events...")
    while True:
        try:
            # BLPOP blocks until an item is available in the queue
            result = await redis_client.blpop([QUEUE_NAME], timeout=1)
            if result:
                _, data = result
                event_data = json.loads(data)
                # Process the event
                await process_event(event_data)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(1) # Backoff on error
