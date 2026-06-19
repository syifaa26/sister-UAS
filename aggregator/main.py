from fastapi import FastAPI, HTTPException, Depends
import asyncio
import os
import json
from sqlalchemy import text
from database import init_db, AsyncSessionLocal
from worker import start_worker, redis_client, QUEUE_NAME
from models import PublishRequest
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)

app = FastAPI(title="Pub-Sub Aggregator API")
worker_task = None

@app.on_event("startup")
async def on_startup():
    # Initialize DB
    await init_db()
    # Start background worker
    global worker_task
    worker_task = asyncio.create_task(start_worker())

@app.on_event("shutdown")
async def on_shutdown():
    if worker_task:
        worker_task.cancel()
    await redis_client.close()

@app.get("/")
async def root():
    return {"message": "Aggregator is running"}

@app.post("/publish", status_code=202)
async def publish_event(request: PublishRequest):
    # Push events to Redis queue
    for event in request.events:
        # Convert datetime to ISO format string
        event_dict = event.model_dump()
        event_dict['timestamp'] = event_dict['timestamp'].isoformat()
        
        await redis_client.rpush(QUEUE_NAME, json.dumps(event_dict))
    
    return {"message": f"Accepted {len(request.events)} events for processing"}

@app.get("/events")
async def get_events(topic: str = None, limit: int = 100):
    async with AsyncSessionLocal() as session:
        query = "SELECT topic, event_id, timestamp, source, payload, processed_at FROM processed_events"
        params = {"limit": limit}
        
        if topic:
            query += " WHERE topic = :topic"
            params["topic"] = topic
            
        query += " ORDER BY processed_at DESC LIMIT :limit"
        
        result = await session.execute(text(query), params)
        events = []
        for row in result:
            events.append({
                "topic": row.topic,
                "event_id": row.event_id,
                "timestamp": row.timestamp,
                "source": row.source,
                "payload": row.payload,
                "processed_at": row.processed_at
            })
        return events

@app.get("/stats")
async def get_stats():
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("SELECT received, unique_processed, duplicate_dropped FROM stats WHERE id = 1"))
        stats = result.fetchone()
        
        if stats:
            return {
                "received": stats.received,
                "unique_processed": stats.unique_processed,
                "duplicate_dropped": stats.duplicate_dropped,
                "uptime_note": "Service is running"
            }
        return {"message": "Stats not initialized"}
