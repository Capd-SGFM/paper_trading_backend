from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import redis.asyncio as redis
import json
import asyncio
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    redis_client = redis.from_url("redis://redis:6379/0")
    pubsub = redis_client.pubsub()
    await pubsub.subscribe("market_data")

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True)
            if message:
                data = message["data"].decode("utf-8")
                await websocket.send_text(data)
            await asyncio.sleep(0.01)
    except WebSocketDisconnect:
        logger.info("WebSocket disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        await pubsub.unsubscribe("market_data")
        await redis_client.close()
