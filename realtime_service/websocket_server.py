import asyncio
import websockets
import redis.asyncio as redis
import json
import os

connected_clients = {} # {user_id: websocket}

async def redis_listener():
    redis_host = os.getenv('REDIS_HOST', 'redis')
    r = redis.Redis(host=redis_host, port=6379, db=0)
    pubsub = r.pubsub()
    await pubsub.subscribe('notifications')
    
    async for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            user_id = data['user_id']
            if user_id in connected_clients:
                await connected_clients[user_id].send(data['message'])

async def ws_handler(websocket):
    # First message from client should be their user_id
    user_id = await websocket.recv()
    connected_clients[user_id] = websocket
    print(f"[WS] User {user_id} connected")
    try:
        await websocket.wait_closed()
    finally:
        del connected_clients[user_id]

async def main():
    asyncio.create_task(redis_listener())
    async with websockets.serve(ws_handler, "0.0.0.0", 8080):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())