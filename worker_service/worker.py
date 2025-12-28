import redis
import json
import os
import time

redis_host = os.getenv('REDIS_HOST', 'redis')
r = redis.Redis(host=redis_host, port=6379, db=0)

def process_notifications():
    pubsub = r.pubsub()
    pubsub.subscribe('notifications')
    print("Worker started. Listening for notifications...")
    
    for message in pubsub.listen():
        if message['type'] == 'message':
            data = json.loads(message['data'])
            print(f"[WORKER] Processing notification for {data['user_id']}: {data['message']}")
            # Simulate heavy lifting (sending email/SMS)
            time.sleep(1) 

if __name__ == "__main__":
    process_notifications()