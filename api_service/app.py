# api_service/app.py
import os
import json
import uuid
import pika
from datetime import datetime
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, Column, String, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# --- Configuration ---
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_QUEUE = "nofrom flask import Flask, request, jsonify
import redis
import json
import os
from database import save_to_db

app = Flask(__name__)
# Use environment variable for Redis host (default to 'redis' for Docker)
redis_host = os.getenv('REDIS_HOST', 'redis')
r = redis.Redis(host=redis_host, port=6379, db=0)

@app.route('/notify', methods=['POST'])
def notify():
    data = request.json
    user_id = data.get('user_id')
    message = data.get('message')

    if not user_id or not message:
        return jsonify({"error": "Missing user_id or message"}), 400

    # 1. Persist to "Database"
    save_to_db(user_id, message)

    # 2. Publish to Redis for background workers and realtime service
    payload = json.dumps({"user_id": user_id, "message": message})
    r.publish('notifications', payload)
    
    return jsonify({"status": "Accepted", "data": data}), 202

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)tification_tasks"

# --- Database Setup (Simplified SQLAlchemy) ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:password@db/notifier_db")
# Using synchronous engine for simplicity in this example
engine = create_engine(DATABASE_URL.replace("asyncpg", "psycopg2"), future=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Notification(Base):
    __tablename__ = "notifications"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    recipient = Column(String, index=True)
    message = Column(String)
    channel = Column(String)
    status = Column(String, default="PENDING")
    created_at = Column(DateTime, default=datetime.utcnow)
    sent_at = Column(DateTime, nullable=True)

# Create tables in DB (Run once)
Base.metadata.create_all(bind=engine)

# --- Pydantic Schema ---
class NotificationRequest(BaseModel):
    recipient: str
    message: str
    channel: str # 'email', 'sms', or 'websocket'

# --- FastAPI App ---
app = FastAPI(title="Notifier API")

def publish_to_queue(notification_data: dict):
    """Publishes the task to the RabbitMQ queue."""
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST))
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        
        # Publish the message
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(notification_data),
            properties=pika.BasicProperties(delivery_mode=2) # make message persistent
        )
        connection.close()
        print(f"Published task: {notification_data['id']}")
    except pika.exceptions.AMQPConnectionError as e:
        print(f"Error connecting to RabbitMQ: {e}")
        # In a real system, you would log this error and use a fallback mechanism
        raise HTTPException(status_code=503, detail="Service unavailable: Message broker disconnected")


@app.post("/notify", status_code=202)
def send_notification(request: NotificationRequest):
    """Receives request, saves to DB, and offloads task to the worker queue."""
    
    # 1. Create unique ID and save record to DB
    new_id = str(uuid.uuid4())
    with SessionLocal() as db:
        new_notification = Notification(
            id=new_id,
            recipient=request.recipient,
            message=request.message,
            channel=request.channel,
            status="PENDING"
        )
        db.add(new_notification)
        db.commit()

    # 2. Prepare payload for the worker
    payload = {
        "id": new_id,
        "recipient": request.recipient,
        "message": request.message,
        "channel": request.channel
    }

    # 3. Publish task asynchronously (Idempotency check happens in worker if needed)
    publish_to_queue(payload)
    
    return {"message": "Notification task accepted.", "id": new_id, "status": "PENDING"}

@app.get("/status/{notification_id}")
def get_status(notification_id: str):
    """Retrieves the status of a notification."""
    with SessionLocal() as db:
        notification = db.query(Notification).filter(Notification.id == notification_id).first()
        if not notification:
            raise HTTPException(status_code=404, detail="Notification not found")
        
        return {
            "id": notification.id,
            "status": notification.status,
            "channel": notification.channel,
            "created_at": notification.created_at,
            "sent_at": notification.sent_at
        }