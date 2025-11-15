# crawling-scheduler/features/scheduler/publisher.py
import os
import aio_pika
import json

RABBIT_URL = os.getenv("RABBIT_URL", " amqp://guest:guest@rabbitmq:5672/")

class RabbitPublisher:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(RABBIT_URL)
        self.channel = await self.connection.channel()
        await self.channel.declare_queue("crawl_tasks", durable=True)

    async def publish(self, payload: dict):
        message = aio_pika.Message(
            body=json.dumps(payload, ensure_ascii=False).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )
        await self.channel.default_exchange.publish(
            message, routing_key="crawl_tasks"
        )

    async def close(self):
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()