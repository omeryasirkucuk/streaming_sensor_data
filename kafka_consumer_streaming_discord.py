import json
import discord
from kafka import KafkaConsumer
import asyncio

asyncio.set_event_loop(asyncio.new_event_loop())

bootstrap_servers = 'your_bootstrap_server'
topic_name = 'raspberrypilogs'
discord_bot_token = 'your_bot_token'

client = discord.Client()

@client.event
async def on_ready():
    print(f'We have logged in as {client.user}')

    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers=bootstrap_servers,
                             value_deserializer=lambda v: json.loads(v.decode('utf-8')))

    try:
        for message in consumer:
            received_data = message.value

            if received_data["detection"] == 'True':
                print(f"Detection is True! Data: {received_data}")


                channel_id = 'your_channel_id'
                channel = client.get_channel(int(channel_id))

                if channel:
                    room_name = received_data["room"]
                    timestamp = received_data["timestamp"]
                    message_content = f"Room where motion was detected: {room_name}, Motion time: {timestamp}"
                    await channel.send(message_content)
                else:
                    print(f"Invalid channel ID: {channel_id}")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

client.run(discord_bot_token)