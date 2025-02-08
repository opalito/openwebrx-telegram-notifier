#!/usr/bin/env python3
"""
MQTT to Telegram Bridge
This script connects to an MQTT broker and forwards messages to a Telegram chat.
"""
import logging
import time
import asyncio
from typing import Any
import json
from datetime import datetime, timezone, timedelta
import aiohttp
import paho.mqtt.client as mqtt
from telegram import Bot
from telegram.error import TelegramError
from telegram.request import HTTPXRequest
from config import *

# Configure logging
log_level = logging.DEBUG if MQTT_DEBUG == "YES" or TELEGRAM_DEBUG == "YES" else logging.INFO
logging.basicConfig(
    level=log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def on_connect(client, userdata, flags, reason_code, properties):
    """Callback for when the client connects to the MQTT broker."""
    if reason_code == 0:  # 0 means success in MQTT v5
        logger.info("Connected to MQTT broker successfully")
        # Subscribe to all configured topics
        for topic in MQTT_TOPICS:
            client.subscribe(topic, qos=1)
            logger.info(f"Subscribed to topic: {topic}")
    else:
        logger.error(f"Failed to connect to MQTT broker with reason code: {reason_code}")

def on_disconnect(client, userdata, disconnect_flags, reason_code, properties):
    """Callback for when the client disconnects from the MQTT broker."""
    logger.warning(f"Disconnected from MQTT broker with reason code: {reason_code}")
    if reason_code != 0:  # Non-zero means unexpected disconnect
        while not client.is_connected():
            try:
                logger.info("Attempting to reconnect...")
                client.reconnect()
                time.sleep(2)
            except Exception as e:
                logger.error(f"Reconnection failed: {e}")
                time.sleep(5)

async def get_ip_info(ip: str) -> dict:
    """Get information about an IP address from ip-api.com."""
    try:
        async with aiohttp.ClientSession() as session:
            url = f"http://ip-api.com/json/{ip}?fields=status,country,city,isp"
            async with session.get(url) as response:
                data = await response.json()
                if data["status"] == "success":
                    return data
                return None
    except Exception as e:
        logger.error(f"Error getting IP info: {e}")
        return None


async def send_telegram_message(message: str) -> None:
    """Send a message to Telegram."""
    global bot
    try:
        if TELEGRAM_DEBUG == "YES":
            logger.debug(f"Preparing to send message to Telegram:")
            logger.debug(f"Chat ID: {CHAT_ID}")
            logger.debug(f"Message content: {message}")
        
        # Create a new event loop for this message
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Create a new bot instance for this message
        request = HTTPXRequest(connection_pool_size=8)
        bot = Bot(token=TELEGRAM_TOKEN, request=request)
        
        # Send the message as is, since it's already formatted
        formatted_message = message
        
        response = await bot.send_message(chat_id=CHAT_ID, text=formatted_message)
        logger.info(f"Message sent successfully to Telegram. Message ID: {response.message_id}")
        logger.debug(f"Full Telegram response: {response}")
        
        # Clean up
        loop.close()
    except Exception as e:
        logger.error(f"Failed to send message to Telegram: {e}")
        logger.error(f"Token: {TELEGRAM_TOKEN[:10]}... Chat ID: {CHAT_ID}")
        logger.exception("Full Telegram error details:")

def on_message(client, userdata, message):
    """Callback for when a message is received from the MQTT broker."""
    try:
        decoded_payload = message.payload.decode()
        
        if MQTT_DEBUG == "YES":
            logger.debug(f"Topic: {message.topic}")
            logger.debug(f"Payload (decoded): {decoded_payload}")

        try:
            # Parse JSON payload
            payload = json.loads(decoded_payload)
            
            if message.topic == "OpenwebRX/CLIENT":
                if payload.get("mode") == "CLIENT":
                    # Extract IP removing ::ffff: prefix if present
                    raw_ip = payload.get("ip", "")
                    ip = raw_ip.split(":")[-1] if "::ffff:" in raw_ip else raw_ip
                    
                    if payload.get("state") == "Connected":
                        message_text = f"👤 Cliente Conectado\n🌐 IP: {ip}"
                        logger.info(f"Client connected from IP: {ip}")

                        # Create and run a new event loop for async operations
                        async def process_ip_info():
                            nonlocal message_text
                            ip_info = await get_ip_info(ip)
                            if ip_info:
                                message_text = (
                                    "👤 Cliente Conectado\n"
                                    f"🌐 IP: {ip}\n"
                                    f"🏳️ País: {ip_info.get('country', 'N/A')}\n"
                                    f"🏢 Ciudad: {ip_info.get('city', 'N/A')}\n"
                                    f"📡 ISP: {ip_info.get('isp', 'N/A')}"
                                )
                            else:
                                message_text = (
                                    "👤 Cliente Conectado\n"
                                    f"🌐 IP: {ip}\n"
                                    "No se pudo obtener información adicional de la IP"
                                )
                            return message_text

                        # Run the async function
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        message_text = loop.run_until_complete(process_ip_info())
                        loop.close()
                    elif payload.get("state") == "Disconnected":
                        message_text = f"👤 Cliente Desconectado\n🌐 IP: {ip}"
                        logger.info(f"Client disconnected from IP: {ip}")
                else:
                    if message.topic == "OpenwebRX/RX":
                        formatted_message_parts = []
                        for key, value in payload.items():
                            formatted_message_parts.append(f"{key}: {value}")
                        message_text = "\n".join(formatted_message_parts)
                    else:
                        message_text = f"Topic: {message.topic}\nMessage: {decoded_payload}"
                    logger.info(f"Other message received: {message_text}")
            else:
                if message.topic == "OpenwebRX/RX":
                    formatted_message_parts = []
                    for key, value in payload.items():
                        if key == "freq":
                            value = float(value) / 1_000_000
                            formatted_message_parts.append(f"{key}: {value:.6f} MHz")
                        else:
                            formatted_message_parts.append(f"{key}: {value}")
                    message_text = "\n".join(formatted_message_parts)
                else:
                    message_text = f"Topic: {message.topic}\nMessage: {decoded_payload}"
                logger.info(f"Message received on topic {message.topic}")
        except json.JSONDecodeError:
            message_text = f"Topic: {message.topic}\nMessage: {decoded_payload}"
            logger.error("Failed to parse JSON payload")

        # Create and run a new event loop for this message
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(send_telegram_message(message_text))
        finally:
            loop.close()
    except Exception as e:
        logger.error(f"Error processing message: {e}")
        logger.exception("Full exception details:")

async def verify_bot_connection():
    """Verify the Telegram bot connection and send startup message."""
    try:
        # Initialize Telegram Bot with custom request handler
        global bot
        request = HTTPXRequest(connection_pool_size=8)
        bot = Bot(token=TELEGRAM_TOKEN, request=request)
        logger.info(f"Telegram bot initialized with token: {TELEGRAM_TOKEN[:10]}...")
        
        # Verify bot connection
        me = await bot.get_me()
        logger.info(f"Bot connection verified. Bot username: @{me.username}")

        # Send a connection message via Telegram
        await bot.send_message(chat_id=CHAT_ID, text="MQTT Bridge Service Started")
        logger.info("Sent startup message to Telegram")
    except TelegramError as e:
        logger.error(f"Failed to send startup message: {e}")
        raise

if __name__ == "__main__":
    try:
        # Initialize bot and verify connection
        asyncio.run(verify_bot_connection())

        # Initialize MQTT Client with protocol v5
        client = mqtt.Client(client_id="telegram_bridge",
                           protocol=mqtt.MQTTv5)
        
        client.username_pw_set(username=MQTT_USER, password=MQTT_PASSWORD)
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect

        # Enable MQTT logging
        client.enable_logger(logger)

        while True:
            try:
                # Connect to MQTT Broker
                client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
                # Start the loop
                client.loop_forever()
            except KeyboardInterrupt:
                logger.info("Service stopped by user")
                client.disconnect()
                break
            except Exception as e:
                logger.error(f"Connection error: {e}")
                time.sleep(5)  # Wait before retrying

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
