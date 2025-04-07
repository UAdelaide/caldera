import asyncio
import json
import logging
import aiohttp
from aiohttp import web
from app.utility.base_world import BaseWorld

name = "WebsocketReporter"
description = "Provides a websocket for real-time operation reporting and control."
address = "/plugin/websocket_reporter/ws"

connections = set()


async def handle_websocket(request):
    """Handles incoming websocket connections."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    logging.info(f"Websocket client connected: {request.remote}")
    connections.add(ws)

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                logging.debug(f"Received websocket message: {msg.data}")
                try:
                    data = json.loads(msg.data)
                    if data.get("action") == "stop_operation":
                        await stop_caldera_operation(data.get("operation_id"))
                        await ws.send_str(
                            json.dumps(
                                {
                                    "status": "stop_received",
                                    "operation_id": data.get("operation_id"),
                                }
                            )
                        )
                    else:
                        await ws.send_str(json.dumps({"status": "unknown_action"}))
                except json.JSONDecodeError:
                    logging.warning(f"Received invalid JSON: {msg.data}")
                    await ws.send_str(
                        json.dumps({"status": "error", "message": "Invalid JSON"})
                    )
                except Exception as e:
                    logging.error(f"Error processing websocket message: {e}")
                    await ws.send_str(
                        json.dumps({"status": "error", "message": str(e)})
                    )

            elif msg.type == aiohttp.WSMsgType.ERROR:
                logging.error(
                    f"Websocket connection closed with exception {ws.exception()}"
                )

    finally:
        logging.info(f"Websocket client disconnected: {request.remote}")
        connections.remove(ws)

    return ws


async def operation_finish_hook(operation):
    """Hook function called when an operation might be considered finished or updated."""
    logging.info(
        f"Hook triggered for operation: {operation.id}, State: {operation.state}"
    )

    message = json.dumps(
        {
            "type": "operation_update",
            "operation_id": operation.id,
            "name": operation.name,
            "state": operation.state,
        }
    )
    if connections:
        logging.debug(f"Broadcasting operation update: {message}")
        await asyncio.gather(
            *[ws.send_str(message) for ws in connections], return_exceptions=True
        )


async def stop_caldera_operation(operation_id):
    """Sends a request to Caldera's API to stop (or finish) an operation."""
    if not operation_id:
        logging.warning("Stop request received without operation_id")
        return

    logging.info(f"Attempting to stop operation via API: {operation_id}")
    app_svc = BaseWorld.get_service("app_svc")
    op_svc = BaseWorld.get_service("op_svc")
    rest_svc = BaseWorld.get_service("rest_svc")

    try:
        caldera_url = f"http://localhost:{app_svc.get_config('port')}"
        api_key = app_svc.get_config("api_key_red")

        headers = {"KEY": api_key, "Content-Type": "application/json"}
        payload = {"state": "finished"}

        async with aiohttp.ClientSession(headers=headers) as session:
            url = f"{caldera_url}/api/v2/operations/{operation_id}"
            async with session.patch(url, json=payload) as resp:
                if resp.status == 200:
                    logging.info(
                        f"Successfully requested stop for operation {operation_id} via API. Status: {resp.status}"
                    )
                else:
                    logging.error(
                        f"Failed to stop operation {operation_id} via API. Status: {resp.status}, Response: {await resp.text()}"
                    )

    except Exception as e:
        logging.error(f"Error while trying to stop operation {operation_id}: {e}")


# Plugin initialization
async def enable(services):
    app_svc = services.get("app_svc")
    app = app_svc.application

    app.router.add_route("GET", address, handle_websocket)
    logging.info(f"Websocket reporter endpoint enabled at {address}")


async def disable(services):
    logging.info("Disabling websocket reporter plugin.")
    for ws in list(connections):
        await ws.close(code=aiohttp.WSCloseCode.GOING_AWAY, message="Server shutdown")
    connections.clear()
