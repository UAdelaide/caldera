import asyncio
from collections import defaultdict
from datetime import date, datetime
import json
import logging
import re

import aiohttp
from aiohttp import web

from app.service.interfaces.i_rest_svc import RestServiceInterface
from app.service.interfaces.i_data_svc import DataServiceInterface
from app.service.interfaces.i_app_svc import (
    AppServiceInterface,
)
from pydantic import BaseModel, Field, validator, ValidationError


# Response Status Codes
OP_CREATED = "operation_created"
OP_STARTED = "operation_started"
OP_UPDATED = "operation_updated"
OP_STOPPED = "operation_stopped"
OP_SUBSCRIBED = "operation_subscribed"
OP_UNSUBSCRIBED = "operation_unsubscribed"
OP_LINK_PENDING = "operation_link_pending"
OP_LINK_APPROVED = "operation_link_approved"
OP_LINK_DISCARDED = "operation_link_discarded"
ERROR = "error"

POLLING_INTERVAL_SECONDS = 5
LINK_EXECUTE_STATUS = -3
LINK_PAUSE_STATUS = -1
LINK_DISCARD_STATUS = -2
LINK_SUCCESS_STATUS = 0
LINK_FAIL_STATUS = 1
LOG_PREFIX = "[WS PLUGIN]"

name = "WebsocketReporter"
description = "Provides a websocket for operation control and subscribable polled updates with link approval."
address = "/plugin/websocket_reporter/ws"
plugin_services: dict[str, any] = {}
connections = set()
# operation_id -> set of (websocket, request_id) tuples
subscriptions = defaultdict(set)
polled_operation_ids = set()
last_operation_states: dict[str, tuple] = {}
polling_task: asyncio.Task | None = None


class CreateOperationData(BaseModel):
    name: str = Field(..., description="Name of the operation")
    planner: str = Field(default="atomic", description="Name of the planner to use")
    adversary_id: str = Field(default="", description="ID of the adversary profile")
    group: str = Field(default="", description="Agent group name for the operation")
    source: str = Field(default="basic", description="Name of the fact source")
    jitter: str = Field(
        default="2/8", description="Operation jitter in format 'min/max'"
    )
    state: str = Field(default="running", description="Initial state of the operation")
    manual_approval: bool = Field(
        default=False,
        description="Use core-modified step-by-step approval (REQUIRES CORE CHANGES - NOT USED BY POLLING)",
    )
    autonomous: bool = Field(
        default=True,
        description="Run autonomously (set to false for link-by-link approval)",
    )
    obfuscator: str = Field(default="plain-text", description="Obfuscator to use")
    auto_close: bool = Field(
        default=False, description="Automatically close operation when complete"
    )
    visibility: int = Field(
        default=50, description="Operation visibility score (higher is more visible)"
    )
    use_learning_parsers: bool = Field(
        default=False, description="Enable learning parsers"
    )

    @validator("jitter")
    def check_jitter_format(cls, v):
        if not re.match(r"^\d+/\d+$", v):
            raise ValueError('Jitter must be in the format "min/max" (e.g., "2/8")')
        try:
            min_val, max_val = map(int, v.split("/"))
            if min_val > max_val:
                raise ValueError("Jitter minimum cannot be greater than maximum")
        except ValueError as e:
            raise ValueError(f"Invalid jitter values: {e}") from e
        return v

    @validator("state")
    def check_state_value(cls, v):
        allowed_initial_states = {"running", "paused"}
        if v not in allowed_initial_states:
            logging.warning(
                f"Operation created with non-standard initial state: {v}. Allowed: {allowed_initial_states}"
            )
        return v

    @validator("visibility")
    def check_visibility_range(cls, v):
        if not 0 <= v <= 100:
            raise ValueError("Visibility must be between 0 and 100")
        return v


# TODO: needs to be used to standardize link approval requests
class ApprovalRequest(BaseModel):
    operation_id: str = Field(..., description="ID of the operation")
    link_id: str = Field(..., description="ID of the link awaiting approval")
    decision: str = Field(
        ..., description="Decision on the link ('approve' or 'discard')"
    )

    @validator("decision")
    def check_decision_value(cls, v):
        if v not in ["approve", "discard"]:
            raise ValueError("Decision must be either 'approve' or 'discard'")
        return v


class Response(BaseModel):
    status: str = Field(..., description="Status of the operation")
    data: dict | None = Field(
        None, description="Data related to the operation (if applicable)"
    )
    request_id: str | None = Field(
        None, description="Request ID for tracking (if applicable)"
    )
    message: str | None = Field(
        None, description="Optional message for additional info"
    )


def json_serializable_converter(obj):
    """Convert non-serializable objects for JSON.

    Parameters
    ----------
        obj: The object to convert.

    Returns
    -------
        str: A string representation of the object or a default value.
    """

    if isinstance(obj, Response):
        return obj.model_dump()

    if isinstance(obj, (datetime, date)):
        return obj.isoformat()

    try:
        if hasattr(obj, "display") and callable(obj.display):
            return obj.display
        return str(obj)
    except Exception:
        return f"{LOG_PREFIX} <unserializable type: {type(obj).__name__}>"


async def poll_operations():
    """Periodically polls subscribed operations for changes and sends updates."""

    log = logging.getLogger("ws_reporter_poller")
    log.info(f"{LOG_PREFIX}  Operation polling task started.")

    while True:
        await asyncio.sleep(POLLING_INTERVAL_SECONDS)

        data_svc: DataServiceInterface = plugin_services.get("data_svc")
        app_svc: AppServiceInterface = plugin_services.get("app_svc")

        if not data_svc or not app_svc or not polled_operation_ids:
            if not polled_operation_ids:
                log.debug(f"{LOG_PREFIX} Polling skipped: No operations being polled.")
            elif not data_svc or not app_svc:
                log.error(
                    f"{LOG_PREFIX} Polling skipped: Required services (data_svc, app_svc) not available."
                )
            continue

        current_polled_ids = list(polled_operation_ids)
        log.debug(
            f"{LOG_PREFIX} Polling {len(current_polled_ids)} operations: {current_polled_ids}"
        )

        for op_id in current_polled_ids:
            try:
                ops = await data_svc.locate("operations", match=dict(id=op_id))
                if not ops:
                    log.warning(
                        f"{LOG_PREFIX} Operation {op_id} not found during polling. Removing."
                    )
                    polled_operation_ids.discard(op_id)
                    last_operation_states.pop(op_id, None)
                    if op_id in subscriptions:
                        del subscriptions[op_id]
                    continue

                operation = ops[0]
                is_finished = await operation.is_finished()

                current_link_ids = frozenset(link.id for link in operation.chain)
                last_link_finish_time = (
                    operation.chain[-1].finish
                    if operation.chain and operation.chain[-1].finish
                    else None
                )
                current_state_snapshot = (
                    operation.state,
                    current_link_ids,
                    last_link_finish_time.isoformat()
                    if last_link_finish_time
                    else None,
                )
                last_state_snapshot = last_operation_states.get(op_id)

                if current_state_snapshot != last_state_snapshot:
                    log.info(
                        f"{LOG_PREFIX} [Poll Op {op_id}] Detected change. Old state: {last_state_snapshot}, New state: {current_state_snapshot}"
                    )
                    last_operation_states[op_id] = current_state_snapshot

                    subscribers = list(subscriptions.get(op_id, set()))
                    if not subscribers:
                        log.debug(
                            f"{LOG_PREFIX} [Poll Op {op_id}] No active subscribers for changed operation."
                        )
                        continue

                    update_payload = Response(
                        status=OP_UPDATED,
                        data=operation.display,
                    )
                    try:
                        log.debug(
                            f"{LOG_PREFIX} [Poll Op {op_id}] Sending general update to {len(subscribers)} subscribers."
                        )
                        tasks = []

                        for ws, req_id in subscribers:
                            if ws.closed:
                                log.warning(
                                    f"{LOG_PREFIX} [Poll Op {op_id}] Skipping closed websocket for subscriber {req_id}."
                                )
                                continue
                            try:
                                update_payload.request_id = req_id
                                op_update_message = json.dumps(
                                    update_payload,
                                    default=json_serializable_converter,
                                )
                                tasks.append(ws.send_str(op_update_message))
                            except Exception as send_err:
                                log.error(
                                    f"{LOG_PREFIX} [Poll Op {op_id}] Failed to send update to subscriber {req_id}: {send_err}",
                                    exc_info=True,
                                )

                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        current_subscribers = []
                        for i, result in enumerate(results):
                            if isinstance(result, Exception):
                                failed_ws = subscribers[i]
                                remote_addr = getattr(failed_ws, "_req", {}).get(
                                    "remote", "unknown"
                                )
                                log.error(
                                    f"{LOG_PREFIX} [Poll Op {op_id}] Failed to send polled update to {remote_addr}: {result}"
                                )
                                connections.discard(failed_ws)
                                if op_id in subscriptions:
                                    subscriptions[op_id].discard(failed_ws)
                                    if not subscriptions[op_id]:
                                        del subscriptions[op_id]
                                        log.info(
                                            f"{LOG_PREFIX} [Poll Op {op_id}] Removing from polling - no subscribers left after send failure."
                                        )
                                        polled_operation_ids.discard(op_id)
                                        last_operation_states.pop(op_id, None)
                            else:
                                current_subscribers.append(subscribers[i])
                        subscribers = (
                            current_subscribers  # Use updated list for approval step
                        )

                    except Exception as json_err:
                        log.error(
                            f"{LOG_PREFIX} [Poll Op {op_id}] Failed to serialize general polled update: {json_err}",
                            exc_info=True,
                        )

                    if subscribers:
                        last_link_ids = (
                            last_state_snapshot[1]
                            if last_state_snapshot
                            else frozenset()
                        )
                        new_link_ids = current_link_ids - last_link_ids

                        if new_link_ids:
                            log.info(
                                f"{LOG_PREFIX} [Poll Op {op_id}] Detected {len(new_link_ids)} new link(s): {new_link_ids}"
                            )
                            approval_tasks = []
                            for link_id in new_link_ids:
                                new_link = next(
                                    (
                                        lnk
                                        for lnk in operation.chain
                                        if lnk.id == link_id
                                    ),
                                    None,
                                )

                                if new_link:
                                    log.debug(
                                        f"{LOG_PREFIX} [Poll Op {op_id}] Checking new link {link_id}. Status: {new_link.status}, Op Autonomous: {operation.autonomous}"
                                    )

                                    needs_approval_request = False
                                    link_status_for_approval = new_link.status

                                    if not operation.autonomous:
                                        if new_link.status == LINK_PAUSE_STATUS:
                                            log.info(
                                                f"{LOG_PREFIX} [Poll Op {op_id}] New link {link_id} is already PAUSED ({LINK_PAUSE_STATUS}). Flagging for approval."
                                            )
                                            needs_approval_request = True
                                        else:
                                            log.debug(
                                                f"{LOG_PREFIX} [Poll Op {op_id}] New link {link_id} has status {new_link.status}. Not requesting approval."
                                            )
                                    else:
                                        log.debug(
                                            f"{LOG_PREFIX} [Poll Op {op_id}] Link {link_id} skipped for approval check (operation is autonomous)."
                                        )

                                    if needs_approval_request:
                                        log.info(
                                            f"{LOG_PREFIX} [Poll Op {op_id}] Preparing approval request for link {link_id} (Status: {link_status_for_approval})."
                                        )

                                        approval_payload = Response(
                                            status=OP_LINK_PENDING,
                                            data=new_link.display,
                                        )
                                        try:
                                            for ws, req_id in subscribers:
                                                if ws.closed:
                                                    log.warning(
                                                        f"{LOG_PREFIX} [Poll Op {op_id}] Skipping closed websocket for subscriber {req_id}."
                                                    )
                                                    continue
                                                approval_payload.request_id = req_id
                                                approval_message = json.dumps(
                                                    approval_payload,
                                                    default=json_serializable_converter,
                                                )
                                                approval_tasks.append(
                                                    ws.send_str(approval_message)
                                                )
                                        except Exception as json_err:
                                            log.error(
                                                f"{LOG_PREFIX} [Poll Op {op_id}] Failed to serialize approval request for link {link_id}: {json_err}",
                                                exc_info=True,
                                            )
                            if approval_tasks:
                                log.debug(
                                    f"{LOG_PREFIX} [Poll Op {op_id}] Sending {len(approval_tasks)} approval requests to relevant subscribers."
                                )
                                approval_results = await asyncio.gather(
                                    *approval_tasks,
                                    return_exceptions=True,
                                )
                                for result in approval_results:
                                    if isinstance(result, Exception):
                                        log.error(
                                            f"[Poll Op {op_id}] Failed to send an approval request: {result}"
                                        )

                if is_finished:
                    log.info(
                        f"{LOG_PREFIX} [Poll Op {op_id}] Operation has finished. Removing from polling list."
                    )
                    polled_operation_ids.discard(op_id)
                    last_operation_states.pop(op_id, None)

            except Exception as poll_err:
                log.error(
                    f"{LOG_PREFIX} [Poll Op {op_id}] Error during polling cycle: {poll_err}",
                    exc_info=True,
                )


async def handle_websocket(request: web.Request):
    """Handle incoming websocket connections and messages.

    Parameters
    ----------
        request: The incoming HTTP request.

    Returns
    -------
        web.WebSocketResponse: The websocket response object.
    """
    ws = web.WebSocketResponse()
    can_prepare = ws.can_prepare(request)
    if not can_prepare:
        logging.warning(
            f"{LOG_PREFIX} Failed ws.can_prepare, cannot upgrade connection."
        )
        return web.Response(status=400, text="Cannot upgrade connection to websocket.")
    try:
        await ws.prepare(request)
    except Exception as e:
        logging.error(
            f"{LOG_PREFIX} Websocket prepare error for {request.remote}: {e}",
            exc_info=True,
        )
        return ws

    remote_addr = request.remote
    logging.info(f"{LOG_PREFIX} Websocket client connected: {remote_addr}")
    connections.add(ws)
    ws_subscriptions = set()

    rest_svc: RestServiceInterface = plugin_services.get("rest_svc")
    data_svc: DataServiceInterface = plugin_services.get("data_svc")
    app_svc: AppServiceInterface = plugin_services.get("app_svc")

    if not all([rest_svc, data_svc, app_svc]):
        logging.error(
            f"{LOG_PREFIX} Required services (rest_svc, data_svc, app_svc) not available."
        )
        try:
            await ws.close(
                code=aiohttp.WSCloseCode.INTERNAL_ERROR,
                message="Server configuration error",
            )
        except Exception:
            pass
        connections.discard(ws)
        return ws

    global polling_task
    if polling_task is None or polling_task.done():
        if polling_task and polling_task.done():
            try:
                polling_task.result()
            except Exception as e:
                logging.error(
                    f"{LOG_PREFIX} Polling task ended unexpectedly: {e}", exc_info=True
                )
        logging.info("{LOG_PREFIX} Polling task not running. Starting...")
        loop = asyncio.get_event_loop()
        polling_task = loop.create_task(poll_operations())

    try:
        async for msg in ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                logging.debug(
                    f"{LOG_PREFIX} Received websocket message from {remote_addr}: {msg.data}"
                )
                response: Response | None = None
                try:
                    message_data = json.loads(msg.data)
                    action = message_data.get("action")
                    payload = message_data.get("data")
                    req_id = message_data.get("request_id")

                    if not action:
                        raise ValueError("Missing 'action' field.")

                    match action:
                        case "create_operation":
                            if payload is None or not isinstance(payload, dict):
                                raise ValueError(
                                    "Missing/invalid 'data' for create_operation"
                                )
                            try:
                                if payload.get("autonomous", True):
                                    logging.info(
                                        f"{LOG_PREFIX} Creating operation with autonomous=True. Link approval requires autonomous=False."
                                    )
                                if payload.get("manual_approval"):
                                    logging.warning(
                                        f"{LOG_PREFIX} 'manual_approval' flag requires core modifications."
                                    )
                                validated_data = CreateOperationData(**payload)
                                operation_data_list = await create_caldera_operation(
                                    rest_svc,
                                    validated_data.model_dump(exclude_unset=True),
                                    access_user="red",
                                )
                                if operation_data_list:
                                    response = Response(
                                        status=OP_CREATED,
                                        message="Operation creation request received.",
                                        data={"operation_data": operation_data_list},
                                        request_id=req_id,
                                    )

                                else:
                                    response = Response(
                                        status=ERROR,
                                        message="Failed to create operation.",
                                        request_id=req_id,
                                    )

                            except ValidationError as e:
                                response = Response(
                                    request_id=req_id,
                                    status=ERROR,
                                    message=f"Invalid operation data: {e}",
                                )

                        case "start":
                            if payload is None or not isinstance(payload, dict):
                                raise ValueError("Missing/invalid 'data' for start")
                            operation_id = payload.get("operation_id")
                            if not operation_id or not isinstance(operation_id, str):
                                raise ValueError("Missing/invalid 'operation_id'")
                            ops = await data_svc.locate(
                                "operations",
                                match=dict(id=operation_id),
                            )
                            if not ops:
                                raise ValueError(
                                    f"Operation '{operation_id}' not found."
                                )
                            if ops[0].autonomous:
                                logging.warning(
                                    f"{LOG_PREFIX} Subscribing to autonomous op {operation_id}."
                                )

                            subscriptions[operation_id].add((ws, req_id))
                            ws_subscriptions.add(operation_id)
                            if not await ops[0].is_finished():
                                polled_operation_ids.add(operation_id)
                                last_operation_states.pop(operation_id, None)
                                logging.info(
                                    f"{LOG_PREFIX} Client {remote_addr} subscribed to op {operation_id}. Added polling."
                                )
                            else:
                                logging.info(
                                    f"{LOG_PREFIX} Client {remote_addr} subscribed to finished op {operation_id}. Not polling."
                                )
                            success = await start_caldera_operation(
                                rest_svc,
                                operation_id,
                            )
                            if success:
                                poll_operations()
                                response = Response(
                                    status=OP_STARTED,
                                    message="Operation successfully started.",
                                    data={
                                        "operation_id": operation_id,
                                    },
                                    request_id=req_id,
                                )
                            else:
                                poll_operations()
                                response = Response(
                                    status=ERROR,
                                    data={
                                        "operation_id": operation_id,
                                    },
                                    message="Failed to start operation.",
                                    request_id=req_id,
                                )

                        case "stop_operation":
                            if payload is None or not isinstance(payload, dict):
                                raise ValueError(
                                    "Missing/invalid 'data' for stop_operation"
                                )
                            operation_id = payload.get("operation_id")
                            if not operation_id or not isinstance(operation_id, str):
                                raise ValueError("Missing/invalid 'operation_id'")
                            success = await stop_caldera_operation(
                                rest_svc, operation_id
                            )
                            if success:
                                response = Response(
                                    status=OP_STOPPED,
                                    message="Operation stop request received.",
                                    data={
                                        "operation_id": operation_id,
                                    },
                                    request_id=req_id,
                                )
                            else:
                                response = Response(
                                    status=ERROR,
                                    message="Failed to stop operation",
                                    data={
                                        "operation_id": operation_id,
                                    },
                                    request_id=req_id,
                                )

                        case "unsubscribe":
                            if payload is None or not isinstance(payload, dict):
                                raise ValueError(
                                    "Missing/invalid 'data' for unsubscribe"
                                )
                            operation_id = payload.get("operation_id")
                            if not operation_id or not isinstance(operation_id, str):
                                raise ValueError("Missing/invalid 'operation_id'")
                            if operation_id in subscriptions:
                                subscriptions[operation_id].discard(ws)
                                if not subscriptions[operation_id]:
                                    del subscriptions[operation_id]
                                    polled_operation_ids.discard(operation_id)
                                    last_operation_states.pop(operation_id, None)
                                    logging.info(
                                        f"{LOG_PREFIX} Removed {operation_id} from polling - no subscribers."
                                    )
                            ws_subscriptions.discard(operation_id)
                            logging.info(
                                f"{LOG_PREFIX} Client {remote_addr} unsubscribed from op {operation_id}"
                            )
                            poll_operations()
                            response = Response(
                                status=OP_UNSUBSCRIBED,
                                data={
                                    "operation_id": operation_id,
                                },
                                request_id=req_id,
                                message="Unsubscribed from operation.",
                            )

                        case "approve_link":
                            if payload is None or not isinstance(payload, dict):
                                raise ValueError(
                                    "Missing/invalid 'data' for approve_link"
                                )

                            link_id = payload.get("link_id")
                            decision = payload.get("decision")
                            if not link_id or not isinstance(link_id, str):
                                raise ValueError("Missing/invalid 'link_id'")
                            if not decision or decision not in ["approve", "discard"]:
                                raise ValueError(
                                    "Invalid 'decision' (must be 'approve' or 'discard')"
                                )
                            link = await app_svc.find_link(link_id)
                            if not link:
                                raise ValueError(f"Link '{link_id}' not found.")

                            new_status = -99  # Use distinct invalid status
                            if decision == "approve":
                                new_status = LINK_EXECUTE_STATUS
                                logging.info(
                                    f"{LOG_PREFIX} Approved link {link_id} (set status to {new_status})."
                                )
                                response = Response(
                                    request_id=req_id,
                                    status=OP_LINK_APPROVED,
                                    message="Link decision was approved.",
                                    data={
                                        "link_id": link_id,
                                        "decision": decision,
                                    },
                                )

                            elif decision == "discard":
                                new_status = LINK_DISCARD_STATUS
                                logging.info(
                                    f"{LOG_PREFIX} Discarded link {link_id} (set status to {new_status})."
                                )
                                response = Response(
                                    request_id=req_id,
                                    status=OP_LINK_DISCARDED,
                                    message="Link decision was discarded.",
                                    data={
                                        "link_id": link_id,
                                        "decision": decision,
                                    },
                                )

                            link.status = new_status
                            poll_operations()

                        case _:
                            response = Response(
                                status=ERROR,
                                message=f"Action '{action}' not recognized.",
                                request_id=req_id,
                            )

                except json.JSONDecodeError:
                    response = Response(
                        status=ERROR,
                        message="Invalid JSON format",
                        request_id=req_id,
                    )
                    logging.warning(f"{LOG_PREFIX} Invalid JSON: {msg.data}")

                except ValueError as e:
                    response = Response(
                        status=ERROR,
                        message=str(e),
                        request_id=req_id,
                    )
                    logging.warning(f"{LOG_PREFIX} Value error: {e}")

                except Exception as e:
                    response = Response(
                        status=ERROR,
                        message=f"Internal error: {type(e).__name__}",
                        request_id=req_id,
                    )

                    logging.error(f"{LOG_PREFIX} Processing error: {e}", exc_info=True)
                if not ws.closed:
                    try:
                        logging.info(f"{LOG_PREFIX} Sending response to {remote_addr}")
                        await ws.send_str(
                            json.dumps(
                                response,
                                default=json_serializable_converter,
                            )
                        )
                    except Exception as send_err:
                        logging.error(
                            f"{LOG_PREFIX} Send error: {send_err}", exc_info=True
                        )
                        break

            elif msg.type == aiohttp.WSMsgType.ERROR:
                logging.error(f"{LOG_PREFIX} WS error: {ws.exception()}")
                break
            elif msg.type == aiohttp.WSMsgType.CLOSED:
                logging.info(f"{LOG_PREFIX} WS closed by client {remote_addr}")
                break

    except asyncio.CancelledError:
        logging.info(f"{LOG_PREFIX} WS handler cancelled for {remote_addr}.")
    except Exception as e:
        logging.error(
            f"{LOG_PREFIX} Exception in WS handler loop {remote_addr}: {e}",
            exc_info=True,
        )
    finally:
        logging.info(f"{LOG_PREFIX} Cleaning up connection: {remote_addr}")
        connections.discard(ws)
        for op_id in list(ws_subscriptions):
            if op_id in subscriptions:
                subscriptions[op_id].discard(ws)
                if not subscriptions[op_id]:
                    del subscriptions[op_id]
                    polled_operation_ids.discard(op_id)
                    last_operation_states.pop(op_id, None)
                    logging.info(
                        f"{LOG_PREFIX} Stopped polling op {op_id} - last subscriber disconnected."
                    )
        logging.info(
            f"{LOG_PREFIX} Client {remote_addr} removed. Subs: {len(subscriptions)}, Polled: {len(polled_operation_ids)}"
        )
    return ws


async def start_caldera_operation(
    rest_svc: RestServiceInterface,
    operation_id: str,
) -> bool:
    """Wrapper function that starts a caldera operation using the REST service API.

    Parameters
    ----------
        rest_svc: The REST service interface to use for the operation.
        operation_id: The ID of the operation to start.
    Returns
    -------
        bool: True if the operation was started successfully, False otherwise.
    """

    logging.info(f"[WS PLUGIN] Attempting to start operation {operation_id} via API")
    try:
        await rest_svc.update_operation(operation_id, state="running")
        logging.info(
            f"{LOG_PREFIX} Sent request to set operation {operation_id} state to 'running'."
        )
        return True
    except Exception as e:
        logging.error(
            f"{LOG_PREFIX} Error starting operation {operation_id}: {e}", exc_info=True
        )
        return False


async def stop_caldera_operation(
    rest_svc: RestServiceInterface,
    operation_id: str,
) -> bool:
    """Wrapper function that stops a caldera operation using the REST service API.

    Parameters
    ----------
        rest_svc: The REST service interface to use for the operation.
        operation_id: The ID of the operation to stop.

    Returns
    -------
        bool: True if the operation was stopped successfully, False otherwise.
    """

    logging.info(f"Attempting to stop operation {operation_id} via API")
    try:
        await rest_svc.update_operation(operation_id, state="finished")
        logging.info(
            f"{LOG_PREFIX} Sent request to set op {operation_id} state to 'finished'."
        )
        return True
    except Exception as e:
        logging.error(
            f"{LOG_PREFIX} Error stopping op {operation_id}: {e}", exc_info=True
        )
        return False


async def create_caldera_operation(
    rest_svc: RestServiceInterface,
    data: dict,
    access_user: str = "red",
) -> list | None:
    """Wrapper function that creates a caldera operation using the REST service API.

    Parameters
    ----------
        rest_svc: The REST service interface to use for the operation.
        data: The data dictionary for the operation to create.
        access_user: The user to set access for the operation.

    Returns
    -------
        list | None: The created operation data if successful, None otherwise.

    """

    logging.info(f"{LOG_PREFIX} Attempting to create operation via API")
    logging.debug(f"{LOG_PREFIX} Data dictionary for creation: {data}")
    try:
        access_payload = {"access": [access_user]}
        op_data_list = await rest_svc.create_operation(
            access=access_payload,
            data=data,
        )
        logging.debug(
            f"{LOG_PREFIX} Operation creation API call returned: {op_data_list}"
        )
        if op_data_list and isinstance(op_data_list, list) and len(op_data_list) > 0:
            logging.info(f"{LOG_PREFIX} Successfully created operation")
            return op_data_list
        else:
            logging.error(
                f"{LOG_PREFIX} Unexpected response from create_operation: {op_data_list}"
            )
            return None
    except Exception as e:
        logging.error(f"{LOG_PREFIX} Error creating operation: {e}", exc_info=True)
        return None


async def enable(services: dict):
    """Enable the websocket reporter plugin.

    Parameters
    ----------
        services: The services dictionary containing the required services.
    """

    global plugin_services, polling_task
    plugin_services = services
    app_svc = services.get("app_svc")
    rest_svc: RestServiceInterface = services.get("rest_svc")
    data_svc: DataServiceInterface = services.get("data_svc")
    if not all([app_svc, rest_svc, data_svc]):
        missing = [
            n
            for n, s in [("app", app_svc), ("rest", rest_svc), ("data", data_svc)]
            if not s
        ]
        logging.error(
            f"{LOG_PREFIX} WebsocketReporter missing services: {', '.join(missing)}"
        )
        plugin_services.clear()
        return
    plugin_services["app_svc"] = app_svc
    app = app_svc.application
    try:
        app.router.add_route("GET", address, handle_websocket)
        logging.info(f"{LOG_PREFIX} Websocket reporter endpoint enabled at {address}")
    except Exception as e:
        logging.error(
            f"{LOG_PREFIX} Failed to add websocket route {address}: {e}", exc_info=True
        )
        return
    if polling_task is None or polling_task.done():
        logging.info(f"{LOG_PREFIX} Starting background polling task...")
        loop = asyncio.get_event_loop()
        polling_task = loop.create_task(poll_operations())
    else:
        logging.info(f"{LOG_PREFIX} Polling task already running.")


async def disable(services):
    global polling_task
    logging.info(f"{LOG_PREFIX} Disabling websocket reporter plugin.")
    if polling_task and not polling_task.done():
        logging.info(f"{LOG_PREFIX} Cancelling background polling task...")
        polling_task.cancel()
        try:
            await polling_task
        except asyncio.CancelledError:
            logging.info(f"{LOG_PREFIX} Polling task cancelled.")
        except Exception as e:
            logging.error(
                f"{LOG_PREFIX} Error cancelling polling task: {e}", exc_info=True
            )
    polling_task = None
    polled_operation_ids.clear()
    last_operation_states.clear()
    logging.info("Polling task stopped and state cleared.")
    active_connections = list(connections)
    if active_connections:
        logging.info(
            f"{LOG_PREFIX} Closing {len(active_connections)} websocket connections."
        )
        tasks = [
            ws.close(code=aiohttp.WSCloseCode.GOING_AWAY, message="Server shutdown")
            for ws in active_connections
        ]
        await asyncio.gather(*tasks, return_exceptions=True)
    connections.clear()
    subscriptions.clear()
    logging.info("{LOG_PREFIX} Websocket connections closed and subscriptions cleared.")
    plugin_services.clear()
