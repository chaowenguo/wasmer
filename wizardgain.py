import asyncio
import aiohttp
import logging
import json
import base64
import sys
import argparse
import ipaddress
import socket
import os
import uuid
import ssl
import certifi
from urllib.parse import urlparse, quote
import platform

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("wizardgain-linux")

SERVER_URL = "https://connector.wizardgain.com"
active_streams = {}  # mapping: req_id -> writer


def is_safe_target(target: str) -> bool:
    """
    Blocks local network, loopback, and private IP ranges.
    Returns True when target appears safe to connect to (public).
    """
    if not target:
        return False

    host = target
    # if URL-like, extract hostname
    if "://" in host:
        try:
            parsed = urlparse(host)
            host = parsed.hostname or ""
        except Exception:
            return False
    else:
        # strip optional :port
        if ":" in host:
            host = host.rsplit(":", 1)[0]

    if not host:
        return False

    # common local hostnames
    if host.lower() in ("localhost", "127.0.0.1", "0.0.0.0", "::1", "::"):
        return False

    try:
        # resolve host to IPv4/IPv6
        ip = socket.gethostbyname(host)
        ip_obj = ipaddress.ip_address(ip)
        if ip_obj.is_private or ip_obj.is_loopback or ip_obj.is_link_local:
            logger.warning(f"BLOCKED access to local address: {ip} ({host})")
            return False
        return True
    except Exception as e:
        # If resolution fails, be conservative and block
        logger.warning(f"is_safe_target: failed to resolve/check host {host}: {e}")
        return False


async def handle_http_request(session: aiohttp.ClientSession, data: dict) -> dict:
    """
    Executes a standard HTTP request (GET, POST, etc).
    Expects `data` with keys: id, method, url, headers (optional), body (base64 optional).
    Returns a dict with id, status, headers, body (base64), type='response'.
    """
    req_id = data.get("id")
    method = data.get("method")
    url = data.get("url")
    headers = data.get("headers") or {}
    body_b64 = data.get("body")

    logger.info(f"Executing HTTP request {req_id}: {method} {url}")

    if not is_safe_target(url):
        return {
            "id": req_id,
            "status": 403,
            "headers": {},
            "body": base64.b64encode(b"Access to local network is forbidden.").decode("utf-8"),
            "type": "response",
        }

    body = None
    if body_b64:
        try:
            body = base64.b64decode(body_b64)
        except Exception as e:
            logger.error(f"Failed to decode request body for {req_id}: {e}")
            return {
                "id": req_id,
                "status": 400,
                "headers": {},
                "body": base64.b64encode(f"Invalid base64 body: {e}".encode()).decode(),
                "type": "response",
            }

    try:
        async with session.request(method, url, headers=headers, data=body, allow_redirects=True) as response:
            resp_body = await response.read()
            resp_b64 = base64.b64encode(resp_body).decode("utf-8")
            result = {
                "id": req_id,
                "status": response.status,
                "headers": dict(response.headers),
                "body": resp_b64,
                "type": "response",
            }
            return result
    except Exception as e:
        logger.error(f"HTTP Request failed: {e}")
        return {
            "id": req_id,
            "status": 502,
            "headers": {},
            "body": base64.b64encode(str(e).encode("utf-8")).decode("utf-8"),
            "type": "response",
        }


async def handle_connect(ws: aiohttp.ClientWebSocketResponse, data: dict):
    """
    Establishes a raw TCP connection for HTTPS tunneling (CONNECT).
    Expects data to include 'id' and 'url' (host[:port]).
    After connecting, stores writer in active_streams and spawns stream_reader to forward bytes.
    """
    req_id = data.get("id")
    target = data.get("url")
    logger.info(f"Opening Tunnel {req_id} to {target}")

    if not is_safe_target(target):
        logger.warning(f"Blocked Tunnel {req_id} to unsafe target {target}")
        await ws.send_json({"id": req_id, "type": "error", "message": "Access to local network is forbidden."})
        return

    try:
        if ":" in target:
            host, port_str = target.rsplit(":", 1)
            try:
                port = int(port_str)
            except ValueError:
                port = 443
        else:
            host = target
            port = 443

        reader, writer = await asyncio.open_connection(host, port)
        active_streams[req_id] = writer
        await ws.send_json({"id": req_id, "type": "connected"})
        asyncio.create_task(stream_reader(ws, req_id, reader))
    except Exception as e:
        logger.error(f"Tunnel connection failed to {target}: {e}")
        try:
            await ws.send_json({"id": req_id, "type": "error", "message": str(e)})
        except Exception:
            pass


async def stream_reader(ws: aiohttp.ClientWebSocketResponse, req_id: str, reader: asyncio.StreamReader):
    """
    Reads raw bytes from TCP and sends them to server via WS as base64 chunks.
    Cleans up on EOF or error.
    """
    try:
        while True:
            data = await reader.read(4096)
            if not data:
                break
            b64_data = base64.b64encode(data).decode("utf-8")
            try:
                await ws.send_json({"id": req_id, "type": "data", "body": b64_data})
            except Exception as e:
                logger.error(f"Failed sending data over ws for stream {req_id}: {e}")
                break
    except Exception as e:
        logger.error(f"Stream Reader Error {req_id}: {e}")
    finally:
        # Close and cleanup writer if present
        try:
            if req_id in active_streams:
                writer = active_streams[req_id]
                try:
                    writer.close()
                except Exception:
                    pass
                del active_streams[req_id]
        except Exception:
            pass

        try:
            await ws.send_json({"id": req_id, "type": "close"})
        except Exception:
            pass


async def run_client(token: str, email: str, server_url: str, status_callback=None, hwid: str = None):
    """
    Main client loop. Connects to server websocket and handles incoming requests.
    Will retry with exponential backoff on failure.
    """
    if server_url.startswith("http://"):
        ws_url = server_url.replace("http://", "ws://", 1)
    elif server_url.startswith("https://"):
        ws_url = server_url.replace("https://", "wss://", 1)
    else:
        ws_url = server_url

    if not ws_url.endswith("/ws"):
        ws_url = ws_url + "/ws"

    hostname = platform.node()
    system = f"{platform.system()} {platform.release()}"
    version = "2.0.1"

    ws_url = (
        f"{ws_url}?token={quote(str(token))}"
        f"&device_name={quote(str(hostname))}"
        f"&device_os={quote(str(system))}"
        f"&version={quote(str(version))}"
    )
    if email:
        ws_url += f"&email={quote(str(email))}"
    if hwid:
        ws_url += f"&hwid={quote(str(hwid))}"

    headers = {"User-Agent": f"Wizardgain/{version} ({system}; {platform.machine()})"}

    retry_delay = 5
    MAX_RETRY = 60

    if status_callback:
        status_callback("connecting")
    logger.info("Connecting to Wizardgain Network...")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                ssl_context = ssl.create_default_context(cafile=certifi.where())
                async with session.ws_connect(ws_url, headers=headers, ssl=ssl_context) as ws:
                    if status_callback:
                        status_callback("online")
                    logger.info("Connected to Server.")
                    retry_delay = 5

                    async for msg in ws:
                        # TEXT messages expected
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception as e:
                                logger.warning(f"Could not parse JSON message: {e}")
                                continue

                            req_id = data.get("id")
                            msg_type = data.get("type", "request")
                            method = data.get("method")

                            # If this req_id corresponds to an active stream (writer), handle data/close
                            if req_id in active_streams:
                                writer = active_streams[req_id]
                                if msg_type == "data":
                                    body = data.get("body")
                                    if body:
                                        try:
                                            chunk = base64.b64decode(body)
                                            writer.write(chunk)
                                            await writer.drain()
                                        except Exception as e:
                                            logger.error(f"Error writing to stream {req_id}: {e}")
                                elif msg_type == "close":
                                    try:
                                        writer.close()
                                    except Exception:
                                        pass
                                    active_streams.pop(req_id, None)
                                else:
                                    logger.debug(f"Unexpected msg_type for active stream {req_id}: {msg_type}")
                                continue

                            # Not an active stream - interpret requests
                            if method == "CONNECT":
                                # open TCP tunnel
                                asyncio.create_task(handle_connect(ws, data))
                            elif method:
                                # standard HTTP request
                                result = await handle_http_request(session, data)
                                try:
                                    await ws.send_json(result)
                                except Exception as e:
                                    logger.error(f"Failed sending HTTP response for {req_id}: {e}")
                            else:
                                # other message types for inactive streams
                                if msg_type == "close":
                                    logger.debug(f"Received close for inactive stream {req_id}")
                                elif msg_type == "data":
                                    logger.debug(f"Received data for inactive stream {req_id}")
                                else:
                                    logger.warning(f"Unknown message: {data}")

                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            logger.error("ws connection closed with exception %s", ws.exception())
                            break

                        elif msg.type == aiohttp.WSMsgType.CLOSE:
                            # connection closed by server
                            if ws.close_code == 4000:
                                logger.warning("Connection Closed: Duplicate connection detected (Replaced by another client with same Token).")
                            else:
                                logger.warning(f"Connection Closed: Code {ws.close_code}, Msg: {ws.close_message}")
                            break

                    # if we exit async for loop, mark offline and continue to retry
                    if status_callback:
                        status_callback("offline")

        except Exception as e:
            logger.error(f"Connection to server failed: {e}")
            if status_callback:
                status_callback("offline")

        logger.info(f"Retrying in {retry_delay} seconds...")
        try:
            await asyncio.sleep(retry_delay)
        except asyncio.CancelledError:
            break
        retry_delay = min(retry_delay * 2, MAX_RETRY)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Wizardgain SDK Client")
    parser.add_argument("--token", default=os.environ.get("TOKEN"), help="User authentication token (Node ID). If not provided, one is generated.")
    parser.add_argument("--email", default=os.environ.get("EMAIL"), help="User email address (required for auto-registration).")
    parser.add_argument("--url", default=os.environ.get("SERVER_URL", SERVER_URL), help="Server URL (default: use SERVER_URL var or hardcoded)")
    args = parser.parse_args()

    token = args.token
    stored_email = None

    # read stored email if exists
    if os.path.exists(".email"):
        try:
            with open(".email", "r") as f:
                stored_email = f.read().strip()
        except Exception:
            stored_email = None

    # If user changed email, reset token
    if args.email and stored_email and (args.email.lower() != stored_email.lower()):
        logger.info(f"User switched from {stored_email} to {args.email}. Generating NEW Node ID.")
        token = None

    # Load or generate token
    if not token:
        if os.path.exists(".token") and (not args.email or not stored_email or (args.email.lower() == (stored_email or "").lower() if args.email else False)):
            try:
                with open(".token", "r") as f:
                    token = f.read().strip()
            except Exception:
                token = None
        if not token:
            token = str(uuid.uuid4())
            try:
                with open(".token", "w") as f:
                    f.write(token)
                print(f"Generated new Node ID: {token}")
            except Exception:
                # couldn't write token file, still proceed with generated token
                print(f"Generated new Node ID (not saved to .token): {token}")

    # Save email if provided
    if args.email:
        try:
            with open(".email", "w") as f:
                f.write(str(args.email))
        except Exception:
            logger.warning("Could not write .email file")

    # require either email or token
    if not args.email and not args.token:
        print("Error: You must provide either a --token (if already registered) or --email (to auto-register).")
        sys.exit(1)

    try:
        asyncio.run(run_client(token, args.email, args.url))
    except KeyboardInterrupt:
        # graceful exit on Ctrl+C
        pass