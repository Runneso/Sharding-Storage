import asyncio
import json
import logging
import uuid
from typing import Any

CONCURRENCY = 50
STATUS_OK = 'OK'
ENCODING = 'utf-8'
TIMEOUT = 5


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
    )


async def make_request(host: str, port: int, payload: dict[str, Any]) -> dict[str, Any] | None:
    reader, writer = await asyncio.open_connection(host, port)

    try:
        raw = json.dumps(payload, ensure_ascii=False).encode(ENCODING) + b'\n'
        writer.write(raw)
        await writer.drain()
        response_line = await asyncio.wait_for(reader.readline(), timeout=TIMEOUT)
        if not response_line:
            return None
        return json.loads(response_line.decode(ENCODING))
    finally:
        writer.close()
        await writer.wait_closed()


async def put_key(
        router_host: str,
        router_port: int,
) -> dict[str, Any] | None:
    request_id = str(uuid.uuid4())
    key = str(uuid.uuid4())

    payload = {
        'request_id': request_id,
        'type': 'CLIENT_PUT_REQUEST',
        'key': key,
        'value': request_id[:10],
    }

    return await make_request(router_host, router_port, payload)


async def main():
    setup_logging()
    logger = logging.getLogger()

    router_host = str(input('Enter router host: '))
    router_port = int(input('Enter router port: '))
    total_requests = int(input('Enter total requests: '))

    semaphore = asyncio.Semaphore(CONCURRENCY)
    success, failed = 0, 0

    async def worker():
        nonlocal success, failed

        async with semaphore:
            try:
                response = await put_key(router_host, router_port)

                if response is None:
                    failed += 1
                    return

                if response.get('status') == STATUS_OK:
                    success += 1
                else:
                    failed += 1
                    logger.error('bad response: %s', response)
            except Exception as error:
                failed += 1
                logger.error('bad exception: %s', error)

    tasks = [
        asyncio.create_task(worker())
        for _ in range(total_requests)
    ]

    await asyncio.gather(*tasks)

    logger.info('Script done')
    logger.info('Success: %d', success)
    logger.info('Failed: %d', failed)


if __name__ == '__main__':
    asyncio.run(main())
