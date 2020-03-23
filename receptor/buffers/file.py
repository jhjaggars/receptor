import asyncio
import datetime
import logging
import os
import uuid
from collections import defaultdict
from json.decoder import JSONDecodeError

from .. import fileio
from .. import serde as json

logger = logging.getLogger(__name__)


class DurableBuffer:
    def __init__(self, dir_, key, loop, write_time=1.0):
        self.q = asyncio.Queue()
        self._base_path = os.path.join(os.path.expanduser(dir_))
        self._message_path = os.path.join(self._base_path, "messages")
        self._manifest_path = os.path.join(self._base_path, f"manifest-{key}")
        self._loop = loop
        self._manifest_lock = asyncio.Lock(loop=self._loop)
        self._manifest_dirty = False
        self._write_time = write_time
        self.ready = asyncio.Event()
        self._loop.create_task(self.start_manifest())

    async def start_manifest(self):
        try:
            os.makedirs(self._message_path, mode=0o700)
        except Exception:
            pass

        loaded_items = await self._read_manifest()
        for item in loaded_items:
            await self.q.put(item)

        self.ready.set()
        self._loop.create_task(self.manifest_writer(self._write_time))

    async def put(self, framed_message):
        await self.ready.wait()
        path = os.path.join(self._message_path, str(uuid.uuid4()))
        item = {
            "path": path,
            "expire_time": datetime.datetime.utcnow() + datetime.timedelta(minutes=5),
        }
        async with fileio.Opened(path, "wb") as fp:
            if isinstance(framed_message, bytes):
                await fp.write(framed_message)
            else:
                for chunk in framed_message:
                    await fp.write(chunk)

        await self.put_ident(item)

    async def put_ident(self, ident):
        await self.q.put(ident)
        self._manifest_dirty = True

    async def get(self):
        await self.ready.wait()
        while True:
            ident = await self.q.get()
            self._manifest_dirty = True
            file_path = ident["path"]
            try:
                f = await fileio.aopen(file_path, "rb")
                return (ident, f)
            except (FileNotFoundError, TypeError):
                pass

    async def _read_manifest(self):
        try:
            async with fileio.Opened(self._manifest_path, "r") as fp:
                data = await fp.read()
        except FileNotFoundError:
            return []
        else:
            try:
                return json.loads(data)
            except JSONDecodeError:
                logger.error("failed to decode manifest: %s", data)
            except Exception:
                logger.exception("Unknown failure in decoding manifest: %s", data)
            finally:
                return []

    def _remove_path(self, path):
        if os.path.exists(path):
            os.remove(path)
        else:
            logger.info("Can't remove {}, doesn't exist".format(path))

    async def expire(self):
        async with self._manifest_lock:
            old, self.q = self.q, asyncio.Queue()
            while old.qsize() > 0:
                item = await old.get()
                path = item["path"]
                expire_time = item["expire_time"]
                if expire_time < datetime.datetime.utcnow():
                    logger.info("Expiring message %s", path)
                    await fileio.run_in_executor(self._remove_path, path)
                else:
                    await self.q.put(item)
            self._manifest_dirty = True

    async def manifest_writer(self, write_time):
        while True:
            if self._manifest_dirty:
                async with self._manifest_lock:
                    try:
                        async with fileio.Opened(self._manifest_path, "w") as fp:
                            await fp.write(json.dumps(list(self.q._queue)))
                        self._manifest_dirty = False
                    except Exception:
                        logger.exception("Failed to write manifest for %s", self._manifest_path)
            await asyncio.sleep(write_time)


class FileBufferManager(defaultdict):
    def __init__(self, path, loop=asyncio.get_event_loop()):
        self.path = path
        self.loop = loop

    def __missing__(self, key):
        self[key] = DurableBuffer(self.path, key, self.loop)
        return self[key]
