import asyncio
import logging

from . import serde

logger = logging.getLogger(__name__)


async def status(receptor_object):
    doc = {}
    doc["config"] = receptor_object.config._parsed_args.__dict__
    while True:
        with open("diagnostics.json", "w") as fp:
            try:
                fp.write(serde.force_dumps(doc))
            except Exception:
                logger.exception("failed to dump")
        await asyncio.sleep(30)
