"""
Reads incoming data.
"""
import json
import logging
from pathlib import Path
import time
from typing import Iterable

__all__ = ('read_factorio',)

LOG = logging.getLogger(__name__)


def _read_and_nuke(file: Path):
    data = json.loads(file.read_text())
    file.unlink()
    return data


def read_factorio(script_output: Path) -> Iterable[tuple[str, dict]]:
    """
    Reads the data from the in-game factorystatsd mod.

    Generates a tuple of:
    * kind: string of either "meta" or "samples"
    * data: The actual blob
    """
    meta_path = script_output / 'factorystatsd-game-data.json'
    samples_path = script_output / 'factorystatsd-samples.json'

    last_meta_mod_time = 0

    while True:
        try:
            try:
                meta_mod_time = meta_path.stat().st_mtime
            except FileNotFoundError:
                pass
            else:
                if meta_mod_time > last_meta_mod_time:
                    LOG.info('loading game data')
                    yield 'meta', json.loads(meta_path.read_text())
                    last_meta_mod_time = meta_mod_time

            try:
                samples = _read_and_nuke(samples_path)
            except FileNotFoundError:
                LOG.debug('No samples data yet')
            else:
                LOG.info('loaded samples data')
                yield "samples", samples
        except Exception as e:
            LOG.exception(f'forwarder exception: {e}')
            time.sleep(1.0)
        else:
            time.sleep(0.1)
