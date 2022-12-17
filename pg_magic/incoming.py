"""
Reads incoming data.
"""
import json
import logging
from pathlib import Path
import time
from typing import Iterable, Tuple

__all__ = ('read_factorio',)

LOG = logging.getLogger(__name__)


def _read_and_nuke(file: Path):
    data = json.loads(file.read_text())
    file.unlink()
    return data


class Checker:
    def __init__(self, path):
        self.path = Path(path)
        self.last_mtime = 0

    def __call__(self):
        try:
            mod_time = self.path.stat().st_mtime
        except FileNotFoundError:
            return
        else:
            if mod_time > self.last_mtime:
                self.last_mtime = mod_time
                return json.loads(self.path.read_text())


def read_factorio(script_output: Path) -> Iterable[Tuple[str, dict]]:
    """
    Reads the data from the in-game factorystatsd mod.

    Generates a tuple of:
    * kind: string of either "meta" or "samples"
    * data: The actual blob
    """
    meta = Checker(script_output / 'factorystatsd-game-data.json')
    samples = Checker(script_output / 'factorystatsd-samples.json')

    while True:
        try:
            meta_data = meta()
            if meta_data is not None:
                LOG.info('loaded game data')
                yield 'meta', meta_data

            sample_data = samples()
            if sample_data is not None:
                LOG.info('loaded samples data')
                yield "samples", sample_data

        except Exception as e:
            LOG.exception(f'forwarder exception: {e}')
            time.sleep(1.0)
        else:
            time.sleep(0.1)
