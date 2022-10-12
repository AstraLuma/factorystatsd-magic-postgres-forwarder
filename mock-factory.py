#!/usr/bin/env python3
"""
Script to just copy the mock JSON into a script output directory.

Uses $SCRIPT_OUTPUT
"""
import itertools
import json
import os
from pathlib import Path
import shutil
import time


source = Path(__file__).absolute().parent
meta = source / "factorystatsd-game-data.json"
sample_data = json.loads((source / "factorystatsd-samples.json").read_text())
dest = Path(os.environ['SCRIPT_OUTPUT']).absolute()

print("Emitting metadata", flush=True)
shutil.copy(meta, dest)
for tick in itertools.count(0, 60):
    sample_data['ticks'] = tick
    (dest / 'factorystatsd-samples.json').write_text(json.dumps(sample_data))
    time.sleep(1)
