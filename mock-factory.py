#!/usr/bin/env python3
"""
Script to just copy the mock JSON into a script output directory.

Uses $SCRIPT_OUTPUT
"""
import os
from pathlib import Path
import shutil
import time


source = Path(__file__).absolute().parent
meta = source / "factorystatsd-game-data.json"
samples = source / "factorystatsd-samples.json"
dest = Path(os.environ['SCRIPT_OUTPUT']).absolute()


shutil.copy(meta, dest)
while True:
    shutil.copy(samples, dest)
    time.sleep(1)
