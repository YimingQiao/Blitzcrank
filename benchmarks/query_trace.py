#!/usr/bin/env python3
"""Create shared little-endian u32 row IDs outside any query timer.

Zipf is a finite, unscrambled rank distribution with explicit theta; this is
not claimed to reproduce the paper's unspecified YCSB generator/settings.
"""
import argparse
from array import array
import bisect
import hashlib
import json
import math
from pathlib import Path
import random
import sys

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("rows", type=int)
p.add_argument("output", type=Path)
p.add_argument("--queries", type=int, default=1000000)
p.add_argument("--distribution", choices=["uniform", "zipf"], default="uniform")
p.add_argument("--theta", type=float, default=0.99)
p.add_argument("--seed", type=int, default=123456)
a = p.parse_args()
if not 0 < a.rows <= 0xffffffff or not 0 < a.queries <= 10000000 or not math.isfinite(a.theta) or a.theta < 0:
    p.error("invalid dimensions/theta")
ids = array("I")
assert ids.itemsize == 4
if a.distribution == "uniform":
    state = a.seed & 0xffffffffffffffff
    if state == 0:
        p.error("xorshift seed must be nonzero")
    for _ in range(a.queries):
        state ^= (state << 13) & 0xffffffffffffffff
        state ^= state >> 7
        state ^= (state << 17) & 0xffffffffffffffff
        ids.append(state % a.rows)
else:
    cdf = array("d")
    total = 0.0
    for rank in range(1, a.rows + 1):
        total += rank ** -a.theta
        cdf.append(total)
    rng = random.Random(a.seed)
    for _ in range(a.queries):
        ids.append(min(bisect.bisect_left(cdf, rng.random() * total), a.rows - 1))
if sys.byteorder != "little":
    ids.byteswap()
data = ids.tobytes()
with a.output.open("xb") as out:
    out.write(data)
metadata = {"rows": a.rows, "queries": a.queries, "distribution": a.distribution,
            "theta": a.theta if a.distribution == "zipf" else None, "seed": a.seed,
            "sha256": hashlib.sha256(data).hexdigest(), "format": "u32-le", "scrambled": False}
with Path(str(a.output) + ".json").open("x") as out:
    json.dump(metadata, out, indent=2)
print(json.dumps(metadata))
