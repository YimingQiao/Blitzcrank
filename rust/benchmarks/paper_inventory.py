"""Audit local paper-data candidates without modifying the source checkout.

python3 paper_inventory.py /absolute/Blitzcrank/playground > inventory.json
Physical line counts are not silently relabeled as logical CSV record counts.
"""
import csv
import hashlib
import json
from pathlib import Path
import sys

root = Path(sys.argv[1])
paper = [
    ("Corel", 68040, 93, None),
    ("Jena Climate", 420551, 14, "jena_climate"),
    ("Cars", 344287, 155, "cps"),
    ("Forest Cover", 581012, 55, "covtype"),
    ("US Census 1990", 2458285, 69, "USCensus1990"),
    ("Food", 5216593, 5, "Food"),
    ("Bimbo", 20259279, 12, "Bimbo_1"),
    ("Yale Languages", 5762082, 30, "YaleLanguages"),
    ("Medicare", 8645072, 26, "Medicare1_1"),
    ("Arade", 9888775, 11, "Arade_1"),
]

def audit(stem):
    data = root / "tables" / f"{stem}.dat"
    config = root / "config" / f"{stem}.config"
    if not data.is_file() or not config.is_file():
        return {"status": "missing", "data": str(data), "config": str(config)}
    digest = hashlib.sha256()
    physical_lines = 0
    last_byte = b""
    with data.open("rb") as source:
        while chunk := source.read(8 << 20):
            digest.update(chunk)
            physical_lines += chunk.count(b"\n")
            last_byte = chunk[-1:]
    if last_byte and last_byte != b"\n":
        physical_lines += 1
    schema = [line.split() for line in config.read_text().splitlines() if line.strip()]
    types = {}
    for field in schema:
        types[field[0]] = types.get(field[0], 0) + 1
    delimiter = "|" if stem == "Medicare1_1" else ","
    with data.open(newline="", encoding="utf-8", errors="replace") as source:
        sample = next(csv.reader(source, delimiter=delimiter))
    return {"status": "present", "data": str(data), "config": str(config),
            "bytes": data.stat().st_size, "sha256": digest.hexdigest(),
            "physical_lines": physical_lines, "delimiter": delimiter, "sample_columns": len(sample),
            "schema_columns": len(schema), "field_types": types,
            "config_sha256": hashlib.sha256(config.read_bytes()).hexdigest()}

result = []
for name, rows, columns, stem in paper:
    item = {"paper_name": name, "paper_rows": rows, "paper_columns": columns}
    if stem:
        item.update(audit(stem))
        item["dimension_match"] = (item.get("physical_lines"), item.get("sample_columns"), item.get("schema_columns")) == (rows, columns, columns)
        if name == "Cars":
            item["identity"] = "UNCONFIRMED: cps matches dimensions; do not infer identity from dimensions alone"
    else:
        item["status"] = "no_verified_merged_file"
        item["components"] = [audit(s) for s in ("ColorMoments", "ColorHistogram", "CoocTexture", "LayoutHistogram")]
    result.append(item)
print(json.dumps({"source": "https://www.vldb.org/pvldb/vol17/p2528-zhang.pdf", "datasets": result}, indent=2))
