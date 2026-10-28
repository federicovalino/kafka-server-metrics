"""
file_feeder.py — Simulador alternativo a Kafka para el procesador de Spark.

Escribe un archivo JSON por evento en una carpeta (por defecto spark/input),
con el mismo formato que el producer. Sirve para probar
streaming_processor.py con --source files sin levantar Kafka.

Uso:
    python spark/file_feeder.py --count 60 --interval 1
"""

import argparse
import json
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

SERVERS = ["srv-01", "srv-02", "srv-03", "srv-04", "srv-05"]
METRICS = ["cpu_usage", "memory_usage"]


def build_event(bad_rate: float) -> dict:
    event = {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "server_id": random.choice(SERVERS),
        "metric_name": random.choice(METRICS),
        "value": round(random.uniform(80.5, 99.5) if random.random() < 0.2 else random.uniform(15, 75), 1),
    }
    if random.random() < bad_rate:  # registros inválidos para probar el filtrado
        random.choice([
            lambda e: e.update(value=None),
            lambda e: e.update(value=150.0),
            lambda e: e.pop("server_id"),
        ])(event)
    return event


def main() -> None:
    parser = argparse.ArgumentParser(description="Genera eventos como archivos JSON")
    parser.add_argument("--out-dir", default="spark/input")
    parser.add_argument("--count", type=int, default=60)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--bad-rate", type=float, default=0.1)
    args = parser.parse_args()

    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for i in range(args.count):
        event = build_event(args.bad_rate)
        tmp = out / f".{uuid.uuid4().hex}.tmp"
        tmp.write_text(json.dumps(event), encoding="utf-8")
        tmp.rename(out / f"event-{uuid.uuid4().hex}.json")  # escritura atómica: Spark nunca lee un archivo a medias
        print(f"[{i + 1}/{args.count}] {event}")
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
