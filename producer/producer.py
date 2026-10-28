"""
Producer: simula métricas de infraestructura (CPU y memoria) de varios
servidores y las publica en el tópico server_metrics cada 2 segundos.

Mensaje (JSON):
    {"timestamp": "...", "server_id": "srv-01", "metric_name": "cpu_usage", "value": 57.3}

Key del mensaje = server_id  ->  todas las métricas de un servidor van a la
misma partición, en orden.

Uso:
    python producer/producer.py
    python producer/producer.py --interval 1 --count 50
    python producer/producer.py --bad-rate 0.1      # 10 % de registros inválidos
"""

import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import KafkaException, Producer

# --- Configuración -----------------------------------------------------------
# Desde el host se usa el listener EXTERNAL del broker (localhost:9092).
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "server_metrics"
SERVERS = ["srv-01", "srv-02", "srv-03", "srv-04", "srv-05"]
METRICS = ["cpu_usage", "memory_usage"]
DEFAULT_INTERVAL_SECONDS = 2
DEFAULT_SPIKE_RATE = 0.2  # probabilidad de generar un pico > 80
DEFAULT_BAD_RATE = 0.0    # probabilidad de un registro inválido (para probar el filtrado en Spark)


def generate_value(metric_name: str, spike_rate: float = DEFAULT_SPIKE_RATE) -> float:
    """Valor sintético en %. Mayormente normal, con picos ocasionales > 80."""
    if random.random() < spike_rate:
        return round(random.uniform(80.5, 99.5), 1)  # pico: dispara alerta
    if metric_name == "cpu_usage":
        return round(random.uniform(15, 70), 1)
    return round(random.uniform(35, 75), 1)  # la memoria suele tener un piso más alto


def build_event(server_id: str, metric_name: str, spike_rate: float = DEFAULT_SPIKE_RATE) -> dict:
    """Arma el evento con timestamp, server_id, metric_name y value."""
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "server_id": server_id,
        "metric_name": metric_name,
        "value": generate_value(metric_name, spike_rate),
    }


def corrupt(event: dict) -> dict:
    """Simula un registro defectuoso: valor nulo, fuera de rango o sin server_id."""
    kind = random.choice(["null_value", "out_of_range", "missing_server"])
    if kind == "null_value":
        event["value"] = None
    elif kind == "out_of_range":
        event["value"] = 150.0
    else:
        event.pop("server_id")
    return event


def delivery_report(err, msg) -> None:
    """Callback: Kafka confirma (o no) que el mensaje quedó guardado.
    Muestra en qué partición y offset quedó."""
    if err is not None:
        print(f"[ERROR] No se pudo entregar el mensaje: {err}", file=sys.stderr)
        return
    event = json.loads(msg.value())
    value = event.get("value")
    shown = f"{value:5.1f}%" if isinstance(value, (int, float)) else f"{value!s:>6}"
    print(
        f"[ENVIADO] {event.get('server_id', '<sin id>')} {event['metric_name']:<12} = {shown}"
        f"  ->  partición {msg.partition()}  offset {msg.offset()}"
    )


def check_topic(producer: Producer) -> int:
    """Verifica que el tópico exista (no se autocrea) y devuelve sus particiones."""
    metadata = producer.list_topics(topic=TOPIC, timeout=10)
    topic = metadata.topics.get(TOPIC)
    if topic is None or topic.error is not None:
        raise SystemExit(
            f"El tópico '{TOPIC}' no existe en {BOOTSTRAP_SERVERS}. "
            "Levantá el clúster con 'docker compose up -d' (kafka-init lo crea con 3 particiones)."
        )
    return len(topic.partitions)


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer de métricas de servidores")
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL_SECONDS,
                        help="segundos entre mensajes (default: 2)")
    parser.add_argument("--count", type=int, default=0,
                        help="cantidad de mensajes a enviar (0 = infinito)")
    parser.add_argument("--spike-rate", type=float, default=DEFAULT_SPIKE_RATE,
                        help="probabilidad de un valor > 80 (default: 0.2)")
    parser.add_argument("--bad-rate", type=float, default=DEFAULT_BAD_RATE,
                        help="probabilidad de un registro inválido (default: 0)")
    args = parser.parse_args()

    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "metrics-producer",
        "acks": "all",  # el mensaje se da por enviado cuando el broker lo guardó
    })

    try:
        partitions = check_topic(producer)
    except KafkaException as exc:
        raise SystemExit(f"No se pudo conectar a Kafka en {BOOTSTRAP_SERVERS}: {exc}")

    print(f"Conectado a {BOOTSTRAP_SERVERS} | tópico '{TOPIC}' con {partitions} particiones")
    print(f"Enviando un mensaje cada {args.interval}s (Ctrl+C para detener)\n")

    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            server_id = random.choice(SERVERS)
            event = build_event(server_id, random.choice(METRICS), args.spike_rate)
            if random.random() < args.bad_rate:
                event = corrupt(event)
            producer.produce(
                TOPIC,
                key=server_id,  # misma key -> misma partición -> orden por servidor
                value=json.dumps(event).encode("utf-8"),
                on_delivery=delivery_report,
            )
            producer.poll(0)  # dispara los callbacks de entrega pendientes
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nDeteniendo producer...")
    finally:
        pending = producer.flush(10)  # no perder mensajes en vuelo
        print(f"Mensajes enviados: {sent} | pendientes sin confirmar: {pending}")


if __name__ == "__main__":
    main()
