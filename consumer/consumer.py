"""
Consumer: lee server_metrics como parte de un consumer group, imprime cada
evento (con partición y offset) y emite una alerta si value > 80.

Uso:
    python consumer/consumer.py                         # grupo por defecto: monitoring-alerts
    python consumer/consumer.py --name c2               # 2.º consumidor del MISMO grupo
    python consumer/consumer.py --group auditoria       # grupo distinto: recibe TODOS los mensajes
"""

import argparse
import json
import logging
import os

from confluent_kafka import Consumer, KafkaError

# --- Configuración -----------------------------------------------------------
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "server_metrics"
DEFAULT_GROUP_ID = "monitoring-alerts"
ALERT_THRESHOLD = 80.0

logging.basicConfig(format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S", level=logging.INFO)
log = logging.getLogger("consumer")


def on_assign(consumer, partitions) -> None:
    """Callback de rebalance: muestra qué particiones le tocaron a este consumidor."""
    ids = sorted(p.partition for p in partitions)
    log.info("REBALANCE | particiones asignadas a este consumidor: %s", ids or "ninguna (consumidor ocioso)")


def on_revoke(consumer, partitions) -> None:
    """Callback de rebalance: el grupo cambió y se le quitan particiones."""
    ids = sorted(p.partition for p in partitions)
    if ids:
        log.info("REBALANCE | particiones revocadas: %s", ids)


def process(event: dict, partition: int, offset: int, threshold: float = ALERT_THRESHOLD) -> bool:
    """Imprime el evento; si value > threshold, imprime una alerta destacada.
    Devuelve True si se generó una alerta."""
    server, metric, value = event["server_id"], event["metric_name"], float(event["value"])
    if not 0 <= value <= 100:
        raise ValueError(f"valor fuera de rango: {value}")
    origin = f"[p{partition} | off {offset}]"

    if value > threshold:
        log.warning(
            "%s *** ALERTA *** %s: %s en %.1f%% supera el umbral de %.0f%% (%s)",
            origin, server, metric, value, threshold, event["timestamp"],
        )
        return True

    log.info("%s %s %-12s = %5.1f%%", origin, server, metric, value)
    return False


def main() -> None:
    parser = argparse.ArgumentParser(description="Consumer de métricas con alertas")
    parser.add_argument("--group", default=DEFAULT_GROUP_ID, help="group.id (default: monitoring-alerts)")
    parser.add_argument("--name", default="c1", help="nombre del consumidor para identificarlo en los logs")
    parser.add_argument("--threshold", type=float, default=ALERT_THRESHOLD, help="umbral de alerta en %% (default: 80)")
    args = parser.parse_args()

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": args.group,
        "client.id": f"{args.group}-{args.name}",
        # Un grupo NUEVO arranca desde el principio del tópico: Kafka no borra
        # los mensajes al leerlos. Un grupo existente sigue desde su último offset.
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([TOPIC], on_assign=on_assign, on_revoke=on_revoke)

    log.info("Consumidor '%s' del grupo '%s' escuchando '%s' en %s (Ctrl+C para salir)",
             args.name, args.group, TOPIC, BOOTSTRAP_SERVERS)

    processed = alerts = 0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error("Error de Kafka: %s", msg.error())
                continue
            try:
                event = json.loads(msg.value())
            except (json.JSONDecodeError, TypeError):
                log.error("[p%s | off %s] Mensaje inválido, se descarta: %r",
                          msg.partition(), msg.offset(), msg.value())
                continue
            try:
                alerts += process(event, msg.partition(), msg.offset(), args.threshold)
                processed += 1
            except (KeyError, TypeError, ValueError):
                log.error("[p%s | off %s] Registro incompleto o inválido, se descarta: %s",
                          msg.partition(), msg.offset(), event)
    except KeyboardInterrupt:
        pass
    finally:
        # Sale del grupo de forma ordenada: el resto del grupo absorbe sus
        # particiones de inmediato (rebalance).
        consumer.close()
        log.info("Consumidor cerrado | procesados: %d | alertas: %d", processed, alerts)


if __name__ == "__main__":
    main()
