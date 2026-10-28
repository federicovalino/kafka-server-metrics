# Monitoreo de métricas de infraestructura en tiempo real con Apache Kafka

Sistema de mensajería asincrónica que simula el monitoreo de servidores. Un **producer** en Python publica métricas sintéticas de CPU y memoria en el tópico `server_metrics`, de **3 particiones**, en un clúster Kafka local con Docker. Un **consumer**, parte de un consumer group, las procesa en tiempo real y emite una **alerta** cuando un valor supera el 80 %.

## Arquitectura

![Arquitectura de la solución](docs/arquitectura.png)

**Flujo:**

1. `docker compose up` levanta el broker en modo **KRaft**. El servicio `kafka-init` espera a que esté sano y crea `server_metrics` con 3 particiones **antes** de que exista cualquier producer.
2. El **producer** publica un JSON cada 2 segundos con `server_id` como **key**. Kafka asigna la partición a partir de esa key, así que todas las métricas de un mismo servidor quedan en la misma partición y en orden.
3. Los **consumers** del grupo `monitoring-alerts` se reparten las particiones. Cada partición la lee un solo consumidor del grupo.
4. Un consumer de **otro grupo** (`auditoria`) recibe todos los mensajes, con su propio offset, sin afectar al primer grupo.
5. Si `value > 80`, el consumer emite un log de nivel `WARNING` con la alerta.

## Estructura

```
.
├── docker-compose.yml      # broker KRaft + kafka-init (crea el tópico) + kafka-ui
├── producer/
│   └── producer.py         # genera métricas y las publica cada 2 s
├── consumer/
│   └── consumer.py         # consumer group + alerta si value > 80
├── requirements.txt        # confluent-kafka
├── docs/                   # diagrama y capturas
└── README.md
```

## Formato del mensaje

```json
{
  "timestamp": "2026-10-27T02:00:51+00:00",
  "server_id": "srv-01",
  "metric_name": "cpu_usage",
  "value": 62.7
}
```

| Campo | Valores |
|---|---|
| `timestamp` | Momento de la medición, ISO 8601 en UTC |
| `server_id` | `srv-01` … `srv-05`. También es la **key** del mensaje |
| `metric_name` | `cpu_usage` o `memory_usage` |
| `value` | Porcentaje de uso. Mayormente entre 15 % y 75 %, con picos de 80 % a 99 % en ~20 % de los mensajes |

## Decisiones de diseño

| Decisión | Por qué |
|---|---|
| **KRaft en lugar de Zookeeper** | Kafka 4 eliminó Zookeeper. En KRaft el mismo proceso cumple los roles de broker y controller, con menos componentes y menos configuración |
| **Tópico creado por `kafka-init`, con `auto.create.topics = false`** | Evita el error común de que el producer autocree el tópico con 1 sola partición. Si el tópico no existe, el producer falla con un mensaje claro en lugar de crear uno mal configurado |
| **3 particiones, replication factor 1** | 3 particiones permiten hasta 3 consumidores en paralelo dentro de un grupo. RF 1 porque hay un solo broker: no hay dónde replicar |
| **Key = `server_id`** | Kafka garantiza el orden solo dentro de una partición. Con esta key, las métricas de cada servidor llegan en orden, y 5 servidores reparten la carga entre las 3 particiones |
| **Dos listeners: `EXTERNAL` y `INTERNAL`** | El broker le devuelve a cada cliente la dirección por la que tiene que seguir hablando (`advertised.listeners`). Los scripts del host usan `localhost:9092` y los contenedores usan `kafka:29092`. Con un solo listener, uno de los dos lados recibiría una dirección inalcanzable (el error de `bootstrap.servers`) |
| **`acks = all` en el producer** | Un mensaje se da por enviado solo cuando el broker confirmó que lo guardó. El callback muestra la partición y el offset asignados |
| **`auto.offset.reset = earliest` en el consumer** | Un grupo nuevo lee desde el principio del tópico. Muestra que Kafka **no borra** los mensajes al leerlos: cada grupo lleva su propio offset |
| **Alerta como log `WARNING`** | Las alertas se distinguen de los eventos normales (`INFO`) y se pueden filtrar o redirigir sin tocar la lógica |

## Requisitos

- Docker Desktop
- Python 3.10 o superior

## Ejecución

### 1. Levantar el clúster

```bash
docker compose up -d
docker compose ps
```

`kafka` y `kafka-ui` quedan en estado `running`. `kafka-init` termina con código 0 después de crear el tópico. Para ver lo que hizo:

```bash
docker compose logs kafka-init
```

```
Created topic server_metrics.
Topic: server_metrics  TopicId: ...  PartitionCount: 3  ReplicationFactor: 1  Configs:
        Topic: server_metrics  Partition: 0  Leader: 1  Replicas: 1  Isr: 1
        Topic: server_metrics  Partition: 1  Leader: 1  Replicas: 1  Isr: 1
        Topic: server_metrics  Partition: 2  Leader: 1  Replicas: 1  Isr: 1
```

### 2. Instalar dependencias de Python

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows (PowerShell)
# source .venv/bin/activate     # Linux / macOS
pip install -r requirements.txt
```

### 3. Ejecutar el producer (terminal 1)

```bash
python producer/producer.py
```

Opciones: `--interval 1` (segundos entre mensajes), `--count 50` (detenerse después de N mensajes), `--spike-rate 0.5` (más alertas).

### 4. Ejecutar el consumer (terminal 2)

```bash
python consumer/consumer.py
```

> Los scripts se conectan a `localhost:9092` por defecto. Se puede cambiar con la variable de entorno `KAFKA_BOOTSTRAP_SERVERS`.

## Flujo de datos

### Producer

Cada mensaje se confirma con la partición y el offset donde quedó guardado:

```
Conectado a localhost:9092 | tópico 'server_metrics' con 3 particiones
Enviando un mensaje cada 2.0s (Ctrl+C para detener)

[ENVIADO] srv-03 cpu_usage    =  42.8%  ->  partición 1  offset 0
[ENVIADO] srv-01 memory_usage =  91.3%  ->  partición 2  offset 0
[ENVIADO] srv-04 cpu_usage    =  27.5%  ->  partición 0  offset 0
```

![Producer enviando métricas](docs/producer.png)

### Consumer con alertas

```
10:15:02 INFO    REBALANCE | particiones asignadas a este consumidor: [0, 1, 2]
10:15:04 INFO    [p1 | off 0] srv-03 cpu_usage    =  42.8%
10:15:06 WARNING [p2 | off 0] *** ALERTA *** srv-01: memory_usage en 91.3% supera el umbral de 80% (2026-10-27T13:15:06+00:00)
10:15:08 INFO    [p0 | off 0] srv-04 cpu_usage    =  27.5%
```

![Consumer procesando y emitiendo alertas](docs/consumer-alertas.png)

### Kafka UI

En `http://localhost:8080` → *Topics* → `server_metrics` se ven las 3 particiones con sus mensajes y offsets. En *Consumers* se ven los grupos, qué consumidor tiene cada partición y el atraso (*lag*).

![Tópico server_metrics en Kafka UI](docs/kafka-ui-topic.png)

## Consumer groups: paralelismo y rebalance

### 1. Dos consumidores del mismo grupo se reparten las particiones

Con el producer corriendo, en dos terminales:

```bash
python consumer/consumer.py --name c1
python consumer/consumer.py --name c2
```

Al sumarse `c2`, Kafka hace un **rebalance** y cada uno informa sus particiones:

```
c1:  REBALANCE | particiones asignadas a este consumidor: [0, 1]
c2:  REBALANCE | particiones asignadas a este consumidor: [2]
```

Cada mensaje lo procesa **un solo** consumidor del grupo. Para verlo desde el broker:

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group monitoring-alerts
```

La columna `CONSUMER-ID` muestra qué consumidor tiene cada partición, y `LAG` cuántos mensajes le faltan procesar.

![Dos consumidores del mismo grupo](docs/consumer-groups.png)

> Con 3 particiones, un **4.º consumidor** del mismo grupo queda ocioso (`particiones asignadas: ninguna`). Las particiones son el techo del paralelismo de un grupo.

### 2. Tolerancia a fallos: un consumidor se cae

Detener `c2` con `Ctrl+C`. `c1` recibe un nuevo rebalance, absorbe la partición 2 y sigue procesando desde el último offset confirmado del grupo, sin perder mensajes:

```
c1:  REBALANCE | particiones asignadas a este consumidor: [0, 1, 2]
```

### 3. Un grupo distinto recibe todos los mensajes

```bash
python consumer/consumer.py --group auditoria --name c3
```

`c3` recibe las 3 particiones y, por `auto.offset.reset = earliest`, lee **desde el principio** todo lo publicado, incluso lo que `monitoring-alerts` ya procesó. Los dos grupos consumen los mismos datos de forma independiente, cada uno con su propio offset.

## Errores comunes y cómo se evitan

| Error | Qué pasaría | Cómo se evita en este proyecto |
|---|---|---|
| No crear el tópico antes del producer | Kafka lo autocrea con 1 partición: sin paralelismo | `kafka-init` crea el tópico con 3 particiones y `auto.create.topics = false` impide la autocreación |
| `bootstrap.servers` mal configurado | El cliente no conecta, o conecta y después falla con una dirección inalcanzable | Listeners `EXTERNAL` (host) e `INTERNAL` (Docker) separados. El producer verifica la conexión y el tópico al arrancar y corta con un mensaje claro |

## Limpieza

```bash
docker compose down        # detiene y elimina los contenedores (los datos del tópico se pierden)
```
