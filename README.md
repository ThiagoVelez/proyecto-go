# 🚀 GopherQueue: Concurrent Task Queue & Worker Pool Engine

Un **motor de colas de tareas en segundo plano de alto rendimiento** construido en **Go (Golang)**, diseñado para procesar trabajos asíncronos y pesados con control estricto de concurrencia, prioridades dinámicas, reintentos con *exponential backoff*, *graceful shutdown* y un panel de control interactivo en tiempo real.

---

## 🏛️ Arquitectura del Sistema

```
                    ┌────────────────────────┐
                    │      Cliente HTTP      │
                    │ (Dashboard Web o REST) │
                    └───────────┬────────────┘
                                │ POST /api/tasks
                                ▼
                    ┌────────────────────────┐
                    │    HTTP API Server     │
                    └───────────┬────────────┘
                                │ Enqueue
                                ▼
                    ┌────────────────────────┐
                    │ Priority Queue (Heap)  │
                    │   [HIGH > MED > LOW]   │
                    └───────────┬────────────┘
                                │ sync.Cond / Channel
                                ▼
            ┌───────────────────────────────────────────────┐
            │             Worker Pool Engine                │
            │  ┌──────────────┐ ┌──────────────┐ ┌────────┐ │
            │  │ Worker Gorout│ │ Worker Gorout│ │ Worker │ │
            │  └──────┬───────┘ └──────┬───────┘ └────┬───┘ │
            └─────────┼────────────────┼──────────────┼─────┘
                      ▼                ▼              ▼
            ┌───────────────────────────────────────────────┐
            │               Task Handlers                   │
            │  (Email, Resizing, Reports, Webhooks, Flaky)  │
            └──────────────────────┬────────────────────────┘
                                   │
              ┌────────────────────┴────────────────────┐
              │                                         │
        [Éxito / OK]                             [Fallo / Error]
              ▼                                         ▼
   ┌───────────────────────┐            ┌───────────────────────────────┐
   │ Task Store & Metrics  │            │  ¿Reintentos < MaxRetries?    │
   └───────────────────────┘            ├──────────────┬────────────────┤
                                        │ (Sí)         │ (No)           │
                                        ▼              ▼                ▼
                                 Exponential       Dead Letter Queue
                                 Backoff Requeue   (Estado FAILED)
```

---

## 💡 Patrones de Concurrencia de Go Implementados

1. **Priority Queue con Heap Min:** Cola thread-safe basada en `container/heap` protegida con `sync.Mutex` y sincronización con `sync.Cond` para desbloquear workers sin consumo de CPU inútil (*no busy waiting*).
2. **Worker Pool Dinámico:** Conjunto configurable de goroutines trabajadoras que consumen tareas en paralelo, con capacidad de escalado en caliente (`POST /api/workers/scale`).
3. **Control de Timeout por Tarea:** Uso de `context.WithTimeout` para asegurar que tareas colgadas o lentas no bloqueen los workers indefinidamente.
4. **Recuperación ante Pánicos (`recover`):** Aislamiento de errores fatales dentro de los handlers para que el motor no caiga si una tarea genera un panic.
5. **Reintentos con Exponential Backoff y Jitter:** Tareas fallidas se reprograman automáticamente con demoras exponenciales ($2^{\text{intento}} \text{ seg}$) + aleatoriedad para evitar tormentas de reintentos (*thundering herd*).
6. **Graceful Shutdown:** Intercepción de señales `SIGINT` (Ctrl+C) y `SIGTERM` para permitir que las tareas en curso finalicen limpiamente antes de cerrar el proceso (`sync.WaitGroup`).
7. **Métricas Atómicas:** Uso de `sync/atomic` para contadores de alto rendimiento y bajo overhead.

---

## 📁 Estructura del Código

```
GO/
├── cmd/
│   ├── server/               # Punto de entrada principal (Servidor HTTP + Worker Pool)
│   │   └── main.go
│   └── cli/                  # CLI de benchmarking y pruebas de carga concurrente
│       └── main.go
├── internal/
│   ├── task/                 # Definición del modelo Task, estados y prioridades
│   │   └── task.go
│   ├── queue/                # Cola de prioridades concurrente con Heap y sync.Cond
│   │   ├── priority_queue.go
│   │   └── priority_queue_test.go
│   ├── pool/                 # Gestor del Worker Pool, ciclo de vida y escalado
│   │   ├── pool.go
│   │   ├── worker.go
│   │   └── pool_test.go
│   ├── handlers/             # Handlers ejecutores de tareas (email, resize, reportes)
│   │   └── registry.go
│   ├── store/                # Almacén en memoria thread-safe y agregador de métricas
│   │   └── store.go
│   └── api/                  # Endpoints REST y servidor de archivos web
│       └── server.go
├── web/                      # Dashboard interactivo en tiempo real (Dark Mode)
│   ├── index.html
│   ├── app.js
│   └── styles.css
├── go.mod
└── README.md
```

---

## ⚡ Guía de Inicio Rápido

### 1. Iniciar el Servidor

```bash
# Iniciar con configuración por defecto (Puerto 8080, 4 workers iniciales)
go run cmd/server/main.go

# O personalizar el puerto y workers:
go run cmd/server/main.go -port 8080 -workers 8
```

Abre tu navegador en **`http://localhost:8080`** para acceder al panel de control interactivo.

---

### 2. Ejecutar la Herramienta CLI de Benchmark

En otra terminal, puedes enviar ráfagas de tareas concurrentes:

```bash
# Enviar 100 tareas con 10 clientes concurrentes
go run cmd/cli/main.go -n 100 -c 10 -type quick_job

# Enviar 20 tareas pesadas de reporte
go run cmd/cli/main.go -n 20 -c 5 -type generate_report
```

---

### 3. Ejecutar Pruebas con el Detector de Condiciones de Carrera (Race Detector)

```bash
# Correr suite de pruebas con detección de data races
go test -v -race ./...
```

---

## 🔌 Referencia de la API REST

| Método | Endpoint | Descripción |
|---|---|---|
| `POST` | `/api/tasks` | Encolar una nueva tarea individual |
| `POST` | `/api/tasks/batch` | Encolar un lote de tareas para pruebas de carga |
| `GET` | `/api/tasks` | Listar tareas recientes (soporta `?limit=50&status=COMPLETED`) |
| `GET` | `/api/tasks/{id}` | Obtener detalle de una tarea específica |
| `GET` | `/api/metrics` | Obtener snapshot de métricas del sistema |
| `GET` | `/api/workers` | Obtener estado y tareas activas de cada worker |
| `POST` | `/api/workers/scale` | Escalar el número de workers (`{"count": 8}`) |
| `GET` | `/api/task-types` | Listar tipos de tareas registradas |

### Ejemplo: Encolar tarea por cURL

```bash
curl -X POST http://localhost:8080/api/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "type": "send_email",
    "priority": 1,
    "payload": {
      "to": "dev@example.com",
      "subject": "Notificación de Servidor"
    },
    "timeout_seconds": 10,
    "max_retries": 3
  }'
```
