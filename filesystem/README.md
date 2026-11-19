# Filesystem KV-cache evictor

This backend is a Linux Go service. It watches a cache on a mounted filesystem, removes cold `.bin` blocks when the filesystem reaches a high-water mark, and stops when usage falls below a low-water mark. It can also remove empty cache directories and publish `BlockRemoved` events over ZMQ.

The service does not create or mount a PersistentVolumeClaim. The workload must provide the mount and cache root; the service waits for them to appear.

## Current scope

This implementation is customized for vLLM's filesystem cache layout,
metadata, and storage-event contract. SGLang, Mooncake, and other cache
formats are not supported yet; add their layout and event rules as separate
backend or adapter work.

## Code layout

```text
filesystem/
├── cmd/
│   └── pvc-evictor/       Service entry point
├── deploy/                Kustomize and Helm deployment flavors
└── internal/
    ├── cleaner/           Empty-directory cleanup
    ├── config/            Flags, environment variables, and validation
    ├── crawler/           Cache scanning and queueing
    ├── deleter/           File deletion and optional event publishing
    ├── disk/              Linux filesystem usage
    ├── events/            ZMQ storage-event publishing
    ├── filemeta/          File and directory metadata
    └── pending/           Paths waiting for deletion
```

## Cache layout

The scanner handles these cache shapes:

```text
<cache-root>/<model>_r<rank>/<hex-bucket>/<block-bucket>/*.bin
<cache-root>/<model>/<rank>/<dtype>/<hex-bucket>/<block-bucket>/*.bin
```

The first hex bucket is assigned to crawler workers by its value modulo 16.
`HEX_BUCKET_LEN` must match the first-bucket name length used by the cache and must be between 2 and 4.
The default is 3.

Files whose access time is newer than `FILE_ACCESS_TIME_THRESHOLD_MINUTES` are protected from deletion.

## Run locally

Run these commands from the repository root:

```sh
make build
./filesystem/bin/pvc-evictor
```

The default mount path is `/kv-cache`, and the default cache root is `/kv-cache/kv/model-cache/models`. Override settings with command-line flags or environment variables. Environment variables take precedence over flags.

For example:

```sh
PVC_MOUNT_PATH=/mnt/kv-cache \
CACHE_DIRECTORY=models \
CLEANUP_THRESHOLD=85 \
TARGET_THRESHOLD=70 \
./filesystem/bin/pvc-evictor
```

The service waits up to 60 seconds for the mount path and up to 5 minutes for the cache root.
It exits if either path does not appear.

## Settings

| Environment variable | Flag | Default | Purpose |
| --- | --- | ---: | --- |
| `PVC_MOUNT_PATH` | `--pvc-mount-path` | `/kv-cache` | Mounted filesystem path |
| `CACHE_DIRECTORY` | `--cache-directory` | `kv/model-cache/models` | Relative cache path below the mount |
| `CLEANUP_THRESHOLD` | `--cleanup-threshold` | `85` | Usage percentage that starts deletion |
| `TARGET_THRESHOLD` | `--target-threshold` | `70` | Usage percentage that stops deletion |
| `DRY_RUN` | `--dry-run` | `false` | Keep files and log deletion batches |
| `LOG_LEVEL` | `--log-level` | `INFO` | `DEBUG`, `INFO`, `WARNING`, or `ERROR` |
| `NUM_CRAWLER_PROCESSES` | `--num-crawler-processes` | `8` | Crawler workers; must be a power of two from 1 to 16 |
| `LOGGER_INTERVAL_SECONDS` | `--logger-interval-seconds` | `0.5` | Filesystem usage check interval |
| `FILE_QUEUE_MAXSIZE` | `--file-queue-max-size` | `10000` | Maximum queued file paths |
| `FILE_QUEUE_MIN_SIZE` | `--file-queue-min-size` | `1000` | Queue depth maintained while deletion is off |
| `DELETION_BATCH_SIZE` | `--deletion-batch-size` | `100` | Files handled per deletion batch |
| `DELETION_MAX_FILES_PER_SECOND` | `--deletion-max-files-per-second` | `0` | Deletion rate; zero means unlimited |
| `FILE_ACCESS_TIME_THRESHOLD_MINUTES` | `--file-access-time-threshold-minutes` | `60` | Minimum file age before deletion |
| `HEX_BUCKET_LEN` | `--hex-bucket-len` | `3` | First hex-bucket name length |
| `ENABLE_DIR_CLEANUP` | `--enable-dir-cleanup` | `true` | Remove empty cache directories |
| `DIR_CLEANUP_TTL_SECONDS` | `--dir-cleanup-ttl-seconds` | `120` | Quiet period before directory cleanup |
| `LOG_FILE_PATH` | `--log-file-path` | empty | Additional append-only log file |
| `STORAGE_EVENTS_ENDPOINT` | `--storage-events-endpoint` | empty | ZMQ endpoint for `BlockRemoved` events |
| `STORAGE_EVENTS_POD_HOSTS` | `--storage-events-pod-hosts` | empty | Comma-separated vLLM pod host names or IP addresses; enables per-pod events |
| `STORAGE_EVENTS_POD_PORTS` | `--storage-events-pod-ports` | `8000` | Comma-separated vLLM serving ports (the InferencePool target ports) |
| `STORAGE_EVENTS_MODEL_NAMES` | `--storage-events-model-names` | empty | Comma-separated `config.json model_name=topic model` pairs |

`TARGET_THRESHOLD` must be lower than `CLEANUP_THRESHOLD`. The cache directory must be a relative path below `PVC_MOUNT_PATH`.
`CLEANUP_THRESHOLD` and `TARGET_THRESHOLD` are percentages of the mounted filesystem’s usable capacity, not of `PVC.spec.resources.requests.storage`.

## Storage events

When `STORAGE_EVENTS_ENDPOINT` is set, the service opens a ZMQ PUB socket and publishes events after successful deletions.
An endpoint with a wildcard host (`tcp://*:5570`, `tcp://0.0.0.0:5570`, `tcp://[::]:5570`) or an `ipc://` or `inproc://` address is bound.
Any other endpoint, such as `tcp://<router-service>:5570`, is connected to, and the value may list several comma-separated endpoints. A connected socket keeps retrying in the background and reconnects after a lost connection; events sent while no peer is connected are dropped.
Without `STORAGE_EVENTS_POD_HOSTS`, the topic is `kv@SHARED_STORAGE@<model-name>` and the event is `["BlockRemoved", [hashes], "SHARED_STORAGE"]`.
The model name is read from the model's `config.json` file and can be renamed with `STORAGE_EVENTS_MODEL_NAMES`.
Each removed block is reported by the low 64 bits of the hash in its file name, which are the last 16 hex characters. vLLM names files after the full block hash (64 hex characters for sha256, 32 for xxhash), and its own KV events carry the same low 64 bits when `VLLM_KV_EVENTS_USE_INT_BLOCK_HASHES=1`, the default.
If that metadata cannot be read after retries, the service continues deleting files but disables event publishing.

### Routing with the llm-d router

The llm-d router keeps one KV-cache index entry per pod, device tier, and KV-cache group.
vLLM's file-system offloading tier adds a `storage` entry for its pod when it writes a file, and never removes it.
A `BlockRemoved` event removes that entry only when it names the same pod, tier, group, and topic model.
`STORAGE_EVENTS_POD_HOSTS` makes the evictor send events in that form:

- one message per pod and serving port, with the topic `kv@<pod IP>:<port>@<model-name>`, which is the pod name the router's pod discovery uses;
- the event `["BlockRemoved", [hashes], "STORAGE", <group>]`, where the group comes from the file's `<hh>_g<group>` directory.

Each host in `STORAGE_EVENTS_POD_HOSTS` is either an IP address or a DNS name. A successful resolution is reused for 5 seconds.
A headless Service name resolves to every ready pod behind it, so a standalone evictor on a shared PVC reaches every pod that may have stored the file.
A sidecar on a per-pod PVC can set it to its own pod IP.
Removing an entry that a pod never stored has no effect.

The router only receives the events on a socket that it binds. Set `kvEventsConfig.zmqEndpoint` (for example `tcp://*:5570`) next to `discoverPods: true` in the precise prefix-cache producer, expose that port on the router's Service, and set `STORAGE_EVENTS_ENDPOINT` to it (for example `tcp://<router-service>:5570`).
With several router replicas, list one endpoint per replica.

The topic model must equal the model in vLLM's `--kv-events-config` topic, which is the model name the router scores requests with.
`config.json` holds the `--model` value, so set `STORAGE_EVENTS_MODEL_NAMES` when vLLM loads the model from a local path or serves it under another name.

A file name holds one event hash, the hash of the last GPU block in the file. `blocks_per_file` in `config.json` is the number of GPU blocks per file.
The removal matches every `storage` entry of the file when `blocks_per_file` is 1, or when vLLM sends placeholder STORAGE events (`self_describing_kv_events` off, the default), which also carry only that hash.
With self-describing events and `blocks_per_file` above 1, vLLM's event also lists the other blocks of the file, and their entries stay in the index.

## Development

Run the complete local validation set from the repository root:

```sh
make verify
```

Individual targets are also available:

```sh
make fmt-check  # Check gofmt output
make test       # Run Go tests
make lint       # Run the pinned golangci-lint version
make build      # Build filesystem/bin/pvc-evictor
make image      # Build the container image
make chart-lint # Lint the Helm chart
make chart-render # Render the Helm chart
make kustomize-render # Render standalone Kustomize manifests
```

`make lint` downloads the pinned `golangci-lint` module when it is not already in the Go module cache.
`make image` uses Podman by default; set `CONTAINER_ENGINE=docker` to use Docker.

## Deployment

Kustomize provides the standalone Deployment. Helm provides standalone mode
and a sidecar container template for vLLM.

- Standalone mode runs one evictor Pod against an existing PVC.
- Sidecar mode runs the evictor container in the same Pod as vLLM.
- You must create the PVC before installing; neither method creates it.

See [deploy/README.md](deploy/README.md) for mode, settings, and install
commands.
