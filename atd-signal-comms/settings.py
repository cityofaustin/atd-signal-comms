MAX_ATTEMPTS = 2
NUM_WORKERS_DEFAULT = 300
TIMEOUT = 20
SOCRATA_RESOURCE_ID = {"dev": "j9p3-9u87", "prod": "pj7k-98z2"}
DATE_FORMAT_FILE = "%Y-%m-%d"
DATE_FORMAT_SOCRATA = "%Y-%m-%dT%H:%M:%S"
STATUS_CODES = {
    0: "no_attempts",
    1: "online",
    -1: "timeout",
    -2: "invalid_hostname",
    -3: "unknown_error",
}
