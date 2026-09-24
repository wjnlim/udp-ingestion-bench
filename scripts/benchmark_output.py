"""
    Extract the fixed C++ output schema into CSV; 
    not revalidate its math.
"""

import csv

CHANNEL_FIELDS = (
    "port", "stop", "target", "received", "valid", "invalid", "gap_events",
    "missing", "late_or_duplicate", "expected_sequence", "sequence_exhausted",
    "enqueued", "queue_full_drops", "processed", "checksum", "warmup_processed",
    "latency_samples", "latency_p50_ns", "latency_p99_ns", "latency_p999_ns",
)
MEASUREMENT_FIELDS = (
    "measurement_scope", "measurement_available", "measurement_elapsed_ns",
    "measurement_process_cpu_ns", "measurement_cpu_percent",
    "measurement_processed_pps", "measurement_processed_channel_0",
    "measurement_processed_channel_1",
)
PUBLISHER_FIELDS = (
    "publisher_rate_scope", "publisher_requested_pps", "publisher_sent_packets",
    "publisher_elapsed_ns", "publisher_achieved_pps",
)
METADATA_FIELDS = (
    "git_commit", "architecture", "requested_pps", "run_index", "publisher_cpu",
    "rx_cpu0", "rx_cpu1", "rx_cpu", "main_cpu", "downstream_cpu",
    "spsc_capacity", "epoll_service_budget", "pause", "kernel_version", "build_type",
)
CSV_FIELDS = (
    *METADATA_FIELDS, "status", "error", "receiver_exit_code", "publisher_exit_code",
    "channel", *CHANNEL_FIELDS, *MEASUREMENT_FIELDS, *PUBLISHER_FIELDS,
)


def parse_outputs(receiver_log_path, publisher_log_path):
    channels_log, metrics_log, publisher_log = {}, {}, {}
    current = None

    with open(receiver_log_path, encoding="utf-8") as receiver_log_file:
        for line in receiver_log_file:
            key, separator, value = line.rstrip("\r\n").partition("=")
            if not separator:
                continue
            if key == "channel":
                if value not in ("0", "1") or value in channels_log:
                    raise ValueError("unexpected or duplicate channel")
                current = channels_log[value] = {}
            elif key in MEASUREMENT_FIELDS:
                if key in metrics_log:
                    raise ValueError(f"duplicate field: {key}")
                metrics_log[key] = value
            elif current is not None and key in CHANNEL_FIELDS:
                if key in current:
                    raise ValueError(f"duplicate channel field: {key}")
                current[key] = value

    with open(publisher_log_path, encoding="utf-8") as publisher_log_file:
        for line in publisher_log_file:
            key, separator, value = line.rstrip("\r\n").partition("=")
            if separator and key in PUBLISHER_FIELDS:
                if key in publisher_log:
                    raise ValueError(f"duplicate field: {key}")
                publisher_log[key] = value
    return channels_log, metrics_log, publisher_log


def require_complete_output(channels_log, metrics_log, publisher_log):
    for name, values, fields in (
        ("channel 0", channels_log.get("0", {}), CHANNEL_FIELDS),
        ("channel 1", channels_log.get("1", {}), CHANNEL_FIELDS),
        ("measurement", metrics_log, MEASUREMENT_FIELDS),
        ("publisher", publisher_log, PUBLISHER_FIELDS),
    ):
        missing = [key for key in fields if not values.get(key)]
        if missing:
            raise ValueError(f"{name}: missing fields:{', '.join(missing)}")


def write_result(path, metadata_log, execution_log, 
                 channels_log, metrics_log, publisher_log):
    # Global metrics repeat per channel: do not sum their CSV columns.
    with path.open("x", newline="", encoding="utf-8") as output:
        writer = csv.DictWriter(output, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for channel in ("0", "1"):
            writer.writerow({
                **metadata_log, **execution_log, "channel": channel,
                **channels_log.get(channel, {}), **metrics_log, **publisher_log,
            })
