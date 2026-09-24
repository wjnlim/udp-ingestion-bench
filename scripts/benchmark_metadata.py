"""Only the metadata needed to identify this benchmark configuration."""

import platform
import subprocess


def collect_metadata(args, project_root):
    try:
        commit = subprocess.check_output(
            ["git", "-C", str(project_root), "rev-parse", "HEAD"],
            stderr=subprocess.DEVNULL, text=True, timeout=2,
        ).strip()
    except (OSError, subprocess.SubprocessError):
        commit = "unknown"

    build_type = "unknown"
    try:
        with (args.build_dir / "CMakeCache.txt").open(encoding="utf-8") as cache:
            for line in cache:
                if line.startswith("CMAKE_BUILD_TYPE:STRING="):
                    build_type = line.rstrip("\r\n").partition("=")[2] or "unknown"
    except OSError:
        pass

    metadata_log = {
        "git_commit": commit, "architecture": args.architecture,
        "requested_pps": args.rate, "run_index": args.run_index,
        "spsc_capacity": args.queue_capacity, "kernel_version": platform.release(),
        "build_type": build_type,
        "epoll_service_budget": 64 if args.architecture == "epoll" else "NA",
        "pause": args.rx_pause,
    }

    roles = {"publisher_cpu", "downstream_cpu"}
    roles.update({"rx_cpu0", "rx_cpu1", "main_cpu"}
                 if args.architecture == "dedicated" else {"rx_cpu"})
    
    for role in ("publisher_cpu", "rx_cpu0", "rx_cpu1", "rx_cpu", 
                                        "main_cpu", "downstream_cpu"):
        cpu = getattr(args, role)
        metadata_log[role] = (cpu if cpu is not None else "inherited") if role in roles else "NA"
    return metadata_log
