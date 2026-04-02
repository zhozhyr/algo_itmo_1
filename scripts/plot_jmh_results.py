#!/usr/bin/env python3

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


def parse_args():
    parser = argparse.ArgumentParser(
        description="Plot JMH dataset-size charts with confidence intervals."
    )
    parser.add_argument("input", type=Path, help="Path to JMH JSON file")
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        default=Path("target/jmh-plots"),
        help="Directory for generated PNG plots",
    )
    parser.add_argument(
        "--title-prefix",
        default="ExtendibleHashTable",
        help="Prefix for plot titles",
    )
    return parser.parse_args()


def sanitize_filename(value):
    return re.sub(r"[^a-zA-Z0-9._-]+", "_", value).strip("_")


def benchmark_name(full_name):
    return full_name.split(".")[-1]


def mode_label(mode):
    if mode == "thrpt":
        return "Throughput", "ops/us"
    if mode == "avgt":
        return "AverageTime", "us/op"
    raise ValueError(f"Unsupported JMH mode: {mode}")


def group_records(records):
    grouped = defaultdict(list)
    for entry in records:
        params = entry.get("params", {})
        dataset_size = params.get("datasetSize", params.get("itemCount"))
        if dataset_size is None:
            continue

        name = benchmark_name(entry["benchmark"])
        mode = entry["mode"]
        metric = entry["primaryMetric"]
        score_unit = metric.get("scoreUnit")
        if score_unit:
            mode_name = "Throughput" if mode == "thrpt" else "AverageTime"
            unit = score_unit
        else:
            mode_name, unit = mode_label(mode)
        confidence = metric.get("scoreConfidence")

        if confidence:
            low = min(confidence[0], confidence[1])
            high = max(confidence[0], confidence[1])
        else:
            low = metric["score"]
            high = metric["score"]

        grouped[(name, mode_name, unit)].append(
            {
                "dataset_size": int(dataset_size),
                "score": metric["score"],
                "low": low,
                "high": high,
            }
        )
    return grouped


def plot_series(name, mode_name, unit, points, output_dir, title_prefix):
    points.sort(key=lambda item: item["dataset_size"])
    x_values = [item["dataset_size"] for item in points]
    y_values = [item["score"] for item in points]
    lows = [min(item["low"], item["score"], item["high"]) for item in points]
    highs = [max(item["low"], item["score"], item["high"]) for item in points]
    y_errors = [
        [max(0.0, score - low) for score, low in zip(y_values, lows)],
        [max(0.0, high - score) for score, high in zip(y_values, highs)],
    ]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.errorbar(
        x_values,
        y_values,
        yerr=y_errors,
        fmt="-o",
        linewidth=2,
        markersize=6,
        capsize=4,
    )
    ax.set_title(f"{title_prefix}: {name} ({mode_name})")
    ax.set_xlabel("Dataset size")
    ax.set_ylabel(unit)
    ax.grid(True, linestyle="--", alpha=0.35)
    ax.ticklabel_format(axis="x", style="plain")
    fig.tight_layout()

    output_path = output_dir / f"{sanitize_filename(name)}_{sanitize_filename(mode_name)}.png"
    fig.savefig(output_path, dpi=160)
    plt.close(fig)
    return output_path


def main():
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)

    with args.input.open("r", encoding="utf-8") as fh:
        records = json.load(fh)

    grouped = group_records(records)
    if not grouped:
        raise SystemExit("No suitable JMH records found in JSON.")

    saved = []
    for (name, mode_name, unit), points in sorted(grouped.items()):
        saved.append(plot_series(name, mode_name, unit, points, args.output_dir, args.title_prefix))

    print("Saved plots:")
    for path in saved:
        print(path)


if __name__ == "__main__":
    main()
