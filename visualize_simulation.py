import argparse
import csv
import html
import math
from collections import defaultdict
from pathlib import Path


DEFAULT_LOG_CSV_FILE = "EventLogs/event_logs.csv"
DEFAULT_OUTPUT_HTML_FILE = "EventLogs/simulation_timeline.html"

ACTIVITY_COLORS = {
    "waiting": "#8fb8de",
    "service": "#5d8a66",
    "transfer": "#d99152",
}


def parse_time(raw_value: str) -> float:
    try:
        return float(raw_value)
    except (TypeError, ValueError):
        return 0.0


def load_events(csv_file: Path) -> list[dict]:
    with csv_file.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        events = []
        for row in reader:
            event = dict(row)
            event["time"] = parse_time(row.get("time", "0"))
            events.append(event)
    events.sort(key=lambda event: (event["time"], event.get("tray_id", ""), event.get("activity", "")))
    return events


def build_segments(events: list[dict]) -> tuple[list[dict], float]:
    open_waiting: dict[tuple[str, str], float] = {}
    open_service: dict[tuple[str, str], float] = {}
    open_transfer: dict[str, dict] = {}
    segments: list[dict] = []
    max_time = 0.0

    for event in events:
        time = event["time"]
        tray_id = event.get("tray_id", "")
        station_id = event.get("station_id", "")
        activity = event.get("activity", "")
        max_time = max(max_time, time)

        if activity == "enqueued":
            open_waiting[(tray_id, station_id)] = time
        elif activity == "dequeued":
            start = open_waiting.pop((tray_id, station_id), None)
            if start is not None and time >= start:
                segments.append(
                    {
                        "tray_id": tray_id,
                        "station_id": station_id,
                        "label": f"{station_id} waiting",
                        "kind": "waiting",
                        "start": start,
                        "end": time,
                    }
                )
        elif activity == "service_start":
            open_service[(tray_id, station_id)] = time
        elif activity == "service_end":
            start = open_service.pop((tray_id, station_id), None)
            if start is not None and time >= start:
                segments.append(
                    {
                        "tray_id": tray_id,
                        "station_id": station_id,
                        "label": f"{station_id} service",
                        "kind": "service",
                        "start": start,
                        "end": time,
                    }
                )
        elif activity == "transfer_start":
            open_transfer[tray_id] = {
                "start": time,
                "station_id": station_id,
            }
        elif activity == "transfer_end":
            transfer = open_transfer.pop(tray_id, None)
            if transfer is not None and time >= transfer["start"]:
                tail_station = transfer["station_id"]
                head_station = station_id if station_id != tail_station else "next"
                segments.append(
                    {
                        "tray_id": tray_id,
                        "station_id": tail_station,
                        "label": f"{tail_station} -> {head_station} transfer",
                        "kind": "transfer",
                        "start": transfer["start"],
                        "end": time,
                    }
                )

    return segments, max_time


def summarize_segments(segments: list[dict]) -> list[dict]:
    totals: dict[tuple[str, str], float] = defaultdict(float)
    counts: dict[tuple[str, str], int] = defaultdict(int)

    for segment in segments:
        duration = max(0.0, segment["end"] - segment["start"])
        key = (segment["station_id"], segment["kind"])
        totals[key] += duration
        counts[key] += 1

    summary = []
    for (station_id, kind), total_duration in sorted(totals.items()):
        summary.append(
            {
                "station_id": station_id,
                "kind": kind,
                "count": counts[(station_id, kind)],
                "total_duration": total_duration,
                "avg_duration": total_duration / counts[(station_id, kind)],
            }
        )
    return summary


def render_html(segments: list[dict], summary: list[dict], max_time: float, title: str) -> str:
    trays = sorted({segment["tray_id"] for segment in segments})
    tray_order = {tray_id: index for index, tray_id in enumerate(trays)}

    row_height = 34
    top_margin = 48
    left_margin = 120
    right_margin = 40
    chart_width = 1200
    chart_height = top_margin + max(1, len(trays)) * row_height + 40
    total_time = max(max_time, 1.0)
    scale = chart_width / total_time

    grid_lines = []
    tick_count = min(10, max(2, math.ceil(total_time)))
    for index in range(tick_count + 1):
        tick_time = total_time * index / tick_count
        x = left_margin + tick_time * scale
        grid_lines.append(
            f'<line x1="{x:.2f}" y1="{top_margin - 20}" x2="{x:.2f}" y2="{chart_height - 18}" class="grid" />'
        )
        grid_lines.append(
            f'<text x="{x:.2f}" y="20" class="tick">{tick_time:.2f}</text>'
        )

    tray_labels = []
    for tray_id, index in tray_order.items():
        y = top_margin + index * row_height + 18
        tray_labels.append(f'<text x="16" y="{y}" class="tray-label">{html.escape(tray_id)}</text>')

    segment_rects = []
    for segment in sorted(segments, key=lambda item: (tray_order[item["tray_id"]], item["start"], item["end"])):
        y = top_margin + tray_order[segment["tray_id"]] * row_height
        x = left_margin + segment["start"] * scale
        width = max(2.0, (segment["end"] - segment["start"]) * scale)
        color = ACTIVITY_COLORS.get(segment["kind"], "#999999")
        tooltip = (
            f'{segment["label"]} | {segment["tray_id"]} | '
            f'{segment["start"]:.3f} -> {segment["end"]:.3f} '
            f'({segment["end"] - segment["start"]:.3f})'
        )
        segment_rects.append(
            f'<rect x="{x:.2f}" y="{y:.2f}" width="{width:.2f}" height="20" '
            f'rx="4" fill="{color}" class="segment"><title>{html.escape(tooltip)}</title></rect>'
        )

    legend_items = []
    for kind, color in ACTIVITY_COLORS.items():
        legend_items.append(
            f'<div class="legend-item"><span class="swatch" style="background:{color}"></span>{html.escape(kind.title())}</div>'
        )

    summary_rows = []
    for row in summary:
        summary_rows.append(
            "<tr>"
            f"<td>{html.escape(row['station_id'])}</td>"
            f"<td>{html.escape(row['kind'])}</td>"
            f"<td>{row['count']}</td>"
            f"<td>{row['total_duration']:.3f}</td>"
            f"<td>{row['avg_duration']:.3f}</td>"
            "</tr>"
        )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{html.escape(title)}</title>
  <style>
    :root {{
      --bg: #f3efe7;
      --panel: #fffaf2;
      --ink: #23313a;
      --muted: #667784;
      --grid: #d8cec0;
      --frame: #c3b6a2;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Avenir Next", "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top right, #efe2ce 0, transparent 28%),
        linear-gradient(180deg, #f7f1e6 0%, var(--bg) 100%);
    }}
    main {{
      max-width: 1480px;
      margin: 0 auto;
      padding: 28px;
    }}
    h1 {{
      margin: 0 0 10px;
      font-size: 2rem;
      letter-spacing: 0.01em;
    }}
    p {{
      margin: 0 0 18px;
      color: var(--muted);
      line-height: 1.5;
    }}
    .panel {{
      background: color-mix(in srgb, var(--panel) 92%, white);
      border: 1px solid var(--frame);
      border-radius: 18px;
      padding: 20px;
      box-shadow: 0 16px 40px rgba(75, 60, 32, 0.08);
      overflow-x: auto;
    }}
    .legend {{
      display: flex;
      gap: 14px;
      flex-wrap: wrap;
      margin-bottom: 16px;
      color: var(--muted);
      font-size: 0.95rem;
    }}
    .legend-item {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }}
    .swatch {{
      width: 14px;
      height: 14px;
      border-radius: 3px;
      display: inline-block;
    }}
    svg {{
      width: {left_margin + chart_width + right_margin}px;
      height: {chart_height}px;
      display: block;
    }}
    .grid {{
      stroke: var(--grid);
      stroke-width: 1;
      stroke-dasharray: 4 8;
    }}
    .tick {{
      fill: var(--muted);
      font-size: 12px;
      text-anchor: middle;
    }}
    .tray-label {{
      fill: var(--ink);
      font-size: 13px;
      font-weight: 600;
    }}
    .segment {{
      opacity: 0.92;
      stroke: rgba(35, 49, 58, 0.18);
      stroke-width: 1;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      margin-top: 24px;
      background: rgba(255, 255, 255, 0.45);
      border-radius: 12px;
      overflow: hidden;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid #e3d9ca;
      text-align: left;
      font-size: 0.95rem;
    }}
    th {{
      background: #efe2ce;
    }}
    @media (max-width: 720px) {{
      main {{
        padding: 18px;
      }}
      h1 {{
        font-size: 1.55rem;
      }}
    }}
  </style>
</head>
<body>
  <main>
    <h1>{html.escape(title)}</h1>
    <p>The timeline groups events by tray. Long orange bars indicate transfers that remained blocked before the next station accepted the tray.</p>
    <section class="panel">
      <div class="legend">{''.join(legend_items)}</div>
      <svg viewBox="0 0 {left_margin + chart_width + right_margin} {chart_height}" role="img" aria-label="Simulation timeline">
        {''.join(grid_lines)}
        {''.join(tray_labels)}
        {''.join(segment_rects)}
      </svg>
      <table>
        <thead>
          <tr>
            <th>Station</th>
            <th>Activity</th>
            <th>Count</th>
            <th>Total Duration</th>
            <th>Average Duration</th>
          </tr>
        </thead>
        <tbody>
          {''.join(summary_rows) or '<tr><td colspan="5">No interval data found in the log.</td></tr>'}
        </tbody>
      </table>
    </section>
  </main>
</body>
</html>
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate an HTML timeline from a simulation event log.")
    parser.add_argument(
        "-i",
        "--input-csv-file",
        default=DEFAULT_LOG_CSV_FILE,
        help="Path to the simulation CSV event log.",
    )
    parser.add_argument(
        "-o",
        "--output-html-file",
        default=DEFAULT_OUTPUT_HTML_FILE,
        help="Path to the generated HTML timeline.",
    )
    parser.add_argument(
        "--title",
        default="Simulation Timeline",
        help="Title shown in the generated HTML report.",
    )
    args = parser.parse_args()

    input_csv = Path(args.input_csv_file)
    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV file not found: {input_csv}")

    events = load_events(input_csv)
    segments, max_time = build_segments(events)
    summary = summarize_segments(segments)
    html_report = render_html(segments, summary, max_time, args.title)

    output_html = Path(args.output_html_file)
    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(html_report, encoding="utf-8")

    print(f"Visualization written to {output_html}")


if __name__ == "__main__":
    main()
