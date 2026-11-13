"""Draw why a plain hour number misleads a model and sine/cosine does not.

Run from the repository root:
    uv run --group notebook python docs/assets/cyclical_time.py
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

SURFACE, INK, MUTED, OTHER, FOCUS = "#fcfcfb", "#0b0b0b", "#52514e", "#b9b8b2", "#2a78d6"
hours = np.arange(24)
angle = 2 * np.pi * hours / 24
hour_sin, hour_cos = np.sin(angle), np.cos(angle)
focus = np.isin(hours, [23, 0])
circle_gap = np.hypot(hour_sin[23] - hour_sin[0], hour_cos[23] - hour_cos[0])

figure, (line_ax, circle_ax) = plt.subplots(
    1, 2, figsize=(11, 4.6), gridspec_kw={"width_ratios": [1.6, 1]}, facecolor=SURFACE
)
for ax in (line_ax, circle_ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.tick_params(colors=MUTED)

# Left: one column called hour. 23:00 and 00:00 look as far apart as possible.
line_ax.axhline(0, color=OTHER, linewidth=1, zorder=1)
line_ax.scatter(hours[~focus], np.zeros(22), s=40, color=OTHER, zorder=2)
line_ax.scatter(hours[focus], [0, 0], s=80, color=FOCUS, edgecolor=SURFACE, lw=2, zorder=3)
line_ax.annotate(
    "", xy=(23, 0.35), xytext=(0, 0.35), arrowprops={"arrowstyle": "<->", "color": FOCUS}
)
line_ax.text(11.5, 0.45, "distance = 23", ha="center", color=INK, fontsize=11)
for hour in (0, 23):
    line_ax.text(hour, -0.3, f"{hour:02d}:00", ha="center", color=INK, fontsize=10)
line_ax.set(ylim=(-0.6, 0.8), yticks=[], xticks=range(0, 24, 3), xlabel="hour (UTC)")
line_ax.xaxis.label.set_color(MUTED)
line_ax.set_title("What the model sees: hour as one number", loc="left", color=INK, fontsize=12)

# Right: two columns, sin and cos. The hours sit on a clock face.
circle_ax.plot(
    np.sin(np.linspace(0, 2 * np.pi, 200)),
    np.cos(np.linspace(0, 2 * np.pi, 200)),
    color=OTHER,
    linewidth=1,
    zorder=1,
)
circle_ax.scatter(hour_sin[~focus], hour_cos[~focus], s=40, color=OTHER, zorder=2)
circle_ax.scatter(
    hour_sin[focus], hour_cos[focus], s=80, color=FOCUS, edgecolor=SURFACE, lw=2, zorder=3
)
# Nudge the two highlighted labels apart so they don't overlap at the top.
label_shift = {0: 0.12, 23: -0.12}
for hour in (0, 6, 12, 18, 23):
    radius = 1.22
    circle_ax.text(
        radius * hour_sin[hour] + label_shift.get(hour, 0),
        radius * hour_cos[hour],
        f"{hour:02d}:00",
        ha={0: "left", 23: "right"}.get(hour, "center"),
        va="center",
        color=INK if hour in (0, 23) else MUTED,
        fontsize=10,
    )
circle_ax.text(
    0,
    0,
    f"23:00 to 00:00\ndistance = {circle_gap:.2f}",
    ha="center",
    va="center",
    color=INK,
    fontsize=11,
)
circle_ax.set(
    xlim=(-1.45, 1.45),
    ylim=(-1.45, 1.45),
    aspect="equal",
    xticks=[],
    yticks=[],
    xlabel="hour_sin",
    ylabel="hour_cos",
)
circle_ax.spines["bottom"].set_visible(False)
circle_ax.xaxis.label.set_color(MUTED)
circle_ax.yaxis.label.set_color(MUTED)
circle_ax.set_title("With sin and cos: hour on a clock", loc="left", color=INK, fontsize=12)

figure.tight_layout()
output = Path(__file__).with_suffix(".png")
figure.savefig(output, dpi=150, facecolor=SURFACE)
print(f"Saved {output}")
