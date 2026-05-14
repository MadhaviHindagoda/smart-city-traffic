import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import csv
import os

csv_path = os.path.join(os.path.dirname(__file__), "traffic_report_2026-05-10.csv")

junctions = []
peak_hours = []
max_counts = []
avg_cis = []
needs_police = []

with open(csv_path, newline='') as f:
    reader = csv.DictReader(f)
    for row in reader:
        junctions.append(row['sensor_id'].replace('_', ' '))
        peak_hours.append(int(row['peak_hour']))
        max_counts.append(int(row['max_count']))
        avg_cis.append(float(row['avg_ci']))
        needs_police.append(row['needs_police'].strip().lower() == 'true')

colors = ['#2196F3', '#4CAF50', '#FF9800', '#E91E63']
bar_colors = ['#E53935' if p else c for p, c in zip(needs_police, colors)]

fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
fig.suptitle('Smart City Colombo — Traffic Analysis Report\n2026-05-10',
             fontsize=14, fontweight='bold')

bars = ax1.bar(junctions, max_counts, color=colors, edgecolor='white', linewidth=0.5)
ax1.set_title('Peak Vehicle Count by Junction', fontsize=12, fontweight='bold')
ax1.set_ylabel('Max Vehicle Count')
ax1.set_xlabel('Junction')
ax1.tick_params(axis='x', rotation=15)
ax1.grid(axis='y', alpha=0.3)
for bar, count in zip(bars, max_counts):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
             str(count), ha='center', va='bottom', fontsize=10, fontweight='bold')

bars2 = ax2.bar(junctions, avg_cis, color=bar_colors, edgecolor='white', linewidth=0.5)
ax2.axhline(y=70, color='red', linestyle='--', linewidth=1.5, label='Police threshold (CI=70)')
ax2.set_title('Average Congestion Index by Junction', fontsize=12, fontweight='bold')
ax2.set_ylabel('Congestion Index (0-100)')
ax2.set_xlabel('Junction')
ax2.tick_params(axis='x', rotation=15)
ax2.set_ylim(0, 100)
ax2.grid(axis='y', alpha=0.3)
ax2.legend(fontsize=9)
for bar, ci in zip(bars2, avg_cis):
    ax2.text(bar.get_x() + bar.get_width()/2, ci + 1,
             f'{ci:.1f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

plt.tight_layout()
output = os.path.join(os.path.dirname(__file__), "traffic_analysis_chart.png")
plt.savefig(output, dpi=150, bbox_inches='tight')
print(f"Chart saved: {output}")
plt.close()