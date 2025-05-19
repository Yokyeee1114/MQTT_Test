import pandas as pd
import matplotlib.pyplot as plt
import json
import numpy as np
import seaborn as sns
import os

# Create plots directory
os.makedirs('plots', exist_ok=True)

# Load the test results
results_file = 'results/mqtt_test_results_02.csv'
df = pd.read_csv(results_file)

# Set the plot style
plt.style.use('ggplot')
sns.set_palette("viridis")

# 1. Message rate by QoS level (for 0ms delay)
plt.figure(figsize=(12, 8))
for qos in [0, 1, 2]:
    for size in [0, 1000, 4000]:
        data = df[(df['qos'] == qos) & (df['delay'] == 0) & (df['messagesize'] == size) & (df['sub_qos'] == qos)]
        if not data.empty:
            plt.plot(data['instancecount'], data['message_rate'],
                     marker='o', linewidth=2, markersize=8,
                     label=f'QoS={qos}, Size={size}')

plt.xlabel('Number of Publishers', fontsize=12)
plt.ylabel('Message Rate (messages/second)', fontsize=12)
plt.title('Message Rate by QoS Level and Message Size (0ms delay)', fontsize=14)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.savefig('plots/message_rate_by_qos_size.png', dpi=300)

# 2. Message loss by QoS level (for 0ms delay)
plt.figure(figsize=(12, 8))
for qos in [0, 1, 2]:
    data = df[(df['qos'] == qos) & (df['delay'] == 0) & (df['messagesize'] == 0) & (df['sub_qos'] == qos)]
    if not data.empty:
        plt.plot(data['instancecount'], data['avg_message_loss'],
                 marker='o', linewidth=2, markersize=8,
                 label=f'QoS={qos}')

plt.xlabel('Number of Publishers', fontsize=12)
plt.ylabel('Message Loss (%)', fontsize=12)
plt.title('Message Loss by QoS Level (0ms delay)', fontsize=14)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.savefig('plots/message_loss_by_qos.png', dpi=300)

# 3. Out-of-order messages by QoS level (for 0ms delay)
plt.figure(figsize=(12, 8))
for qos in [0, 1, 2]:
    data = df[(df['qos'] == qos) & (df['delay'] == 0) & (df['messagesize'] == 0) & (df['sub_qos'] == qos)]
    if not data.empty:
        plt.plot(data['instancecount'], data['avg_out_of_order'],
                 marker='o', linewidth=2, markersize=8,
                 label=f'QoS={qos}')

plt.xlabel('Number of Publishers', fontsize=12)
plt.ylabel('Out-of-Order Messages (%)', fontsize=12)
plt.title('Out-of-Order Messages by QoS Level (0ms delay)', fontsize=14)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.savefig('plots/out_of_order_by_qos.png', dpi=300)

# 4. Duplicate messages by QoS level (for 0ms delay)
plt.figure(figsize=(12, 8))
for qos in [0, 1, 2]:
    data = df[(df['qos'] == qos) & (df['delay'] == 0) & (df['messagesize'] == 0) & (df['sub_qos'] == qos)]
    if not data.empty:
        plt.plot(data['instancecount'], data['avg_duplicates'],
                 marker='o', linewidth=2, markersize=8,
                 label=f'QoS={qos}')

plt.xlabel('Number of Publishers', fontsize=12)
plt.ylabel('Duplicate Messages (%)', fontsize=12)
plt.title('Duplicate Messages by QoS Level (0ms delay)', fontsize=14)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.savefig('plots/duplicates_by_qos.png', dpi=300)

# 5. Mean inter-message gap by QoS level (for 0ms delay)
plt.figure(figsize=(12, 8))
for qos in [0, 1, 2]:
    data = df[(df['qos'] == qos) & (df['delay'] == 0) & (df['messagesize'] == 0) & (df['sub_qos'] == qos)]
    if not data.empty:
        plt.plot(data['instancecount'], data['avg_mean_gap'],
                 marker='o', linewidth=2, markersize=8,
                 label=f'QoS={qos}')

plt.xlabel('Number of Publishers', fontsize=12)
plt.ylabel('Mean Inter-Message Gap (ms)', fontsize=12)
plt.title('Mean Inter-Message Gap by QoS Level (0ms delay)', fontsize=14)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.savefig('plots/mean_gap_by_qos.png', dpi=300)

# 6. Impact of message size on performance for each QoS
plt.figure(figsize=(10, 8))
for qos in [0, 1, 2]:
    sizes = []
    rates = []
    for size in [0, 1000, 4000]:
        data = df[(df['qos'] == qos) & (df['delay'] == 0) & (df['messagesize'] == size) &
                  (df['instancecount'] == 1) & (df['sub_qos'] == qos)]
        if not data.empty:
            sizes.append(size)
            rates.append(data['message_rate'].values[0])

    if sizes:
        plt.plot(sizes, rates, marker='o', linewidth=2, markersize=8, label=f'QoS={qos}')

plt.xlabel('Message Size (bytes)', fontsize=12)
plt.ylabel('Message Rate (messages/second)', fontsize=12)
plt.title('Impact of Message Size on Performance by QoS Level', fontsize=14)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.savefig('plots/message_size_impact.png', dpi=300)

# 7. Comparison of publisher QoS vs subscriber QoS
# Create a 3x3 grid showing message rates for all combinations of pub/sub QoS
plt.figure(figsize=(15, 10))

data_subset = df[(df['delay'] == 0) & (df['messagesize'] == 0) & (df['instancecount'] == 5)]
if not data_subset.empty:
    pivot_data = data_subset.pivot_table(
        values='message_rate',
        index='qos',  # Publisher QoS
        columns='sub_qos',  # Subscriber QoS
        aggfunc='mean'
    )

    sns.heatmap(pivot_data, annot=True, fmt='.1f', cmap='viridis')
    plt.xlabel('Subscriber QoS', fontsize=12)
    plt.ylabel('Publisher QoS', fontsize=12)
    plt.title('Message Rate by Publisher/Subscriber QoS Combination', fontsize=14)
    plt.tight_layout()
    plt.savefig('plots/pub_sub_qos_comparison.png', dpi=300)

# 8. Effect of delay on message rate
plt.figure(figsize=(12, 8))
for delay in [0, 100]:
    data = df[(df['qos'] == 0) & (df['delay'] == delay) &
              (df['messagesize'] == 0) & (df['sub_qos'] == 0)]
    if not data.empty:
        plt.plot(data['instancecount'], data['message_rate'],
                 marker='o', linewidth=2, markersize=8,
                 label=f'Delay={delay}ms')

plt.xlabel('Number of Publishers', fontsize=12)
plt.ylabel('Message Rate (messages/second)', fontsize=12)
plt.title('Effect of Delay on Message Rate (QoS=0)', fontsize=14)
plt.legend(fontsize=10)
plt.grid(True)
plt.tight_layout()
plt.savefig('plots/delay_effect.png', dpi=300)

print("Analysis plots created successfully in the 'plots' directory")