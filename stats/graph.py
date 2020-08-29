import matplotlib.pyplot as plt
import numpy as np
import itertools
import pdb


# grep "mean" stats.log  | awk  '{print $7,$11,$14,$17,$20,$23}' &> data.log
# scp corfu@10.172.208.209:/home/corfu/CorfuDB/stats.log .

def column(matrix, i):
    return [row[i] for row in matrix]


mean_idx = 0
mean_idx_name = "mean"
median_idx = 1
median_idx_name = "median"
perc95_idx = 2
perc95_idx_name = "95pct"
perc99_idx = 3
perc99_idx_name = "99pct"
max_idx = 4
max_idx_name = "max"
thrpt_idx = 5
thrpt_idx_name = "thrpt"

with open('data.log') as f:
    content = f.readlines()
content = [x.strip() for x in content]
content = [x.split() for x in content]

data = []

for x in content:
    data.append(map(float, x))

frame = {
    'x-axis' : np.arange(len(data)),
    mean_idx_name : column(data, mean_idx),
    median_idx_name : column(data, median_idx),
    perc95_idx_name: column(data, perc95_idx),
    perc99_idx_name: column(data, perc99_idx),
    max_idx_name: column(data, max_idx),
    thrpt_idx_name: column(data, thrpt_idx),

}

colors = itertools.cycle(["r", "b", "g", "y", "c", "m"])

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()

xaxis =  np.arange(len(data))
ax1.scatter(xaxis,  column(data, mean_idx), color=next(colors), label=mean_idx_name)
ax1.scatter(xaxis,  column(data, median_idx), color=next(colors), label=median_idx_name)
ax1.scatter(xaxis,  column(data, perc95_idx), color=next(colors), label=perc95_idx_name)
ax1.scatter(xaxis,  column(data, perc99_idx), color=next(colors), label=perc99_idx_name)
ax1.scatter(xaxis,  column(data, max_idx), color=next(colors), label=max_idx_name)

ax2.scatter(xaxis,  column(data, thrpt_idx), color=next(colors), label=thrpt_idx_name)

ax1.set_ylabel('Latency(ms)')
ax2.set_ylabel('Throughput(op/s)')
plt.xlabel('elapsed time (sec)')

lines_labels = [ax.get_legend_handles_labels() for ax in fig.axes]
lines, labels = [sum(lol, []) for lol in zip(*lines_labels)]

fig.legend(lines, labels)

ax1.grid(True)
ax2.grid(True)
plt.savefig('plot.png')
plt.savefig('myimage.svg', format='svg', dpi=1200)
#ax2.legend()
#plt.show()



