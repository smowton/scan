
# series: (x, y) list list
# First level delimits subplots, second delimits series within them.

import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams['font.size'] = 8

series_colours = ['k', '0.25']
series_markers = [' ', '|']

def draw_stackplot(plots, xlabel, ylabel, save_file = None):

    fig = plt.figure()
    masterax = fig.add_subplot(111, axisbg='none')
    masterax.spines['top'].set_color('none')
    masterax.spines['bottom'].set_color('none')
    masterax.spines['left'].set_color('none')
    masterax.spines['right'].set_color('none')
    masterax.tick_params(labelcolor='none', top='off', bottom='off', left='off', right='off')

    for i, plot in enumerate(plots):

        arg = int("%s%s%s" % (len(plots), 1, i+1))
        ax = fig.add_subplot(arg)

        for ((xs, ys), colour, marker) in zip(plot, series_colours, series_markers):
            ax.plot(xs, ys, color=colour, marker=marker)

        if i != len(plots) - 1:
            ax.set_xticklabels([])
        else:
            ax.set_xlabel(xlabel)
        lims = ax.get_ylim()
        ax.set_ylim(lims[0], lims[1] * 1.1)

    masterax.set_ylabel(ylabel)

    if save_file is None:
        plt.show()
    else:
        plt.savefig(save_file)
