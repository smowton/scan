

import matplotlib
import matplotlib.pyplot as plt

matplotlib.rcParams['font.size'] = 8

def draw_distribution_plot(vals, binsize, xlabel, ylabel, save_file = None):

    binsize = long(binsize)

    firstbin = (long(vals[0]) / binsize) * binsize
    lastbin = (((long(vals[-1]) / binsize) + 1) * binsize)

    plt.figure(figsize = (4, 4))
    plt.hist(vals, bins = range(firstbin, lastbin + binsize, binsize), color = "0.25")
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if save_file is None:
        plt.show()
    else:
        plt.savefig(save_file)
    
