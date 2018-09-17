from models import *
from matplotlib.ticker import FuncFormatter
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
sns.set(style="whitegrid")

# Initialize the matplotlib figure
f, ax = plt.subplots(figsize=(15, 6))
plt.title("Number of authors in Scopus per country which wrote in Physics field")

authors = pd.read_csv('./authors_per_country.csv').sort_values('total_authors', ascending=False)

def thousands(x, pos):
    return '%1iK' % (x*1e-3)

formatter = FuncFormatter(thousands)

# Plot the total authors
sns.set_color_codes("pastel")
sns.barplot(x="country", y="total_authors", data=authors,
            label="Total", color="b")

# Plot downloaded authors
sns.set_color_codes("muted")
sns.barplot(x="country", y="stored", data=authors,
            label="Saved", color="b")


# Add a legend and informative axis label
ax.yaxis.set_major_formatter(formatter)
ax.legend(ncol=2, loc="upper right", frameon=True)
ax.set(ylabel="", xlabel="")

sns.despine(left=True, bottom=True)

plt.show()