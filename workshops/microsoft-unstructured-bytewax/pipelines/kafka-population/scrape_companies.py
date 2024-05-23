import pandas as pd

link = (
    "https://en.wikipedia.org/wiki/Nasdaq-100#Components"
)
df = pd.read_html(link, header=0)[4]

# Write to CSV
df.to_csv("components.csv", index=False)