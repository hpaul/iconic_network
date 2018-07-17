from models import *
import matplotlib.pyplot as plt
import pandas as pd
# View statistics per year
# SELECT year, count(year) as articles, sum(cited_by) as cited FROM
#   (SELECT * FROM collaboration GROUP BY author)
#   GROUP BY year

columns = [
    Collaboration.year,
    Collaboration.author,
    Collaboration.cited_by
]
grouped = Collaboration.select(*columns).where(Collaboration.year > 2006).group_by(Collaboration.author)

years = (Select(columns=[
    grouped.c.year,
    fn.COUNT(grouped.c.year).alias('articles'),
    fn.SUM(grouped.c.cited_by).alias('cited')]
).from_(grouped).group_by(grouped.c.year))

results = years.execute(db)
years = [r['year'] for r in results]
articles = [r['articles'] for r in results]
cited = [r['cited'] for r in results]

plt.figure(1, figsize=(10, 10))

plt.subplot(211)
plt.title('Articles per year')
plt.plot(years, articles)
plt.xticks(years)

plt.subplot(212)
plt.title('Article citation per year')
plt.plot(years, cited)
plt.xticks(years)

plt.show()