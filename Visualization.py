import matplotlib.pyplot as plt
import seaborn as sns

# Load geolocated data
geo_df = pd.read_csv('geolocated_property_data.csv')

# Plot property locations
plt.figure(figsize=(10, 6))
sns.scatterplot(x='Longitude', y='Latitude', data=geo_df, hue='tax_value', size='tax_value')
plt.title('Property Locations in Sarpy County by Tax Value')
plt.show()

# Plot average tax value by year built
avg_tax_by_year = geo_df.groupby('year_built')['tax_value'].mean().reset_index()
plt.figure(figsize=(10, 6))
sns.lineplot(x='year_built', y='tax_value', data=avg_tax_by_year)
plt.title('Average Tax Value by Year Built')
plt.show()