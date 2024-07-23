# Valerio Malerba, Uppsala, 2024

import pandas as pd
from sklearn.preprocessing import RobustScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.metrics import silhouette_score, davies_bouldin_score

# Function to save plots as images
def save_plot_as_image(plt, filename):
    plt.savefig(filename, format='png')
    plt.close()

# Generate clustering data and plots
random_state = 79
number_of_samples = 20000
n_clusters = 8

print("Reading source file...")
df = pd.read_csv("ML_data 14x6 regions - -1 items.csv", index_col="organization number")
df = df[(df['Status'] == 2) & (df['Number of employees_2022'] == 1)].sample(n=number_of_samples, random_state=random_state) # Select only active companies with 1 employee

# Selection of KPI for clustering
Clustering_features = [
    'Net revenue_2022', 'EBITDA_2022', 'Solvency_2022'
]

df = df[Clustering_features]

# Removing outliers using IQR
Q1 = df.quantile(0.25)
Q3 = df.quantile(0.75)
IQR = Q3 - Q1
df = df[~((df < (Q1 - 1.5 * IQR)) | (df > (Q3 + 1.5 * IQR))).any(axis=1)]

# Replace NaNs with "X", -inf with minimum non-infinite, and inf with maximum non-infinite values
df = df.fillna('X')
numeric_columns = df.select_dtypes(include=[np.number]).columns
for col in numeric_columns:
    min_val = df.loc[df[col] != -np.inf, col].min()
    max_val = df.loc[df[col] != np.inf, col].max()
    df[col] = df[col].replace(-np.inf, min_val)
    df[col] = df[col].replace(np.inf, max_val)

# Standardizing the data using RobustScaler to handle outliers
scaler = RobustScaler()
df_scaled = scaler.fit_transform(df)

# Applying PCA to reduce dimensionality (if necessary)
pca = PCA(n_components=2)
principal_components = pca.fit_transform(df_scaled)
df_pca = pd.DataFrame(data=principal_components, columns=['PC1', 'PC2'])

# Applying K-Means for clustering
kmeans = KMeans(n_clusters=n_clusters, random_state=random_state)  # Tuned parameters
clusters = kmeans.fit_predict(df_pca)
df['Cluster'] = clusters
df_pca['Cluster'] = clusters

# Plotting the clustering results
plt.figure(figsize=(10, 6))
scatter = sns.scatterplot(x='PC1', y='PC2', hue='Cluster', data=df_pca, palette='colorblind', s=100)
plt.title('Clustering results with PCA and K-Means')
plt.legend(loc='lower right', bbox_to_anchor=(1.25, 0))
plt.tight_layout()

# Display the plot
plt.show()

# Calculating the medians of the principal components
medians = df_pca.median()

# Visualizing the results with medians
plt.figure(figsize=(10, 6))
scatter = sns.scatterplot(x='PC1', y='PC2', hue='Cluster', data=df_pca, palette='colorblind', s=100)
plt.axvline(medians['PC1'], color='black', linestyle='--', linewidth=1)
plt.axhline(medians['PC2'], color='black', linestyle='--', linewidth=1)
plt.scatter(medians['PC1'], medians['PC2'], color='black', s=200, marker='+')  # Adding the black cross
plt.title('Clustering results with PCA and K-Means')
plt.legend(loc='lower right', bbox_to_anchor=(1.25, 0))
plt.tight_layout()  # Ensure the layout is correct
save_plot_as_image(plt, 'clustering_results.png')

# Visualizing the original features colored by cluster with medians
g = sns.pairplot(df, hue='Cluster', palette='colorblind')
for ax in g.axes.flatten():
    if ax.get_xlabel() in df.columns and ax.get_ylabel() in df.columns:
        ax.axvline(df[ax.get_xlabel()].median(), color='black', linestyle='--', linewidth=1)
        ax.axhline(df[ax.get_ylabel()].median(), color='black', linestyle='--', linewidth=1)
        ax.scatter(df[ax.get_xlabel()].median(), df[ax.get_ylabel()].median(), color='black', s=200, marker='+')
save_plot_as_image(plt, 'pairplot_results.png')

# Print cluster statistical characteristics
cluster_stats = []
for cluster_id in np.unique(clusters):
    cluster_data = df[df['Cluster'] == cluster_id]
    if not cluster_data.empty:
        stats = cluster_data.describe().to_string()
        cluster_stats.append(f'Cluster {cluster_id} statistics:\n{stats}')

# Calculate silhouette score and Davies-Bouldin Index
sil_score = silhouette_score(df_pca, clusters)
db_index = davies_bouldin_score(df_pca, clusters)

cluster_stats = df.groupby('Cluster').describe()
print(cluster_stats)