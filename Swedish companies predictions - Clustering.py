# Valerio "Weed" Malerba, Uppsala, 2024

import pandas as pd
from sklearn.preprocessing import RobustScaler
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from sklearn.metrics import silhouette_score, davies_bouldin_score

# Constants
PATH = "D:\Documents\Python Scripts\Scrapers\Bolagsskrapare\\"
FULL_FILE_PATH = PATH + "ML_data 14x6 regions.csv"
INDEX_COL = "organization number"
STATUS_COL = 'Status'
EMPLOYEES_COL = 'Number of employees_2022'
NET_REVENUE_COL = 'Net revenue_2022'
EBITDA_COL = 'EBITDA_2022'
SOLVENCY_COL = 'Solvency_2022'
PCA_COMPONENTS = 2
N_CLUSTERS = 8
RANDOM_STATE = 79
NUMBER_OF_SAMPLES = 20000
MISSING_VALUE_REPLACEMENT = 'X'
PLOT_PALETTE = 'colorblind'
CLUSTER_COLUMN = 'Cluster'
PLOT_STYLE = '--'
CLUSTERING_RESULTS_FILENAME = 'clustering_results.png'
PAIRPLOT_RESULTS_FILENAME = 'pairplot_results.png'

# Function to save plots as images
def save_plot_as_image(plt, filename):
    plt.savefig(filename, format='png')
    plt.close()

# Generate clustering data and plots
print("Reading source file...")
df = pd.read_csv(FULL_FILE_PATH, index_col=INDEX_COL)
df = df[(df[STATUS_COL] == 2) & (df[EMPLOYEES_COL] == 1)].sample(n=NUMBER_OF_SAMPLES, random_state=RANDOM_STATE)  # Select only active companies with 1 employee

# Selection of KPI for clustering
clustering_features = [
    NET_REVENUE_COL, EBITDA_COL, SOLVENCY_COL
]

df = df[clustering_features]

# Removing outliers using IQR
Q1 = df.quantile(0.25)
Q3 = df.quantile(0.75)
IQR = Q3 - Q1
df = df[~((df < (Q1 - 1.5 * IQR)) | (df > (Q3 + 1.5 * IQR))).any(axis=1)]

# Replace NaNs with MISSING_VALUE_REPLACEMENT, -inf with minimum non-infinite, and inf with maximum non-infinite values
df = df.fillna(MISSING_VALUE_REPLACEMENT)
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
pca = PCA(n_components=PCA_COMPONENTS)
principal_components = pca.fit_transform(df_scaled)
df_pca = pd.DataFrame(data=principal_components, columns=['PC1', 'PC2'])

# Applying K-Means for clustering
kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE)  # Tuned parameters
clusters = kmeans.fit_predict(df_pca)
df[CLUSTER_COLUMN] = clusters
df_pca[CLUSTER_COLUMN] = clusters

# Plotting the clustering results
plt.figure(figsize=(10, 6))
scatter = sns.scatterplot(x='PC1', y='PC2', hue=CLUSTER_COLUMN, data=df_pca, palette=PLOT_PALETTE, s=100)
plt.title('Clustering results with PCA and K-Means')
plt.legend(loc='lower right', bbox_to_anchor=(1.25, 0))
plt.tight_layout()

# Display the plot
plt.show()

# Calculating the medians of the principal components
medians = df_pca.median()

# Visualizing the results with medians
plt.figure(figsize=(10, 6))
scatter = sns.scatterplot(x='PC1', y='PC2', hue=CLUSTER_COLUMN, data=df_pca, palette=PLOT_PALETTE, s=100)
plt.axvline(medians['PC1'], color='black', linestyle=PLOT_STYLE, linewidth=1)
plt.axhline(medians['PC2'], color='black', linestyle=PLOT_STYLE, linewidth=1)
plt.scatter(medians['PC1'], medians['PC2'], color='black', s=200, marker='+')  # Adding the black cross
plt.title('Clustering results with PCA and K-Means')
plt.legend(loc='lower right', bbox_to_anchor=(1.25, 0))
plt.tight_layout()  # Ensure the layout is correct
save_plot_as_image(plt, CLUSTERING_RESULTS_FILENAME)

# Visualizing the original features colored by cluster with medians
g = sns.pairplot(df, hue=CLUSTER_COLUMN, palette=PLOT_PALETTE)
for ax in g.axes.flatten():
    if ax.get_xlabel() in df.columns and ax.get_ylabel() in df.columns:
        ax.axvline(df[ax.get_xlabel()].median(), color='black', linestyle=PLOT_STYLE, linewidth=1)
        ax.axhline(df[ax.get_ylabel()].median(), color='black', linestyle=PLOT_STYLE, linewidth=1)
        ax.scatter(df[ax.get_xlabel()].median(), df[ax.get_ylabel()].median(), color='black', s=200, marker='+')
save_plot_as_image(plt, PAIRPLOT_RESULTS_FILENAME)

# Print cluster statistical characteristics
cluster_stats = []
for cluster_id in np.unique(clusters):
    cluster_data = df[df[CLUSTER_COLUMN] == cluster_id]
    if not cluster_data.empty:
        stats = cluster_data.describe().to_string()
        cluster_stats.append(f'Cluster {cluster_id} statistics:\n{stats}')

# Calculate silhouette score and Davies-Bouldin Index
sil_score = silhouette_score(df_pca, clusters)
db_index = davies_bouldin_score(df_pca, clusters)

cluster_stats = df.groupby(CLUSTER_COLUMN).describe()
print(cluster_stats)