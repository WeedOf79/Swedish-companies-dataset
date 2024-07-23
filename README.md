# Swedish Companies Dataset with Machine Learning Techniques

## Introduction

Once I completed a series of online courses about data science and machine learning, I was eager to put my knowledge into practice to show others what I am capable of achieving.
I needed a dataset. The internet offers plenty of good ones ready to use. However, a data scientist must also be able to collect and prepare data by themselves. So, I thought, what could be something interesting to build up? Being an entrepreneur and living in Sweden, I decided to create a dataset about Swedish companies and their balance sheet data.
Unfortunately the dataset is too large to be uploaded in GitHub. Soon I will add a link to an external source. ;-)

## How the Dataset was Built

There are several websites that freely supply access to balance sheet data. However, they don’t allow downloading a complete database at once.
Therefore, I decided to create a Python script to do some web scraping on one of these sites using libraries such as Beautiful Soup. The process took about one week to run.
The result is a comma-separated value file which contains (at the current date) among other features:
- 462,468 Swedish joint-stock companies.
- Historical balance sheet data ranging from 2012 to 2022.
- General data such as organization number, location, type, commercial category, etc.
- KPIs such as DuPont analysis, solvency, EBIT, and so on.

## Data Cleaning – Level 1

The raw data collected by the first script is far from being suitable for machine learning. For several reasons, it contains missing entry points, invalid values, duplicates, and more.
So, I created a second script that prepares the data. The cleaning process is performed in two steps.
The first level drops duplicates (by organization number, a unique identification number used in Sweden), removes irrelevant features, translates column names and activity types into English.
Note: this first cleaning does not alter the raw values.

## Data Cleaning – Level 2

The second level is deeper: some features are replaced with more meaningful ones, some others (textual ones) are mapped to ordinals.
Data points with too many missing values are deleted.
Data points with a limited amount of missing or invalid values are instead processed. There is a series of 5 different criteria used to replace the bad data depending on the availability of adjacent values and the median values against tailored subsets of the main dataset.

## Data Analysis Examples

The data is now clean and can be used.
The dataset is huge but versatile. Rather than using it on its whole scale, its content can be filtered to create new lighter datasets for specific purposes.
It can be used, for example, for extracting trends grouped by commercial category or number of employees.
The data may also be used to evaluate the state of wealth in a certain region.

## Data Analysis: A Simple Case Study

Recently, my partner and I decided to paint our home. It is a set of 4 buildings that would require us to spend most probably more time than a year of holidays. So, we started to look for a good house painting company in Uppsala’s county.
I’ve got the feeling that the dataset could provide interesting insights about which painting companies better meet our needs.

## Data Visualization

I plotted the companies as dots of different sizes (corresponding to the number of employees) on a Cartesian space of solvency and quick ratio and then split into 4 areas according to the median values. In the lower left square, there are, let’s say, "bad" companies while in the upper right, there are "good" ones (so high solvency and quick ratio).
Is it more convenient to choose a company with low solvency and low quick ratio, which is probably facing a crisis and may likely accept a lower rate, or a wealthy one? Better a small company or a big one?

## Classification

I was wondering which companies would likely pay dividends. Having such information for the past years and lots of parameters, it may be possible to make an accurate prediction.
Let’s say that my goal is to outperform my friend Giovanni. Giovanni is a guy known for not being very smart. If he would have to predict which companies pay dividends, he would tend to apply the simplest possible rule, shaking his hands: "If they paid last year, then they will do it this year as well!"
By simulating such a rudimental model in Excel, it turns out that he gets an F1-score of 70%. That will be my threshold to discriminate between a good classification model and a bad one!

## Classification: Logistic Regression

Since my purpose is a binary classification, I chose to build a model with logistic regression because of its good compromise between lightness and performance.
The model is basically made of the following parts:
- Read the data.
- Feature engineering: creation, reduction, selection, combination, and scaling.
- Balance and weigh the classes.
- Double search of optimal parameters: first a random search across logistic regression parameters; then a refined grid search around the previous result.
- Training and testing are performed on a reasonable subset of the dataset; the model built on this subset is then used to predict the whole dataset.
The final result is a fair F1-score of 85%, so the model works considerably better than Giovanni!

## Clustering

Another thing I wanted to do is a basic clustering of the companies considering net revenue, EBITDA, and solvency.
For this example, I limited the clustering to a subset containing only 20,000 randomly chosen active companies with 1 employee.
K-means was used but before applying the algorithm some pre-processing actions were taken:
- IQR technique to remove outliers (so keeping values between 25% and 75% quantiles).
- Scaling with RobustScaler.
- PCA to reduce the dimensionality.
The results were evaluated with the Silhouette score. The best result was obtained with 8 clusters with a score of 69%.

## Clustering: Interpretation

8 clusters may appear excessive for such a limited task against EBITDA, solvency, and net revenue.
However, the clustering itself isn’t necessarily meant to provide human-readable information; rather, it can be used as a middle step in a bigger classification model. By creating a feature with a cluster number, a classification model may improve its performance.
A brief look at the statistics of the clusters reveals that, for example, cluster 4 is made up of quite homogenous companies (since all three standard deviations are relatively low) with general low performance but very high solvency.

## Contents

The first commit contains the following files:
1) "Swedish companies scraper.py" - Web-scraper, retrieves data about swedish joint-stock companies
2) "Swedish companies dataset.py" - Script for data cleaning
3) "Swedish companies dataset - Data analysis.py" - Script containing examples of data analysis
4) "Swedish companies predictions - Classification.py" - A complete example of classification problem
5) "Swedish companies predictions - Clustering.py" - A simple example of clustering problem
6) "Swedish companies dataset - With a bunch of machine learning techniques.pptx" - A comprehensible illustration of the work, in PowerPoint
7) "Swedish companies dataset - With a bunch of machine learning techniques.pdf" - A comprehensible illustration of the work, in PDF format
