# Valerio Malerba, Uppsala, 2024

from imblearn.under_sampling import RandomUnderSampler
import pandas as pd
from sklearn.model_selection import train_test_split, GridSearchCV, RandomizedSearchCV
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
from sklearn.preprocessing import StandardScaler
from sklearn.feature_selection import SelectKBest, f_classif
import numpy as np
import pickle

def Weighted_mean(column_root, df, decay_factor=5):
    relevant_columns = [col for col in df.columns if col.startswith(column_root + "_") and '_20' in col and not col.endswith('22')]
    weights = pd.DataFrame(index=df.index)
    for year in range(2012, 2022):
        weights[column_root + "_" + str(year)] = 0
    for col in weights.columns:
        feature_year = int(weights[col].name.split("_")[1])
        column_number = weights.columns.get_loc(col)
        number_of_columns = len(weights.columns)
        weights[col] = 1 / (number_of_columns - column_number / decay_factor) * (df['registration year'] <= feature_year)
    relevant_df = pd.DataFrame(df, columns=relevant_columns)
    weighted_relevant_df = (relevant_df.mul(weights)).div(weights.sum(axis=1), axis=0)
    df[column_root] = weighted_relevant_df.sum(axis=1)
    df = df.drop(columns=relevant_columns)
    return df

def Save_model(filename, model):
    import pickle
    with open(filename + '.pkl', 'wb') as f:
        pickle.dump(model, f)

load_model = False
predicting_feature = 'Dividends_2022'
random_state = 79
number_of_samples = 250
number_of_selected_features = 2
cross_validation = 3
random_iterations = 4
max_iterations = 6000
scoring = 'f1'

print("Reading source file...")
df = pd.read_csv("ML_data 14x6 regions - -1 items.csv", index_col="organization number")
df = df[df['Status'] == 2] # Status not equal to 2 indicates inactive companies

# Feature engineering: some new features are defined ad hoc to help the model to classify correctly.
# Note: the thresholds have been defined according to a statistical analysis performed in another script.
df['Dividends_last_year>10'] = df['Dividends_2021'] > 10
df['High solvency'] = df['Solvency_2021'] > 40
df['Profit after financial items>81'] = df['Profit after financial items_2021'] > 81

# Here is assumed that older balance sheets should have less importance, so each feature type is reduced to one by computing a weighted average over the years
# Also, the features of the same type but of adjacent years are often highly correlated, so they would give a similar contribute to the model.
# This reduction makes the model much easier to train, hopefully without affecting the quality too much.
to_be_weighted = list(set([col.split("_")[0] for col in df.columns if '_20' in col and not col.endswith('22')]))
for col in to_be_weighted:
    df = Weighted_mean(col, df)

# Now the dataset contains only an average of each feature type and the corresponding ones for the year 2022. These ones
# will be used separately for testing the model, which will be trained against the averaged features and/or the new ad hoc features.
if not load_model:
    print("Training model to predict", predicting_feature, "with", number_of_samples, "samples.")
    # Define a subset of the dataset
    df_samples = df.sample(n=number_of_samples, random_state=random_state)
    # Convert the target feature to a categorical one, to allow classification
    categorized_feature = predicting_feature + "_cat"
    target_feature = df_samples[predicting_feature].apply(lambda x: 0 if x <= 0 else 1)
    df_samples[categorized_feature] = target_feature

    # The columns containing the last year will be used to test the model, so they are excluded from the train dataset
    columns_except_2022 = [col for col in df_samples.columns if '_2022' not in col or col == categorized_feature]
    df_train = df_samples[columns_except_2022]

    # If in this smaller dataframe contains columns with constant values (thus bringing no relevant information to the model), they are removed.
    print("Removing constant columns...")
    df_train = df_train.loc[:, (df_train != df_train.iloc[0]).any()]  # Remove constant columns

    # Undersampling the train sets to balance the classes
    print("Balancing", categorized_feature)
    try:
        rus = RandomUnderSampler(random_state=random_state)
        X_resampled, Y_resampled = rus.fit_resample(df_train.drop(columns=categorized_feature), df_train[categorized_feature])
        print(f"Class distribution after resampling: {np.bincount(Y_resampled)}")
    except Exception as e:
        print("Error during resampling:", e)
        exit

    # To make the model even lighter and more efficient, only the most meaningful features are kept
    print("Selecting best", number_of_selected_features, "features:")
    selector = SelectKBest(f_classif, k=number_of_selected_features)
    X_selected = selector.fit_transform(X_resampled, Y_resampled)
    selected_features = df_train.drop(columns=categorized_feature).columns[selector.get_support()]

    scores = selector.scores_
    pvalues = selector.pvalues_
    feature_scores = pd.DataFrame({
        'Feature': df_train.drop(columns=categorized_feature).columns,
        'F-Score': scores,
        'P-Value': pvalues
    })
    feature_scores = feature_scores.sort_values(by='F-Score', ascending=False)
    print("\nBest feature scores:\n", feature_scores.head(number_of_selected_features))

    # Scaling the features    
    print("Scaling features...")
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(pd.DataFrame(X_selected, columns=selected_features))

    X_train, X_test, Y_train, Y_test = train_test_split(X_scaled, Y_resampled, test_size=0.2, random_state=random_state)

    # Definition of a wide grid of parameters: according to the setting n_iter in the randomized search engine, only a bunch of them will be tried
    random_param_grid = {
        'C': np.logspace(-5, 2, num=100),
        'penalty': ['l1', 'l2'],
        'solver': ['liblinear', 'saga']
    }
    # Perform the training by assigning different weights to the classes, to make the model predicting in a balanced way
    # (e. g. if the test set is made of 50% of class 0 and 50% in class 1 should give 50% of predictions in class 0 and 50% in 1)
    for w in [2.5]:
        print("Trying with class 0 weight equal to", w)
        class_weight = {0: w, 1: 1}
        # Random search
        print("Performing random search...")
        random_search = RandomizedSearchCV(LogisticRegression(random_state=random_state, max_iter=max_iterations, class_weight=class_weight),
                                            param_distributions=random_param_grid,
                                            n_iter=random_iterations,
                                            scoring=scoring,
                                            cv=cross_validation,
                                            random_state=random_state)
        random_search.fit(X_train, Y_train)
        best_random_model = random_search.best_estimator_

        # Grid search around the results of random search
        print("Exploring the space around the best random combination...")
        param_grid = {
            'C': [best_random_model.C * 0.8, best_random_model.C * 0.9, best_random_model.C, best_random_model.C * 1.1, best_random_model.C * 1.2],
            'penalty': [best_random_model.penalty],
            'solver': [best_random_model.solver]
        }
        grid_search = GridSearchCV(LogisticRegression(random_state=random_state, max_iter=max_iterations, class_weight=class_weight),
                                   param_grid=param_grid,
                                   cv=cross_validation,
                                   scoring=scoring)
        grid_search.fit(X_train, Y_train)
        
        # After this second search, the model is ready
        best_grid_model = grid_search.best_estimator_
        print("Best parameters:", grid_search.best_params_)
        Y_pred = best_grid_model.predict(X_test)

        # The model has been trained against a fraction of the whole dataset. Now it's time to see how it performs when predicting the classes
        # in the whole dataset
        print("\nNow predicting against the whole dataset...")
        df_aligned = df[selected_features]
        X_full = scaler.transform(df_aligned)
        df[categorized_feature] = df[predicting_feature].apply(lambda x: 0 if x <= 0 else 1)
        Y_full = df[categorized_feature]
        Y_pred_full = pd.Series(best_grid_model.predict(X_full))
        total_accuracy = round(accuracy_score(Y_full, Y_pred_full), 3)
        total_precision = round(precision_score(Y_full, Y_pred_full, average='weighted'), 3)
        total_recall = round(recall_score(Y_full, Y_pred_full, average='weighted'), 3)
        total_f1 = round(f1_score(Y_full, Y_pred_full, average='weighted'), 3)
        model_name = "LR - Target=" + predicting_feature + " RS=" + str(random_state) + " NoS=" + str(number_of_samples) + " F=" + str(number_of_selected_features) + " CV=" + str(cross_validation) + " RI=" + str(random_iterations) + " MI=" + str(max_iterations) + " Scoring=" + scoring + " Score=" + str(total_f1)
        print("Results for", model_name)
        print(f"Accuracy:\t{total_accuracy}")
        print(f"Recall:\t\t{total_recall}")
        print(f"Precision:\t{total_precision}")
        print(f"F1 Score:\t{total_f1}")
        print(f"Class distribution in full dataset: {np.bincount(Y_full)}")
        Y_pred_full.index = Y_full.index
        Y_diff = Y_full - Y_pred_full
        print("Unpredicted dividends:", (Y_diff >= 1).sum())
        print("Mispredicted dividends:", (Y_diff <= -1).sum())
        print("Ratio", round((Y_diff >= 1).sum() / (Y_diff <= -1).sum(), 1), "\n\n\n\n")
else:
    print("Loading model...")
    with open(model_name + '.pkl', 'rb') as f:
        best_grid_model = pickle.load(f)