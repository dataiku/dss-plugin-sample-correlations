import pandas as pd, numpy as np


def compute_pairs(df):
    # We'll only compute correlations on numerical columns
    # So extract all pairs of names of numerical columns
    pairs = []
    column_names = df.columns
    for i in xrange(0, len(column_names)):
        for j in xrange(i + 1, len(column_names)):
            col1 = column_names[i]
            col2 = column_names[j]
            if df[col1].dtype == "float64" and \
               df[col2].dtype == "float64":
                pairs.append((col1, col2))
    return pairs

def compute_corr(df,threshold):
    # Compute the correlation for each pair, and write a
    # row in the output array
    output = []
    pairs = compute_pairs(df)
    for pair in pairs:
        corr = df[[pair[0], pair[1]]].corr().iloc[0][1]
        if np.abs(corr) > threshold:
          output.append({"col0" : pair[0],
                         "col1" : pair[1],
                         "corr" :  corr})
    return output

