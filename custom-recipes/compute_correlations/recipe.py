# Code for custom code recipe compute_correlations (imported from a Python recipe)

# To finish creating your custom recipe from your original PySpark recipe, you need to:
#  - Declare the input and output roles in recipe.json
#  - Replace the dataset names by roles access in your code
#  - Declare, if any, the params of your custom recipe in recipe.json
#  - Replace the hardcoded params values by acccess to the configuration map

# See sample code below for how to do that.
# The code of your original recipe is included afterwards for convenience.
# Please also see the "recipe.json" file for more information.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *


#############################
# Your original recipe
#############################

# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from compute_corr import *

# Retrieve array of dataset names from 'input' role, then create datasets
input_names = get_input_names_for_role('input')
input_datasets = [dataiku.Dataset(name) for name in input_names]

# For outputs, the process is the same:
output_names = get_output_names_for_role('main_output')
output_datasets = [dataiku.Dataset(name) for name in output_names]

# Retrieve parameter values from the of map of parameters
threshold = get_recipe_config()['threshold']


# Read recipe inputs
input_dataset = input_datasets[0]
df = input_dataset.get_dataframe()
column_names = df.columns



output = compute_corr(df, threshold)


# Write the output to the output dataset
output_dataset =  output_datasets[0]
output_dataset.write_with_schema(pd.DataFrame(output))