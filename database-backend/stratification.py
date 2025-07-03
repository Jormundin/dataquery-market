"""
Stratification module for creating balanced data groups
Integrated from external stratification service
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import StratifiedKFold, StratifiedShuffleSplit
from scipy.stats import ks_2samp, chi2_contingency
from typing import List, Dict, Any, Tuple, Optional
from pydantic import BaseModel, Field, ValidationError


class StratificationRequest(BaseModel):
    data: list  # The DataFrame data as a list of records (dicts)
    columns: list  # List of column names corresponding to the data
    n_splits: int = Field(None, ge=2, le=10, description="Number of stratified groups (2-10). Not required if split_sizes is provided.")
    stratify_cols: list = Field(..., description="List of column names for stratification.")
    replace_nan: bool = True
    random_state: int = 42
    test_size: float = Field(None, ge=0.0, le=1.0, description="Proportion of the dataset to include in the test sample (0.0 - 1.0).")
    split_sizes: list = Field(None, description="List of proportions for custom splits (e.g., [0.9, 0.1] for 90%/10% split). Must sum to 1.0.")
    ks_test_columns: list = Field(None, description="List of specific columns to monitor for statistical tests (KS test for numeric, Chi-square for categorical). If None, all numeric columns will be used.")
    min_p_value: float = Field(None, ge=0.0, le=1.0, description="Minimum p-value threshold for statistical tests. Stratification will iterate until this threshold is met or exceeded.")
    max_iterations: int = Field(100, ge=1, le=1000, description="Maximum number of iterations to attempt when trying to meet p-value threshold.")
   
    def __init__(self, **data):
        super().__init__(**data)
        # Validate that either n_splits or split_sizes is provided
        if self.n_splits is None and self.split_sizes is None:
            raise ValueError("Either 'n_splits' or 'split_sizes' must be provided.")
        if self.n_splits is not None and self.split_sizes is not None:
            raise ValueError("Provide either 'n_splits' or 'split_sizes', not both.")
        if self.split_sizes is not None:
            if len(self.split_sizes) < 2:
                raise ValueError("split_sizes must contain at least 2 proportions.")
            if not all(0 < size < 1 for size in self.split_sizes):
                raise ValueError("All split sizes must be between 0 and 1.")
            if abs(sum(self.split_sizes) - 1.0) > 1e-6:
                raise ValueError("Split sizes must sum to 1.0.")
            # Set n_splits based on split_sizes length for compatibility
            self.n_splits = len(self.split_sizes)
       
        # Validate min_p_value and ks_test_columns relationship
        if self.min_p_value is not None and self.ks_test_columns is None:
            raise ValueError("When min_p_value is specified, ks_test_columns must also be provided.")


def calculate_statistical_test(original_data: pd.DataFrame, split_data: pd.DataFrame, column_name: str) -> Tuple[float, float, str]:
    """
    Calculate appropriate statistical test based on column type.
    Returns (statistic, p_value, test_type)
    """
    original_col = original_data[column_name]
    split_col = split_data[column_name]
   
    # Check if column is numeric
    if pd.api.types.is_numeric_dtype(original_col):
        # Use Kolmogorov-Smirnov test for numeric data
        statistic, p_value = ks_2samp(original_col, split_col)
        return statistic, p_value, 'ks_test'
    else:
        # Use Chi-square test for categorical data
        try:
            # Create contingency table
            original_counts = original_col.value_counts()
            split_counts = split_col.value_counts()
           
            # Get all unique categories
            all_categories = set(original_counts.index) | set(split_counts.index)
           
            # Create aligned counts (fill missing categories with 0)
            original_aligned = [original_counts.get(cat, 0) for cat in all_categories]
            split_aligned = [split_counts.get(cat, 0) for cat in all_categories]
           
            # Create contingency table
            contingency_table = np.array([original_aligned, split_aligned])
           
            # Perform chi-square test
            if contingency_table.sum() > 0 and len(all_categories) > 1:
                chi2_stat, p_value, dof, expected = chi2_contingency(contingency_table)
                return chi2_stat, p_value, 'chi2_test'
            else:
                # If no variation or insufficient data, return high p-value
                return 0.0, 1.0, 'chi2_test'
               
        except Exception as e:
            # If chi-square test fails, return low p-value to be conservative
            return float('inf'), 0.0, 'chi2_test_failed'


def perform_stratification(df: pd.DataFrame, y: pd.Series, request: StratificationRequest, iteration_seed: Optional[int] = None) -> Tuple[List[pd.DataFrame], List[Dict]]:
    """
    Perform a single stratification operation and return splits with statistical test results.
    """
    X = df.drop(columns=request.stratify_cols)
    seed = iteration_seed if iteration_seed is not None else request.random_state
   
    # Determine which columns to use for statistical testing
    if request.ks_test_columns is not None:
        test_columns = request.ks_test_columns
    else:
        # Default to all numeric columns if none specified
        test_columns = X.select_dtypes(include=np.number).columns.tolist()
   
    if request.split_sizes is not None:
        # Custom split sizes - use multiple StratifiedShuffleSplit operations
        splits = []
        test_scores = []
        remaining_df = df.copy()
        remaining_y = y.copy()
       
        for i, split_size in enumerate(request.split_sizes):
            if i == len(request.split_sizes) - 1:
                # Last split gets all remaining data
                split_df = remaining_df.reset_index(drop=True)
            else:
                # Calculate the proportion relative to remaining data
                relative_size = split_size / sum(request.split_sizes[i:])
               
                sss = StratifiedShuffleSplit(
                    n_splits=1,
                    test_size=relative_size,
                    random_state=seed + i
                )
                remaining_index, split_index = next(sss.split(remaining_df, remaining_y))
               
                split_df = remaining_df.iloc[split_index].reset_index(drop=True)
                remaining_df = remaining_df.iloc[remaining_index].reset_index(drop=True)
                remaining_y = remaining_y.iloc[remaining_index].reset_index(drop=True)
           
            splits.append(split_df)
           
            # Calculate statistical tests for specified columns
            test_dict = {}
            for col in test_columns:
                if col in df.columns:
                    statistic, p_value, test_type = calculate_statistical_test(df, split_df, col)
                    test_dict[col] = {
                        'p_value': p_value,
                        'statistic': statistic,
                        'test_type': test_type
                    }
            test_scores.append(test_dict)
    else:
        # Equal splits - use StratifiedKFold
        skf = StratifiedKFold(n_splits=request.n_splits, shuffle=True, random_state=seed)
        splits = []
        test_scores = []

        for _, test_index in skf.split(X, y):
            split_df = df.iloc[test_index].reset_index(drop=True)
            splits.append(split_df)

            # Calculate statistical tests for specified columns
            test_dict = {}
            for col in test_columns:
                if col in df.columns:
                    statistic, p_value, test_type = calculate_statistical_test(df, split_df, col)
                    test_dict[col] = {
                        'p_value': p_value,
                        'statistic': statistic,
                        'test_type': test_type
                    }
            test_scores.append(test_dict)
   
    return splits, test_scores


def check_p_value_criteria(ks_scores: List[Dict], ks_test_columns: List[str], min_p_value: float) -> bool:
    """
    Check if all specified columns meet the minimum p-value threshold across all splits.
    """
    for split_ks in ks_scores:
        for col in ks_test_columns:
            if col in split_ks and split_ks[col]['p_value'] < min_p_value:
                return False
    return True


def get_min_p_values(ks_scores: List[Dict], ks_test_columns: List[str]) -> Dict[str, Optional[float]]:
    """
    Get the minimum p-values for each specified column across all splits.
    """
    min_p_values = {}
    for col in ks_test_columns:
        min_p = float('inf')
        for split_ks in ks_scores:
            if col in split_ks:
                min_p = min(min_p, split_ks[col]['p_value'])
        min_p_values[col] = min_p if min_p != float('inf') else None
    return min_p_values


def stratify_data(request_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Main stratification function that replicates the /stratify endpoint functionality
    """
    try:
        # Create StratificationRequest from input data
        request = StratificationRequest(**request_data)
    except (ValidationError, ValueError) as e:
        raise ValueError(f"Invalid stratification request: {str(e)}")

    # Reconstruct the DataFrame from the input data
    try:
        df = pd.DataFrame(data=request.data, columns=request.columns)
    except Exception as e:
        raise ValueError(f"Error reconstructing DataFrame: {e}")

    # Validate n_splits (after it's been set by __init__ if split_sizes was provided)
    if not (2 <= request.n_splits <= 10):
        if request.split_sizes is not None:
            raise ValueError("Number of split_sizes must be between 2 and 10.")
        else:
            raise ValueError("n_splits must be between 2 and 10.")

    # Validate stratify_cols
    stratify_cols_list = request.stratify_cols
    if not stratify_cols_list:
        raise ValueError("Please provide at least one stratification column.")
    for col in stratify_cols_list:
        if col not in df.columns:
            raise ValueError(f"Column '{col}' not found in the DataFrame.")

    # Validate ks_test_columns if provided
    if request.ks_test_columns is not None:
        for col in request.ks_test_columns:
            if col not in df.columns:
                raise ValueError(f"Test column '{col}' not found in the DataFrame.")

    # Handle NaN values in stratification columns
    if request.replace_nan:
        for col in stratify_cols_list:
            if df[col].isnull().any():
                if df[col].dtype in [np.float64, np.int64]:
                    df[col].fillna(0, inplace=True)
                elif df[col].dtype == object:
                    df[col].fillna('None', inplace=True)
                else:
                    df[col].fillna('None', inplace=True)

    # Combine stratification columns
    y = df[stratify_cols_list].astype(str).agg('_'.join, axis=1)

    # Remove strata with insufficient samples
    min_samples = request.n_splits
    if request.test_size and request.test_size > 0:
        min_samples += 1  # Need at least one sample for the test set
    value_counts = y.value_counts()
    insufficient_strata = value_counts[value_counts < min_samples].index.tolist()

    if insufficient_strata:
        df = df[~y.isin(insufficient_strata)]
        y = y[~y.isin(insufficient_strata)]

    if y.nunique() < 2:
        raise ValueError("Not enough unique strata after removing insufficient ones.")

    # Split off test set if test_size is specified
    if request.test_size and request.test_size > 0:
        sss = StratifiedShuffleSplit(n_splits=1, test_size=request.test_size, random_state=request.random_state)
        train_index, test_index = next(sss.split(df, y))
        test_df = df.iloc[test_index].reset_index(drop=True)
        df = df.iloc[train_index].reset_index(drop=True)
        y = y.iloc[train_index].reset_index(drop=True)
    else:
        test_df = None

    # Perform stratified splitting on the remaining data
    X = df.drop(columns=stratify_cols_list)
   
    # Determine which columns to use for KS testing
    if request.ks_test_columns is not None:
        test_columns = request.ks_test_columns
    else:
        test_columns = X.select_dtypes(include=np.number).columns.tolist()
   
    # Perform stratification with optional iterative p-value checking
    if request.min_p_value is not None and request.ks_test_columns is not None:
        # Iterative approach to meet p-value criteria
        best_splits = None
        best_ks_scores = None
        best_min_p_values = None
        iteration = 0
        criteria_met = False
       
        while iteration < request.max_iterations and not criteria_met:
            # Use different random seed for each iteration
            iteration_seed = request.random_state + iteration * 1000
           
            # Perform stratification
            splits, test_scores = perform_stratification(df, y, request, iteration_seed)
           
            # Check if p-value criteria are met
            criteria_met = check_p_value_criteria(test_scores, test_columns, request.min_p_value)
           
            # Get minimum p-values for tracking
            min_p_values = get_min_p_values(test_scores, test_columns)
           
            # Keep track of the best result so far (highest minimum p-values)
            if best_splits is None or (best_min_p_values and min_p_values):
                current_min = min([p for p in min_p_values.values() if p is not None])
                best_min = min([p for p in best_min_p_values.values() if p is not None]) if best_min_p_values else 0
               
                if current_min > best_min:
                    best_splits = splits
                    best_ks_scores = test_scores
                    best_min_p_values = min_p_values
           
            iteration += 1
       
        # Use the best result found
        splits = best_splits
        test_scores = best_ks_scores
       
        # Add iteration information to response
        iteration_info = {
            'iterations_performed': iteration,
            'criteria_met': criteria_met,
            'target_p_value': request.min_p_value,
            'achieved_min_p_values': best_min_p_values,
            'max_iterations': request.max_iterations
        }
    else:
        # Single stratification without p-value criteria
        splits, test_scores = perform_stratification(df, y, request)
        iteration_info = None

    # Prepare the response
    stratified_data = []
    total_rows = df.shape[0]
   
    for i, split_df in enumerate(splits):
        # Convert DataFrame to JSON
        split_data_json = split_df.to_dict(orient='records')
        actual_proportion = split_df.shape[0] / total_rows
       
        stratum_info = {
            'stratum': i + 1,
            'data': split_data_json,
            'num_rows': split_df.shape[0],
            'proportion': round(actual_proportion, 4),
            'test_statistics': test_scores[i]
        }
       
        # Add requested proportion if custom split sizes were used
        if request.split_sizes is not None:
            stratum_info['requested_proportion'] = request.split_sizes[i]
           
        stratified_data.append(stratum_info)

    response = {
        'n_splits': request.n_splits,
        'stratify_cols': stratify_cols_list,
        'ks_test_columns': test_columns,
        'stratified_groups': stratified_data,
        'total_rows': total_rows,
        'message': 'Stratification successful.'
    }
   
    # Add split method information
    if request.split_sizes is not None:
        response['split_method'] = 'custom_proportions'
        response['requested_split_sizes'] = request.split_sizes
    else:
        response['split_method'] = 'equal_kfold'

    # Include test set in the response if applicable
    if test_df is not None:
        # Convert test DataFrame to JSON
        test_data_json = test_df.to_dict(orient='records')
        # Calculate KS statistics for the test set using specified columns
        ks_dict_test = {}
        for col in test_columns:
            if col in df.columns:
                statistic, p_value, test_type = calculate_statistical_test(df, test_df, col)
                ks_dict_test[col] = {
                    'p_value': p_value,
                    'statistic': statistic,
                    'test_type': test_type
                }
        response['test_set'] = {
            'data': test_data_json,
            'num_rows': test_df.shape[0],
            'test_statistics': ks_dict_test
        }

    # Add iteration information to response
    if iteration_info:
        response['iteration_info'] = iteration_info

    return response 