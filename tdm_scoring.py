import re
import traceback

from sklearn.preprocessing import MinMaxScaler
import numpy as np
from streamlit import columns
from traitlets import Bool
from word_list import get_vocabulary, get_vocabulary_df
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
import pandas as pd

def create_tdm_dict(dir: str) -> dict[str:pd.DataFrame]:
    tdm_dict = {}
    for file in os.listdir(dir):
        if file.lower().endswith('.csv'):
            match = re.search(r'(\d{4})', file)
            year = match.group(1)
            tdm_path = os.path.join(dir, file)
            df = pd.read_csv(tdm_path, index_col=0)
            tdm_dict[year] = df
    return tdm_dict

def scoring_yearly_tdms(input_dir, TDM_output_dir, scores_output_dir):
    '''
    Score companies based on yearly TDMs in TDMs/ folder.
    
    Args: 
        input_dir(str): Input directory containing TDMs organized by year
        output_dir (str): output directory for final scores
        
    Returns:
        dict: a report containing information about scored files'''

    scores = {}
    token_count_df = extract_document_token_counts('10-K_or_equivalent-txt-preprocessed-lemmatized\success_report.csv')
    os.makedirs(TDM_output_dir, exist_ok=True)
    os.makedirs(scores_output_dir, exist_ok=True)
    tdm_dict = create_tdm_dict(input_dir)
    with ThreadPoolExecutor() as executor:
        futures = []
        for year, tdm_df in tdm_dict.items():
            if tdm_df is not None:
                futures.append(executor.submit(scoring_tdm, tdm_df, token_count_df, year)) 

        for future in tqdm(as_completed(futures), total = len(futures), desc='Scoring yearly TDMs'):
            try:
                tdm_df, scores_df, year, num_compaies, num_files = future.result()
                tdm_df.to_csv(os.path.join(TDM_output_dir, f'tdm_{year}.csv'))
                scores_df.to_csv(os.path.join(scores_output_dir, f'scores_{year}.csv'))
                scores[year] = scores_df
                
            except Exception as e:
                print(f'Error scoring year - {year}: {e}')
                #traceback.print_exc()

    return scores

def scoring_tdm(tdm_df: pd.DataFrame, token_count_df: pd.DataFrame, year: str):
    tokens_tdm_df = merge_tdm_with_token_counts(tdm_df, token_count_df)
    mapped_tdm_df = apply_mapping_to_tdm(tokens_tdm_df)
    norm_df = normalize_tdm_by_token_count(mapped_tdm_df)
    mapped_norm_collapsed_tdm_df, num_companies = ticker_collapse_tdm(norm_df)
    #print(mapped_norm_collapsed_tdm_df.columns)
    yearly_norm_final_tdm_df = normalize_tdm_by_yearly_statistics(mapped_norm_collapsed_tdm_df)

    CSR_category_scores_df = compute_CSR_category_scores(yearly_norm_final_tdm_df, weight=False)
    return yearly_norm_final_tdm_df, CSR_category_scores_df, year, num_companies, len(yearly_norm_final_tdm_df) 

def merge_tdm_with_token_counts(tdm_df: pd.DataFrame, token_count_df: pd.DataFrame)-> pd.DataFrame:
    tokens_tdm_df = pd.merge(tdm_df, token_count_df[['final_tokens']], left_index=True, right_index=True, how='inner')
    tokens_tdm_df = tokens_tdm_df[['final_tokens'] + [col for col in tokens_tdm_df.columns if col != 'final_tokens']]
    return tokens_tdm_df

def apply_mapping_to_tdm(tdm_df: pd.DataFrame):
    '''Map terms to hierarchy and add the mapping information to the TDM'''

    mapped_terms = map_terms_to_hierarchy(tdm_df)

    mapping_df = pd.DataFrame.from_dict(mapped_terms, orient='index')
    mapping_df.index.name = 'Term'

    tdm_df = tdm_df.T
    tdm_df = tdm_df.merge(mapping_df, left_index=True, right_index=True, how='left')

    return tdm_df.T

def map_terms_to_hierarchy(tdm_df: pd.DataFrame):
    '''
    Map the terms in the TDM Dataframe to their hierarchical levels (Topic, Category, Subcategory)
    using the vocabulary DataFrame
    '''
    # Create a dictionary to store the hierarchical mapping of each term
    term_hierarchy_mapping = {}
    
    # Loop through the vocabulary DataFrame and assign hierarchy levels
    for _, row in vocab_df.iterrows():
        topic = row['Topic']
        category = row['Category']
        subcategory = row['Subcategory']
        keywords = row['Keywords']

        # Map each keyword to its hierarchical levels
        for keyword in keywords:
            term_hierarchy_mapping[keyword] = {
                'Topic': topic,
                'Category': category,
                'Subcategory': subcategory
            }
    
    # Now, map the terms in the TDM DataFrame to the hierarchical levels
    mapped_terms = {}
    for term in tdm_df.columns:
        if term in term_hierarchy_mapping:
            mapped_terms[term] = term_hierarchy_mapping[term]
        else:
            mapped_terms[term] = {
                'Topic': None,
                'Category': None,
                'Subcategory': None
            }
    
    return mapped_terms

# Norm 1
def ticker_collapse_tdm(tdm_df: pd.DataFrame):
    tdm_df['document name'] = tdm_df.index
    tdm_df['ticker'] = tdm_df['document name'].str.extract(r'^([A-Z]{3,})')
    tdm_df['ticker'] = tdm_df['ticker'].fillna('None')

    # Separate hierarchy rows
    hierarchical_rows = tdm_df.iloc[-3:].copy(deep=True)
    tdm_df = tdm_df.iloc[:-3].copy(deep=True)

    # Aggregate count data
    count_columns = tdm_df.columns.difference(['ticker', 'document name'])
    aggregated_df = tdm_df.groupby('ticker').agg({
        **{col: 'mean' for col in count_columns},
        'document name': lambda x: "; ".join(x)
    })
    num_companies = len(aggregated_df)

    final_df = pd.concat([aggregated_df, hierarchical_rows], axis=0)
    final_df = final_df[['document name'] + [col for col in final_df.columns if col != 'document name']]
    return final_df, num_companies

def extract_document_token_counts(filepath):
    df = pd.read_csv(filepath)
    df = df[['file', 'final_tokens']]
    df['final_tokens'] = df['final_tokens'].str.replace(' tokens', '').astype(int)
    df['file'] = df['file'].apply(os.path.basename)
    df = df.set_index('file')
    return df

def normalize_tdm_by_token_count(tdm_df: pd.DataFrame)-> pd.DataFrame:
    vocab_hierarchy = tdm_df.tail(3)
    tdm_df_numeric = tdm_df.drop(vocab_hierarchy.index)
    tdm_df_normalized = tdm_df_numeric.div(tdm_df_numeric['final_tokens'] / len(vocab), axis=0)
    tdm_df_normalized = pd.concat([tdm_df_normalized, vocab_hierarchy], axis=0)
    tdm_df_normalized = tdm_df_normalized.drop(columns='final_tokens')
    return tdm_df_normalized

def normalize_tdm_by_yearly_statistics(tdm_df: pd.DataFrame)-> pd.DataFrame:
    vocab_hierarchy = tdm_df.tail(3)
    vocab_hierarchy = vocab_hierarchy.drop(columns=['document name', 'ticker'])
    tdm_df_no_hierarchy = tdm_df.drop(vocab_hierarchy.index)
    document_name = tdm_df_no_hierarchy['document name']
    tdm_df_numeric = tdm_df_no_hierarchy.drop(columns=['document name', 'ticker'])
    scaler = MinMaxScaler(feature_range=(0,100))
    tdm_df_normalized = pd.DataFrame(
        scaler.fit_transform(tdm_df_numeric),
        columns=tdm_df_numeric.columns,
        index=tdm_df_numeric.index
    )

    # year_mean = tdm_df_numeric.mean()
    # year_std_dev = tdm_df_numeric.std()
    # year_std_dev = np.where(year_std_dev == 0, 1, year_std_dev)
    
    # tdm_df_normalized = (tdm_df_numeric - year_mean) / year_std_dev
    tdm_df_normalized.insert(0, 'document names', document_name)
    tdm_df_normalized = pd.concat([tdm_df_normalized, vocab_hierarchy], axis=0)
    return tdm_df_normalized

# Compiler
def compute_CSR_category_scores(tdm_df: pd.DataFrame, weight: bool) -> pd.DataFrame:
    if weight:
        return None
    topic_row = tdm_df.loc['Topic']
    # Extract the 'document names' column
    document_names = tdm_df[~tdm_df.index.isin(['Topic', 'Category', 'Subcategory'])]['document names']
    
    # Select rows corresponding to companies (exclude hierarchy rows and 'document names')
    company_rows = tdm_df[~tdm_df.index.isin(['Topic', 'Category', 'Subcategory'])].drop(columns=['document names'])
    
    # Initialize an empty DataFrame to store aggregated results
    aggregated_df = pd.DataFrame(index=company_rows.index)
    
    # Aggregate scores for each CSR topic
    for topic in ['Environmental', 'Social', 'Governance']:
        # Select columns belonging to the current topic
        topic_columns = topic_row[topic_row == topic].index
        
        # Sum scores across the selected columns
        aggregated_df[topic] = company_rows[topic_columns].sum(axis=1)
    
    # Add the 'document names' column as the first column
    aggregated_df.insert(0, 'document names', document_names)
    
    return aggregated_df
    
def compile_scores(scores_dict: dict[pd.DataFrame], output_dir: str, timeseries_name: str)-> pd.DataFrame:  
    all_indexes = set()
    for year, df in scores_dict.items():
        all_indexes.update(df.index)
    all_indexes = sorted(all_indexes)

    sorted_years = sorted(scores_dict.keys())
    # Step 2: Reindex each yearly DataFrame and prepare for concatenation
    reindexed_dfs = []
    for year in sorted_years:
        df = scores_dict[year].drop(columns=['document names'])
        
        # Reindex the DataFrame, filling missing entries with NaN
        df_reindexed = df.reindex(all_indexes)
        
        # Create a MultiIndex for the columns: (year, CSR component)
        df_reindexed.columns = pd.MultiIndex.from_product(
            [[year], df.columns],
            names=["Year", "CSR Component"]
        )
        reindexed_dfs.append(df_reindexed)
    
    # Step 3: Concatenate all reindexed DataFrames along columns
    time_series_df = pd.concat(reindexed_dfs, axis=1)
    time_series_df.to_csv(os.path.join(output_dir, timeseries_name))
    

def main():
    input_dir = 'TDMs/'
    parent_output_dir = 'n1_avg_compile_sum/'
    TDMs_output_dir = f'{parent_output_dir}TDMs_mapped_collapsed_normalized'
    scores_output_dir = f'{parent_output_dir}SCORES'
    timeseries_name = 'CSR_n1_avg_compile_sum_scores_timeseries.csv'
    global vocab 
    vocab = get_vocabulary()
    global vocab_df
    vocab_df = get_vocabulary_df()
    
    scores = scoring_yearly_tdms(input_dir, TDMs_output_dir, scores_output_dir)
    compile_scores(scores, parent_output_dir, timeseries_name)

if __name__ == "__main__":
    main()