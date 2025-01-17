from concurrent.futures import ThreadPoolExecutor, as_completed
import re

import pandas as pd
from preprocess_text import is_10K
from pydantic_core import InitErrorDetails
from tqdm import tqdm
import regex
import os
import spacy
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import word_tokenize

#nltk
lemmatizer = WordNetLemmatizer()

# spacy
nlp = spacy.load('en_core_web_sm')
nlp.max_length = 2000000  

def lemmatize_test(excerpt):
    tokens = word_tokenize(excerpt)
    lemmatized_tokens = []
    for token in tokens:
        print(token)
        lemmatized_tokens.append(lemmatizer.lemmatize(token))
        
    lemmatized_text = " ".join(lemmatized_tokens)

    print("ran :", lemmatizer.lemmatize("ran", pos='v'))
    return lemmatized_text

def lemmatize_text(text, filepath):
    '''
    Lemmatize the text using SpaCy and remove stop words.
    Args:
        text (str): The content of the text file.
        filepath (str): The file path of the text file.
    Returns:
        tuple: Initial length, is10K, final length, lemmatized text, filepath.
    '''
    try:
        initial_length = len(text)

        # Check if the file is a 10-K report
        is10K = '10-K' in text.upper()

        # process the text with SpaCy
        doc = nlp(text)
        lemmatized_tokens = [
            token.lemma_ for token in doc
            if not token.is_stop and not token.is_punct and token.is_alpha and len(token) > 2
        ]
        lemmatized_text = ' '.join(lemmatized_tokens)
        initial_token_count = len(doc)
        final_token_count = len(lemmatized_tokens)
        final_length = len(lemmatized_text)
        return initial_length, is10K, final_length, initial_token_count, final_token_count, lemmatized_text, filepath
    
    except Exception as e:
        raise RuntimeError(f'Error processing file {filepath}: {e}')
    

def lemmatize_directory(input_dir, output_dir):
    txt_files = []
    
    # Collect all txt files from the directory and subdirectories
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.txt') and 'run_report' not in file:
                txt_files.append(os.path.join(root, file))

    total_files = len(txt_files)
    failed_files = []
    run_report = {
        'total_files': total_files,
        'failed_files' : [],
        'details': []
    }
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for txt_file in txt_files:
            output_subdir = os.path.join(output_dir, os.path.relpath(os.path.dirname(txt_file), input_dir))
            os.makedirs(output_subdir, exist_ok=True) # Ensure output subdirectory exists
            
            # Submit file content and name for processing
            with open(txt_file, 'r', encoding='utf-8') as file:
                file_content = file.read()
            futures.append(executor.submit(lemmatize_text, file_content, txt_file))
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Lemmatizing txt files"):
            try:
                initial_length, is10K, final_length, initial_token_count, final_token_count, preprocessed_text, filepath = future.result()

                # Define output path
                relative_path = os.path.relpath(filepath, input_dir)
                output_path = os.path.join(output_dir, relative_path)

                # Save preprocessed text to the corresponding subdirectory
                with open(output_path, 'w', encoding = 'utf-8') as output_file:
                    output_file.write(preprocessed_text)

                
                run_report['details'].append({
                    'file': filepath,
                    'initial length': initial_length,
                    'final length': final_length,
                    'initial tokens': initial_token_count,
                    'final tokens' : final_token_count,
                    'chars removed' : initial_length - final_length,
                    'tokens removed' : initial_token_count - final_token_count,
                    '10K' : is10K,
                    'status': 'Success'
                })

            except Exception as e:
                failed_files.append((filepath, e))
                

    # Create the run report file
    report_file_path = os.path.join(output_dir, "run_report.txt")
    with open(report_file_path, 'w') as report_file:
        report_file.write("Run Report\n")
        report_file.write(f"Total Files - {run_report['total_files']}\n")
        
        if failed_files:
            report_file.write("Failed Files\n")
            for filepath, error in failed_files:
                report_file.write(f" - {filepath} with error - {error}\n")

        report_file.write("\nDetailed Report\n")
        for detail in run_report['details']:
            if detail['10K'] == True:
                report_file.write(f"File: {detail['file']}, "
                              f"      Initial length: {detail['initial length']} char, "
                              f"Final length: {detail['final length']} char, "
                              f"Chars removed: {detail['chars removed']}, "
                              f"Initial tokens: {detail['initial tokens']} tokens,"
                              f"Final tokens: {detail['final tokens']} tokens,"
                              f"token removed: {detail['tokens removed']},"
                              f"is_10K: {detail['10K']}, "
                              f"Status: {detail['status']}\n")
        for detail in run_report['details']:
            if detail['10K'] == False:
                report_file.write(f"File: {detail['file']}, "
                              f"      Initial length: {detail['initial length']} char, "
                              f"Final length: {detail['final length']} char, "
                              f"Chars removed: {detail['chars removed']}, "
                              f"Initial tokens: {detail['initial tokens']} tokens,"
                              f"Final tokens: {detail['final tokens']} tokens,"
                              f"token removed: {detail['tokens removed']},"
                              f"is_10K: {detail['10K']}, "
                              f"Status: {detail['status']}\n")
    
    print(f"Run report saved to {report_file_path}")

    return run_report, report_file_path

def process_row(row):
    pattern = r'([^:]+):\s*([^,]+)'
    return {key.lstrip(" ,").lower().replace(" ", "_") : value.rstrip(" c").strip() for key, value in dict(re.findall(pattern, row)).items()}

def extract_dataframes_from_run_report(filepath):
    with open(filepath, 'r') as report:
        lines = report.readlines()
    
    success_data = []
    for line in lines:
        if ((line.isspace() or not re.search(r'([^:]+):\s*([^,]+)', line)) and (len(success_data) == 0)):
            continue

        if not re.search(r'([^:]+):\s*([^,]+)', line) and len(success_data) > 0:
            break

        row_data = process_row(line)
        if row_data:
            success_data.append(row_data)
    
    if success_data:
        columns = list(success_data[0].keys())
        df_success = pd.DataFrame(success_data, columns=columns)
    else:
        df_success = pd.DataFrame()
    
    return df_success

def output_df(df: pd.DataFrame, output_dir, name: str):
    df = df.sort_values(by='file', ascending=False)
    report_file_path = os.path.join(output_dir, name)
    df.to_csv(report_file_path)
    return

def main():
    # input_dir = '10-K_or_equivalent-txt-preprocessed'  # Input directory containing txt files
    # output_dir = '10-K_or_equivalent-txt-preprocessed-lemmatized'# output directory containing processed txt files
    input_dir = '10-K_or_equivalent-txt-preprocessed'
    output_dir = '10-K_or_equivalent-txt-preprocessed-lemmatized'
    run_report, report_filepath = lemmatize_directory(input_dir, output_dir)
    df_success = extract_dataframes_from_run_report(report_filepath)
    # print(df_success)
    output_df(df_success, output_dir, "success_report.csv")

    # lemmatize_directory(input_dir, output_dir)
    # Further processing can be done with run_report if needed
    return

if __name__ == "__main__":
    main()