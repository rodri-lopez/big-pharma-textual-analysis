from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
from cycler import K
import regex as re
import nltk 
import pandas as pd
import os
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

stop_words = set(stopwords.words('english'))

def remove_numbers(text):
    length = len(text)
    text = re.sub(r'\d+', '', text)
    return text, length - len(text)

def remove_page(text):
    # Remove the word 'page' and its variations (case-insensitive)
    text = re.sub(r'\bpage\b', '', text)
    return text

def remove_form_10K(text):
    # Remove the phrase 'form 10-K' and its variations (case-insensitive)
    text = re.sub(r'\bform 10-K\b', '', text, flags=re.IGNORECASE)
    return text

def remove_symbols(text):
    length = len(text)
    text = re.sub(r"[^a-zA-Z\s']", '', text)
    return text, length - len(text)

def remove_stop_words(text):
    tokens = word_tokenize(text)
    filtered_text = [word for word in tokens if word not in stop_words]

    text = " ".join(filtered_text)
    num_stop_words = len(tokens) - len(filtered_text)

    return text, num_stop_words

def pattern_first_seen(text, *patterns):
    indices = []
    for pattern in patterns:
        idx = text.find(pattern.lower())
        if idx != -1:
            indices.append((idx, pattern))
    
    if indices:
        return min(indices, key=lambda x: x[0])
    return -1, None

def check_proximity(text, text_lines, *patterns):
    count = 0
    for line in text_lines:
        count+=1
        for pattern in patterns:
            if line.find(pattern.lower()) != -1:
                first = count
                break

def find_all_matches(text, pattern):
    matches = []
    for match in re.finditer(pattern, text, flags=re.IGNORECASE):
        matches.append(match.start())
    return matches

def first_match_past_relative_location(text, pattern, thresh_loc, length):
    match_indices = find_all_matches(text, pattern)
    for match_idx in match_indices:
        rel_loc = match_idx/length
        if rel_loc > thresh_loc:
            return match_idx
    return -1
    

def check_relative_location(initial_length, idx):
    return idx/initial_length

def remove_text_before_index(text, index):
    if index < 0 or index > len(text):
        raise ValueError('Index out of bounds')
    return text[index:]

def remove_text_after_index(text, index):
    if index < 0 or index > len(text):
        raise ValueError('Index out of bounds')
    return text[:index]

def is_10K(filepath):
    _, _, filename = filepath.rpartition('\\')
    if '10-K' in filename:
        return True
    
    return False


def preprocess_text(text, filepath):
    '''Preprocess a single text file'''
    initial_length = len(text)

    text = text.lower()
    
    header_removed = 0
    signatures_removed = 0
    exhibits_removed = 0
    # text_lines = text.splitlines()
    if (is_10K(filepath)):
        # Step 1: Remove text above 'PART 1' or 'documents incorporated by reference' (SEC Header and table of contents)
        pattern1 = r'part i\([ \n]\)'
        pattern2 = 'documents incorporated by reference'
        idx, first_match = pattern_first_seen(text, pattern1, pattern2)
        length = len(text)
        if idx != -1:
            text = remove_text_before_index(text, idx)
            header_removed = length - len(text)
        
        pattern = 'signatures'
        length = len(text)
        idx = first_match_past_relative_location(text, pattern, 0.95, length)
        if idx != -1:
            text = remove_text_after_index(text, idx)
            signatures_removed = length - len(text)
        
        pattern = 'exhibits'
        length = len(text)
        idx = first_match_past_relative_location(text, pattern, 0.92, length)
        if idx != -1:
            text = remove_text_after_index(text, idx)
            exhibits_removed = length - len(text)



    text = remove_page(text)
    text = remove_form_10K(text)
    text, digits_removed = remove_numbers(text)
    text, symbols_removed = remove_symbols(text)
    text, num_stop_words = remove_stop_words(text)

    final_length = len(text)

    preprocessed_text = text

    return (initial_length,  final_length, header_removed, signatures_removed, exhibits_removed, 
            digits_removed,  symbols_removed, num_stop_words, is_10K(filepath),
            preprocessed_text, filepath)

def preprocess_directory(input_dir, output_dir):
    txt_files = []
    
    # Collect all txt files from the directory and subdirectories
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.txt') and 'run_report' not in file:
                txt_files.append(os.path.join(root, file))

    total_files = len(txt_files)
    run_report = {
        'total_files': total_files,
        'failed_files' : [],
        'details': [],
        'warnings': []
    }
    
    with ThreadPoolExecutor() as executor:
        futures = []
        for txt_file in txt_files:
            
            output_subdir = os.path.join(output_dir, os.path.relpath(os.path.dirname(txt_file), input_dir))
            os.makedirs(output_subdir, exist_ok=True) # Ensure output subdirectory exists
            
            # Submit file content and name for processing
            with open(txt_file, 'r', encoding='utf-8') as file:
                file_content = file.read()

            futures.append(executor.submit(preprocess_text, file_content, txt_file))
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Preprocessing txt files"):
            try:
                (initial_length,  final_length, header_removed, signatures_removed, exhibits_removed,
                    digits_removed, symbols_removed, num_stop_words, is10K,
                    preprocessed_text, filepath) = future.result()
                
                print(f"Processed file: {filepath} - 10-K: {is10K}")
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
                    'chars removed' : initial_length - final_length,
                    'header removed' : header_removed,
                    'signatures removed' : signatures_removed,
                    'exhibits removed' : exhibits_removed,
                    'digits removed' : digits_removed,
                    'symbols removed' : symbols_removed,
                    'stopwords removed' : num_stop_words,
                    '10K' : is10K,
                    'status': 'Success'
                })

                run_report['warnings'].append({
                    'file': filepath,
                    'initial length': initial_length,
                    'final length': final_length,
                    'percentage removed' : (initial_length-final_length)/initial_length
                })

            except Exception as e:
                run_report['failed_files'].append({
                    'file': filepath,
                    'exception' : e
                })

    # Create the run report file
    report_file_path = os.path.join(output_dir, "run_report.txt")
    with open(report_file_path, 'w') as report_file:
        report_file.write("Run Report\n")
        report_file.write(f"Total Files - {run_report['total_files']}\n")
        
        report_file.write("Failed Files\n")
        for failed_file in run_report['failed_files']:
            report_file.write(f" - {failed_file['file']}\n")
            report_file.write(f"{failed_file['exception']}\n")

        report_file.write("\nDetailed Report\n")
        for detail in run_report['details']:
            if detail['10K'] == True:
                report_file.write(f"File: {detail['file']}, "
                              f"      Initial length: {detail['initial length']} c, "
                              f"Final length: {detail['final length']} c, "
                              f"Chars removed: {detail['chars removed']} c, "
                              f"Header removed: {detail['header removed']} c, "
                              f"Signatures removed: {detail['signatures removed']} c, "
                              f"Exhibits removed: {detail['exhibits removed']} c, "
                              f"Digits removed: {detail['digits removed']} c, "
                              f"Symbols removed: {detail['symbols removed']} c, "
                              f"Stopwords removed: {detail['stopwords removed']} c, "
                              f"is_10K: {detail['10K']}, "
                              f"Status: {detail['status']}\n")
                
        for detail in run_report['details']:
            if detail['10K'] == False:
                report_file.write(f"File: {detail['file']}, "
                              f"Initial length: {detail['initial length']} c, "
                              f"Final length: {detail['final length']} c, "
                              f"Chars removed: {detail['chars removed']} c, "
                              f"Header removed: {detail['header removed']} c, "
                              f"Signatures removed: {detail['signatures removed']} c, "
                              f"Exhibits removed: {detail['exhibits removed']} c, "
                              f"Digits removed: {detail['digits removed']} c, "
                              f"Symbols removed: {detail['symbols removed']} c, "
                              f"Stopwords removed: {detail['stopwords removed']} c, "
                              f"is_10K: {detail['10K']}, "
                              f"Status: {detail['status']}\n")

        report_file.write("\nWarnings\n")
        flag = True
        for warning in run_report['warnings']:
            percent_removed = warning['percentage removed']
            if percent_removed > 0.5 or percent_removed < 0.1:
                flag = False
                report_file.write(f"File: {detail['file']}, "
                              f"Initial length: {detail['initial length']} char, "
                              f"Final length: {detail['final length']} char, "
                              f"Percentage removed: {warning['percentage removed']}\n")
        if flag:
            report_file.write("No warnings\n")
    
    print(f"Run report saved to {report_file_path}")

    return run_report, report_file_path

def process_row(row):
    pattern = r'([^:]+):\s*([^,]+)'
    return {key.lstrip(" ,").lower().replace(" ", "_") : value.rstrip(" c").strip() for key, value in dict(re.findall(pattern, row)).items()}
    

def extract_dataframes_from_run_report(filepath):
    with open(filepath, 'r') as report:
        lines = report.readlines()
    
    success_data = []
    warning_data = []
    is_bottom_section = False
    count = 0
    for line in lines:
        if ((line.isspace() or not re.search(r'([^:]+):\s*([^,]+)', line)) and (len(success_data) == 0)):
            continue

        if not re.search(r'([^:]+):\s*([^,]+)', line) and len(success_data) > 0:
            is_bottom_section = True
            continue

        if is_bottom_section:
            row_data = process_row(line)
            if row_data:
                warning_data.append(row_data)
        
        else:
            row_data = process_row(line)
            if row_data:
                success_data.append(row_data)
    
    if success_data:
        columns = list(success_data[0].keys())
        df_success = pd.DataFrame(success_data, columns=columns)
    else:
        df_success = pd.DataFrame()
    
    if warning_data:
        columns = list(warning_data[0].keys())
        df_warning = pd.DataFrame(warning_data, columns=columns)
    else:
        df_warning = None

    return df_success, df_warning

def output_df(df: pd.DataFrame, output_dir, name: str):
    df = df.sort_values(by='file', ascending=False)
    report_file_path = os.path.join(output_dir, name)
    df.to_csv(report_file_path)
    return

def main():
    input_dir = '10-K_or_equivalent-txt'  # Input directory containing txt files
    output_dir = '10-K_or_equivalent-txt-preprocessed'# output directory containing processed txt files

    run_report, report_file_path = preprocess_directory(input_dir, output_dir)
    df_success, df_warning = extract_dataframes_from_run_report(report_file_path)

    output_df(df_success, output_dir, "success_report.csv")

    output_df(df_warning, output_dir, "warning_report.csv")

    # Further processing can be done with run_report if needed
    return

if __name__ == "__main__":
    main()