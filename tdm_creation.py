from word_list import get_vocabulary, get_vocabulary_df, import_word_list, create_vocabulary
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import os
import pandas as pd


def create_vectorizer(vocabulary):
    return CountVectorizer(vocabulary=vocabulary)

# Function to create a TDM
def create_tdm(documents, document_names, vectorizer: CountVectorizer):
    '''Create a term-document matrix restricted to the given vocabulary'''

    tdm = vectorizer.fit_transform(documents)
    tdm_df = pd.DataFrame(tdm.toarray(), columns=vectorizer.get_feature_names_out(), index=document_names)
    return tdm, tdm_df

# Process documents grouped by year
def process_yearly_documents(input_dir, output_dir, vectorizer):
    '''
    Process documents in the input directory grouped by year, creating yearly TDMs.
    
    Args: 
        input_dir(str): Input directory containing txt files organized by year
        output_dir (str): output directory for yearly TDMs.
        vocabulary (List): vocabulary for TDM creation.
        
    Returns:
        dict: a report containing information about processed files'''
    
    run_report = {
        'total_years': 0,
        'details' : []
    }

    failed_files = []
    os.makedirs(output_dir, exist_ok=True)

    tdms = {}

    with ThreadPoolExecutor() as executor:
        futures = []
        for year in os.listdir(input_dir):
            year_dir = os.path.join(input_dir, year)
            if os.path.isdir(year_dir): # Ensure it's a directory (year folder)
                yearly_documents = []
                yearly_document_names = []

                # Process each file within the year's directory
                for file in os.listdir(year_dir):
                    if file.lower().endswith('.txt') and 'run_report' not in file:
                        file_path = os.path.join(year_dir, file)
                        yearly_documents.append(file_path)
                        yearly_document_names.append(file)
                
                if len(yearly_documents) > 0:
                    futures.append(executor.submit(process_year, year, yearly_documents, yearly_document_names, vectorizer, output_dir))
                else:
                    print(f"No documents found for year {year}")

        for future in tqdm(as_completed(futures), total=len(futures), desc='Processing yearly documents'):
            try:
                year, tdm, tdm_df, year_output_path, num_documents = future.result()
                
                tdms[year] = tdm_df

                # Save the TDM for the year
                os.makedirs(os.path.dirname(year_output_path), exist_ok=True)
                tdm_df.to_csv(year_output_path)

                # Update the run report
                run_report['total_years'] += 1
                run_report['details'].append({
                    'year': year,
                    'num_documents': num_documents,
                    'output_file': year_output_path,
                    'status': 'Success'
                })

            except Exception as e:
                failed_files.append((year, e))
    
    # Create the run report file
    run_report['details'].sort(key=lambda x: x['year'])

    report_file_path = os.path.join(output_dir, "run_report.txt")
    with open(report_file_path, 'w') as report_file:
        report_file.write("Run Report\n")
        report_file.write(f"Total Years: {run_report['total_years']}\n")

        report_file.write("\nDetailed Report:\n")
        for detail in run_report['details']:
            report_file.write(f"Year: {detail['year']}, " 
                                f"Num Documents: {detail.get('num_documents', 0)}, "
                                f"Status: {detail['status']}, "
                                f"Output: {detail.get('output_file', 'N/A')}\n")

    print(f"Run report saved to {report_file_path}")
    return run_report, tdms
    
def process_year(year, documents, document_names, vectorizer, output_dir):
    '''
    Process documents for a specific year and generate a TDM
    
    Args:
        year (str): Year of the documents
        documents (list): list of ifle apths for the documents of the year
        document_names (list): List of document names corresponding to the files
        vocabulary (list): vocabulary for TDM creation
        output_dir (str): Directory to save the TDM
    
    Returns:
        tuple: (year, tdm_df, year_output_path) - the year, the TDM Dataframe, and the output path
        '''

    try:
        print(f"Processing {len(documents)} documents for year {year}")
        documents_content = [open(file, 'r', encoding='utf-8').read() for file in documents]

        # Create TDM for the year
        tdm, tdm_df = create_tdm(documents_content, document_names, vectorizer)
        
        # Define the output path for the TDM
        year_output_path = os.path.join(output_dir, f'tdm_{year}.csv')

        return year, tdm, tdm_df, year_output_path, len(documents)
    
    except Exception as e:
        print(f'Error processing year {year}: {e}')
        raise


def main():
    input_dir = 'BY-YEAR-10-K_or_equivalent-txt-preprocessed-lemmatized/'
    output_dir = 'TDMs'

    vocabulary = get_vocabulary()
    global vocab_df
    vocab_df = get_vocabulary_df()

    vectorizer = create_vectorizer(vocabulary)

    run_report, tdms = process_yearly_documents(input_dir, output_dir, vectorizer)

    # scoring_yearly_tdms(tdms)
    # terms = map_terms_to_hierarchy(tdms['2000'][1], get_vocabulary_df())
    # print(len(terms))
    print(len(tdms))

    return

if __name__ == "__main__":
    main()