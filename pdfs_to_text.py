import os
import pdfplumber
from tqdm import tqdm
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed

def convert_pdf(pdf_file, output_subdir):
    """Convert a single PDF file to TXT."""
    try:
        os.makedirs(output_subdir, exist_ok=True)  # Ensure the output directory exists
        txt_file = os.path.join(output_subdir, os.path.basename(pdf_file).replace('.pdf', '.txt'))

        # Perform the conversion
        with pdfplumber.open(pdf_file) as pdf:
            with open(txt_file, 'w', encoding='utf-8') as f:
                for page in pdf.pages:
                    text = page.extract_text()
                    if text:  # Check if text was extracted
                        f.write(text)
        
        # Return the sizes of the PDF and TXT files (in bytes)
        return os.path.getsize(pdf_file), os.path.getsize(txt_file) if os.path.exists(txt_file) else 0

    except Exception as e:
        print(f"Error converting {pdf_file}: {e}")
        return os.path.getsize(pdf_file), 0  # Return PDF size and 0 for TXT size if failed

def convert_directory(input_dir, output_dir):
    """Convert all PDF files in the input directory to TXT files in the output directory."""
    pdf_files = []
    
    # Collect all PDF files from the directory and subdirectories
    for root, _, files in os.walk(input_dir):
        for file in files:
            if file.lower().endswith('.pdf'):
                pdf_files.append(os.path.join(root, file))

    total_files = len(pdf_files)
    successful_conversions = 0
    failed_conversions = 0
    failed_files = []

    run_report = {
        'total_files': total_files,
        'successful_conversions': successful_conversions,
        'failed_conversions': failed_conversions,
        'failed_files': failed_files,
        'details': []
    }

    with ThreadPoolExecutor() as executor:
        futures = []
        for pdf_file in pdf_files:
            output_subdir = os.path.join(output_dir, os.path.relpath(os.path.dirname(pdf_file), input_dir))
            futures.append(executor.submit(convert_pdf, pdf_file, output_subdir))
        
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing PDFs"):
            try:
                pdf_size, txt_size = future.result()  # Get sizes from the future
                successful_conversions += 1
                run_report['details'].append({
                    'file': pdf_file,
                    'pdf_size': pdf_size,
                    'txt_size': txt_size,
                    'status': 'Success'
                })
            except Exception as e:
                failed_conversions += 1
                failed_files.append(pdf_file)
                run_report['details'].append({
                    'file': pdf_file,
                    'pdf_size': os.path.getsize(pdf_file),
                    'txt_size': 0,
                    'status': 'Failed'
                })

    # Create the run report file
    report_file_path = os.path.join(output_dir, "run_report.txt")
    with open(report_file_path, 'w') as report_file:
        report_file.write("Run Report\n")
        report_file.write(f"Total Files: {run_report['total_files']}\n")
        report_file.write(f"Successful Conversions: {run_report['successful_conversions']}\n")
        report_file.write(f"Failed Conversions: {run_report['failed_conversions']}\n")
        
        if run_report['failed_files']:
            report_file.write("Failed Files:\n")
            for failed_file in run_report['failed_files']:
                report_file.write(f" - {failed_file}\n")

        report_file.write("\nDetailed Report:\n")
        for detail in run_report['details']:
            report_file.write(f"File: {detail['file']}, PDF Size: {detail['pdf_size']} bytes, TXT Size: {detail['txt_size']} bytes, Status: {detail['status']}\n")
    
    print(f"Run report saved to {report_file_path}")

    return run_report

def main():
    input_dir = '10-K_or_equivalent'  # Input directory containing PDF files
    output_dir = '10-K_or_equivalent-txt'  # Output directory for TXT files
    run_report = convert_directory(input_dir, output_dir)
    # Further processing can be done with run_report if needed

if __name__ == "__main__":
    main()