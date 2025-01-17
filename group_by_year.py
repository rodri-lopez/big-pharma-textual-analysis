import os
import re
import shutil
import pandas as pd

def extract_year_from_filename(filename):
    match = re.search(r'(\d{4})', filename)
    return match.group(0) if match else None

def group_files_by_year(root_dir):
    year_groups = {}
    for subdir, _, files in os.walk(root_dir):
        for file in files:
            year = extract_year_from_filename(file)
            if year:
                if year not in year_groups:
                    year_groups[year] = []
                year_groups[year].append(os.path.join(subdir, file))
    return year_groups

def reorganize_files_by_year(source_root_dir):

    # Get the absolute source path of the source directory
    absolute_source_path = os.path.abspath(source_root_dir)

    # Create the new root directory with the 'By-Year-' prefix if it doesn't exist
    target_root_dir = f'BY-YEAR-{os.path.basename(absolute_source_path)}'
    target_root_dir_path = os.path.join(os.path.dirname(absolute_source_path), target_root_dir)
    os.makedirs(target_root_dir_path, exist_ok=True)

    # Get the year groups
    year_groups = group_files_by_year(source_root_dir)

    # Loop through the files and create subdirectories based on years in the new directory
    for year, files in year_groups.items():
        year_dir = os.path.join(target_root_dir_path, year)
        os.makedirs(year_dir, exist_ok=True)

        # Move each file into its respective year directory
        for file in files:
            try:
                destination = os.path.join(year_dir, os.path.basename(file))
                shutil.copy(file, destination)
            except Exception as e:
                print(f'Error copying file {file}: {e}')

def main():
    root_directory = '10-K TEST-txt-preprocessed-lemmatized/'
    reorganize_files_by_year(root_directory)

if __name__ == "__main__":
    main()