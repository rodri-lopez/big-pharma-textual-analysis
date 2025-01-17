import pandas as pd
import os

from matplotlib import category
from collections import Counter
from collections import defaultdict

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

def import_word_list(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()

        data = []
        topic = category = subcategory = None
        keywords = []
        keyword_hierarchy = defaultdict(list)  # To store keywords with their hierarchy

        for line in lines:
            stripped_line = line.strip()
            if not stripped_line:
                continue

            if stripped_line.startswith("Topic:"):
                # Add previous level keywords to data
                if keywords:
                    data.append({"Topic": topic, "Category": category, "Subcategory": subcategory, "Keywords": keywords})
                    for kw in keywords:
                        keyword_hierarchy[kw.lower()].append((topic, category, subcategory))
                    keywords = []

                # Update topic
                topic = stripped_line.split(":")[1].strip()
                category = subcategory = None
            
            elif stripped_line.startswith("Keywords:"):
                # Collect keywords
                keywords = [kw.strip().lower() for kw in stripped_line.split(":")[1].split(",")]
            
            elif stripped_line.startswith("Category:"):
                # Add previous level keywords to data
                if keywords:
                    data.append({"Topic": topic, "Category": category, "Subcategory": subcategory, "Keywords": keywords})
                    for kw in keywords:
                        keyword_hierarchy[kw.lower()].append((topic, category, subcategory))
                    keywords = []
                
                # Update category
                category = stripped_line.split(":")[1].strip()
                subcategory = None
            
            elif stripped_line.startswith("Subcategory:"):
                # Add previous level keywords to data
                if keywords:
                    data.append({"Topic": topic, "Category": category, "Subcategory": subcategory, "Keywords": keywords})
                    for kw in keywords:
                        keyword_hierarchy[kw.lower()].append((topic, category, subcategory))
                    keywords = []
                
                # Update subcategory
                subcategory = stripped_line.split(":")[1].strip()
        
        if keywords: 
            data.append({"Topic": topic, "Category": category, "Subcategory": subcategory, "Keywords": keywords})
            for kw in keywords:
                keyword_hierarchy[kw.lower()].append((topic, category, subcategory))
        
        # Find duplicates
        duplicates = {kw: locations for kw, locations in keyword_hierarchy.items() if len(locations) > 1}
        
        if duplicates:
            print("Duplicate Keywords Found:")
            for kw, locations in duplicates.items():
                print(f"\nKeyword: {kw}")
                for loc in locations:
                    print(f"  Topic: {loc[0]}, Category: {loc[1]}, Subcategory: {loc[2]}")
        
        for row in data:
            row['Keywords'] = [kw.lower() for kw in row['Keywords']]
            
        return pd.DataFrame(data)

def create_vocabulary(df: pd.DataFrame):
    all_keywords = df['Keywords'].explode().tolist()
    unique_keywords = sorted(set(kw for kw in all_keywords if kw.strip()))
    return unique_keywords

def get_vocabulary():
    vocab_df = import_word_list('CSR-Word-Taxonomy.txt')
    vocabulary = create_vocabulary(vocab_df)

    vocabulary = [word.lower() for word in vocabulary]
    return vocabulary

def get_vocabulary_df():
    return import_word_list('CSR-Word-Taxonomy.txt')

def main():
    file = 'CSR-Word-Taxonomy.txt'
    df = import_word_list(file)
    # print(df)
    vocabulary = create_vocabulary(df)
    # print(vocabulary)
    return

if __name__ == "__main__":
    main()
