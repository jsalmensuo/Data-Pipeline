import json
import re
from collections import Counter
import os

def get_field_word_frequency(file_path, target_fields):
    """
    Analyzes specific fields in a JSON file for raw word frequencies.
    
    Args:
        file_path (str): Path to the JSON file ('interim/outage_data.json').
        target_fields (list): List of fields to analyze.
        
    Returns:
        Counter: A Counter object containing word frequencies, or an error string.
    """
    try:
        # 1. Load the JSON data
        with open(file_path, 'r', encoding='utf-8') as f:
            data_list = json.load(f)
    except FileNotFoundError:
        return "Error: File not found at the specified path."
    except json.JSONDecodeError:
        return "Error: Could not decode JSON. Check file integrity."
    except Exception as e:
        return f"An unexpected error occurred: {e}"

    all_text = ""
    
    # 2. Iterate through records and specific fields
    for entry in data_list:
        for field in target_fields:
            if field in entry and entry[field] is not None:
                # Convert content to string and append to total text
                all_text += str(entry[field]) + " "

    # 3. Clean and Tokenize the text
    text_lower = all_text.lower()
    
    # Finds all sequences of letters, numbers, and Finnish special characters (åäö)
    words = re.findall(r'[a-zåäö0-9]+', text_lower)
    
    # 4. Count Frequencies
    word_counts = Counter(words)
    
    return word_counts

# --- Execution Block to Print Results to Terminal ---

file_path = 'data/interim/outage_data.json'
target_fields = [
    "weekday", 
    "day", 
    "month", 
    "year", 
    "time_start", 
    "time_end", 
    "tags", 
    "location"
]

# Calculate the frequencies
frequency_results = get_field_word_frequency(file_path, target_fields)

# Check the result type and print accordingly
if isinstance(frequency_results, Counter):
    print("\n📊 Word Frequencies (Sorted by Count):")
    
    # Iterate through the Counter, sorted by most common first
    for word, count in frequency_results.most_common():
        print(f"{word}: [{count}]")
    
    print("\nAnalysis complete.")
    
else:
    # Print the error message returned by the function
    print(f"\n🛑 Error during analysis: {frequency_results}")
