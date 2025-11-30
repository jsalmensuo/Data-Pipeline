import os
import json

print("Reached file_utils.py")
def save_to_json(data, filename):
    """
    Save the provided data to a JSON file.

    :param data: The data to save to the file.
    :param filename: The name of the file to save the data in.
    """
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Save the data to a JSON file
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    
    print(f"Data saved to {filename}")
