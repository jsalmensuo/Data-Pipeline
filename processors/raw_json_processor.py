from processors.json_day_processor import find_weekday
# Main function to process the raw JSON data
def raw_processor1(raw_data):
    """Process the raw JSON data."""
    interim_data = []
    
    for entry in raw_data:
        processed_entry = find_weekday(entry)
        if processed_entry:
            interim_data.append(processed_entry)

    return interim_data