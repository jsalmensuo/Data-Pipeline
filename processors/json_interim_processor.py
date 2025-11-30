def filter_data(interim_data, required_fields=None):
    """
    Filters a list of processed outage entries, removing those that are 
    missing data in critical fields.

    Args:
        interim_data (list): List of dictionaries (your processed data).
        required_fields (list): List of keys that must have a valid value.
                                Defaults to ['location', 'keywords_only', 'time_end'].
    
    Returns:
        list: Filtered list of dictionaries.
    """
    if required_fields is None:
        
        required_fields = ['location', 'tags', 'time_start', 'time_end']
        
    cleaned_data = []
    
    for entry in interim_data:
        is_complete = True
        
        for field in required_fields:
            value = entry.get(field)
            
            # Check for:
            # 1. Null value (None)
            # 2. String 'Unknown' or 'Puuttuva vuosi...' (from year imputation)
            # 3. Empty list (for 'keywords_only' or 'tags')
            if value is None or \
               value == 'Unknown' or \
               (isinstance(value, list) and not value):
                
                is_complete = False
                print(f"Puutteellinen data poistettu: {field}")
                break
        
        if is_complete:
            cleaned_data.append(entry)
            
    return cleaned_data