import re
import json
import os

# List of canonical cities (unchanged)
canonical_cities = [
    "Iisalmi", "Joensuu", "Joroinen", "Juankoski", "Karttula", "Keitele", 
    "Kiuruvesi", "Lapinlahti", "Leppävirta", "Maaninka", "Nilsiä", "Pieksämäki", 
    "Pielavesi", "Rautalampi", "Siilinjärvi", "Suonenjoki", "Tahkovuori", 
    "Varpaisjärvi", "Vuorela", "Toivala"
]


def filter_prefix_keywords(tags_text):
    """
    Filters the raw text to return the full word matching the prefix criteria.
    """
    prefixes = ["kauko", "huol", "jake", "saneer","lämmö", "kesk", "vahin", "kaiv", "sähk", "vaurio", "korj", "per"]
    
    # CORRECTED PATTERN:
    # 1. (?:...) is a non-capturing group for the prefixes.
    # 2. (...) is the outer capturing group for the FULL word.
    prefix_pattern = r'\b((?:' + '|'.join(prefixes) + r')\w*)\b'
    
    return re.findall(prefix_pattern, tags_text, re.IGNORECASE)

def process_data(entry, canonical_cities, last_valid_year, last_valid_month):
    """Process each entry and return processed data along with the updated last_valid_year."""
    WHITESPACE = r"[\s\xa0]"
    
    FINAL_PATTERN = (
        rf"(?i).*?"
        rf"(?:tänään{WHITESPACE}+)?"
        rf"(maanantaina|tiistaina|keskiviikkona|torstaina|perjantaina|lauantaina|sunnuntaina){WHITESPACE}*"  # Weekday
        rf"(\d{{1,2}})\.(\d{{1,2}})(?:\.(\d{{4}}))?"                                                         # Date parts (day, month, year)
        rf"{WHITESPACE}*\.?"                                                                                 # Optional trailing dot
        rf"(?:" # Time block (optional)
        rf"{WHITESPACE}*"
        rf"(?:kello|klo){WHITESPACE}*"  # Time prefix (optional)
        rf"(\d{{1,2}}(?:[:\.]\d{{2}})?)?"  # Start Time (Minutes optional)
        rf"[–-]*"  # Separator
        rf"(\d{{1,2}}(?:[:\.]\d{{2}})?)?"  # End Time (Minutes optional)
        rf")?"  # End non-capturing optional block
        rf"{WHITESPACE}*(.*)"  # Tags/Message
    )

    match = re.search(FINAL_PATTERN, entry)
    
    if match:
        weekday     = match.group(1)
        day         = match.group(2)
        month       = match.group(3)
        year        = match.group(4) if match.group(4) else "Unknown"
        time_start  = match.group(5) 
        time_end    = match.group(6) if match.group(6) else "Unknown"
        message     = match.group(7).strip()
        tags        = filter_prefix_keywords(message)

        # If year is explicitly captured, use it; otherwise, use last_valid_year
        if year != "Unknown":  # If year is captured, update last_valid_year
            last_valid_year = year
            final_year_output = year
            last_valid_month = month
        else:
            # If year is missing, apply the last known year or default to 2025
            if last_valid_year and int(month) > int(last_valid_month):
                print(f"m: {month} lvm: {last_valid_month}")
                final_year_output = f"{int(last_valid_year)-1} (Puuttuva vuosi generoitu ympärillä olevasta datasta)"
                
            elif last_valid_year:
                final_year_output = f"{last_valid_year} (Puuttuva vuosi generoitu ympärillä olevasta datasta)"
            else:
                final_year_output = "2025"  # Default to 2025 if no year is found

        # --- Location Extraction ---
        location = None 
        entry_lower = entry.lower()
        entry_words = re.findall(r'\b[a-zåäö]{3,}\b', entry_lower)
        sorted_cities = sorted(canonical_cities, key=len, reverse=True)
        FIXED_MATCH_LEN = 5
        
        for city in sorted_cities:
            city_lower = city.lower()
            match_len = min(FIXED_MATCH_LEN, len(city_lower))
            for word in entry_words:
                try:
                    if word[:match_len] == city_lower[:match_len]:
                        location = city
                        break
                except IndexError:
                    continue
            if location:
                break
    

        return {
            'weekday': weekday,
            'day': day,
            'month': month,
            'year': final_year_output,  # Use the final year output here
            'time_start': time_start,
            'time_end': time_end,
            'tags': tags,
            'location': location,
        }, last_valid_year, last_valid_month
    else:
        return None, last_valid_year, last_valid_month


# Main function to process the raw JSON data
def raw_processor(raw_data, canonical_cities, last_valid_year=None, last_valid_month=12):
    """Process the raw JSON data (weekday, date, time)."""
    interim_data = []
    
    for entry in raw_data:
        processed_entry, last_valid_year, last_valid_month = process_data(entry, canonical_cities, last_valid_year, last_valid_month)
        if processed_entry:
            interim_data.append(processed_entry)

    return interim_data

# Function to save the interim processed data to a JSON file
def save_to_interim_json(data, filename):
    """Save the interim data to a JSON file."""
    # Ensure the directory exists
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    
    # Save the data to a JSON file
    with open(filename, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)
    
    print(f"Interim data saved to {filename}")
