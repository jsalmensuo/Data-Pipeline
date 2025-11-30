import re

rejected_entries = []
last_valid_year = 2025

def find_weekday(data: str):
    """
    Final, robust unified solution with safe indexing and flexible time capture.
    """
    global last_valid_year
    
    # Updated Weekday list to handle the common truncation errors seen in your data
    WEEKDAYS = r"(Maanantaina|Tiistaina|Keskiviikkona|Torstaina|Perjantaina|Lauantaina|Sunnuntaina)"
    
    FULL_PATTERN = r""".*?(Maanantaina|Tiistaina|Keskiviikkona|Torstaina|Perjantaina|Lauantaina|Sunnuntaina)\s*(?:(\d{1,2}\.\d{1,2}(?:\.\d{4})?)\s*(?:\s*(?:kello|klo|klo\.)\s*(\d{1,2}(?:[.:]\d{2})?)(?:\s*[-\u2014–]+\s*(\d{1,2}(?:[.:]\d{2})?))?)?)?\s*(.*)"""

    regex_match = re.search(FULL_PATTERN, data, re.IGNORECASE | re.X)
    
    if regex_match:
        # Use the correct logical indices (G1, G2, G3, G4, G5)
        weekday    = regex_match.group(1)
        date       = regex_match.group(2)
        
        # Using the correct logical indices (G3 and G4)
        time_start = regex_match.group(3) if regex_match.group(3) else "Unknown"
        time_end   = regex_match.group(4) if regex_match.group(4) else "Unknown"
        
        # G5 is the message
        tags       = regex_match.group(5)
        tags       = tags.lstrip(". ") if tags else "Unknown"

        # --- Date Processing Logic ---
        if date and len(date.split(".")) == 3:
            last_valid_year = date.split(".")[2]

        if date and len(date.split(".")) == 2 and last_valid_year:
            date = f"{date}.{last_valid_year} (Puuttuva vuosi generoitu ympärillä olevasta datasta)"
        
        # --- Final Result ---
        result = {
            "weekday": weekday,
            "date": date if date else "Unknown",
            "time_start": time_start,
            "time_end": time_end,
            "message": tags
        }
        
        return result
    
    else:
        # If no match, we hit a truncated entry (like "Jär" or "Tii")
        rejected_entries.append(data)
        return None
    
def extract_all():
        pass