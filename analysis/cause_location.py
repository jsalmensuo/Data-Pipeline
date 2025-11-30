import pandas as pd
import matplotlib.pyplot as plt
import os
import numpy as np
import re


def normalize_cause(tag):
    if not isinstance(tag, str):
        return None

    tag = tag.lower().strip()

    # --- Maintenance (huolto*) ---
    if re.search(r"\bhuol", tag):
        return "Huolto"

    # --- District heating (kaukolämpö*, lämmönjakelu*) ---
    if re.search(r"\bkaiv", tag):
        return "Kaivuutyöt"

    # --- Outage (keskeytys*) ---
    if re.search(r"\bsaneer", tag):
        return "Saneeraus"
    
    if re.search(r"\bkorj", tag):
        return "Korjaustyöt"
    
    if re.search(r"\bvaurio", tag):
        return "Vauriokorjaus"

    # --- If no pattern matches → DROP the tag ---
    return None


def analyze_cause_by_location(data):
    """
    Groups data by location and individual tags to show cause frequency per city.
    
    Args:
        data (list): List of dictionaries (your clean outage data).
        
    Returns:
        pd.DataFrame: A pivot table showing tag counts per location.
    """
    df = pd.DataFrame(data)
    
    # 1. Explode the 'tags' column and rename the tags column
    df_exploded = df.explode('tags').rename(columns={'tags': 'Cause'})
    
    # Normalize
    df_exploded['Cause'] = df_exploded['Cause'].apply(normalize_cause)

# Drop rows where normalization returned None
    df_exploded = df_exploded.dropna(subset=['Cause'])
    
    # 2. Group and Count
    # Use the correct, lowercase column name 'location'
    cause_counts = df_exploded.groupby(['location', 'Cause']).size().reset_index(name='Count')
    
    # 3. Pivot the Table
    # Pivot the data to get locations as rows and causes as columns.
    pivot_table = cause_counts.pivot_table(
        index='location', 
        columns='Cause', 
        values='Count', 
        fill_value=0
    )
    
    # 4. Add a 'Total Outages' column for context (needed by the plotting function)
    pivot_table['Total Outages'] = pivot_table.sum(axis=1)
    
    # Sort the locations by total number of outages
    pivot_table = pivot_table.sort_values(by='Total Outages', ascending=False)

    return pivot_table

def plot_cause_by_location(pivot_table_df):
    """
    Generates and saves a stacked horizontal bar chart showing the breakdown 
    of outage causes for each location.

    Args:
        pivot_table_df (pd.DataFrame): The matrix from analyze_causes_by_location.
    """
    
    # 1. Prepare Data
    # Exclude the 'Total Outages' column from the columns used for stacking
    plot_data = pivot_table_df.drop(columns=['Total Outages'], errors='ignore')

    # Ensure the location (index) is sorted by Total Outages descending
    plot_data = plot_data.sort_values(by=plot_data.index.name, ascending=False)
    
    # 2. Create Plot
    fig, ax = plt.subplots(figsize=(12, 10))
    
    # Get the list of locations (index of the DataFrame)
    locations = plot_data.index
    
    # Initialize a cumulative base for stacking
    cumulative_sum = np.zeros(len(locations))
    
    # 3. Plot Stacked Bars
    for column in plot_data.columns:
        # Plot the current cause on top of the cumulative sum
        ax.barh(
            locations,
            plot_data[column],
            left=cumulative_sum,
            label=column
        )
        # Update the cumulative sum for the next cause
        cumulative_sum += plot_data[column].values
    
    # 4. Customize and Label
    ax.set_title('Keskeytyksien jakauma paikkakunnittain', fontsize=18)
    ax.set_xlabel('Keskeytyksien lukumäärä vuosina 2021-2025', fontsize=14)
    ax.set_ylabel('Sijainti', fontsize=14)
    ax.tick_params(axis='y', labelsize=16)
    
    # Move the legend outside the plot area
    ax.legend(
    title="Häiriön syy",
    bbox_to_anchor=(1.05, 1),
    loc='upper left',
    fontsize=18,        # size of legend labels
    title_fontsize=24,  # size of legend title
    frameon=True        # optional: adds a box around the legend
)

    plt.tight_layout(rect=[0, 0, .9, 1]) # Adjust layout to make room for legend
    
    # 5. Save the figure
    output_dir = 'reports/charts'
    os.makedirs(output_dir, exist_ok=True)
    
    chart_path = os.path.join(output_dir, 'cause_by_location_stacked_bar.png')
    plt.savefig(chart_path)
    plt.close()
    
    print(f"\nStacked bar chart saved to: {chart_path}")
