#!/usr/bin/env python3
import pandas as pd
import random
import sys
import argparse
from math import floor

def allocate_hospitals_proportional(regions, total_hospitals, seed=None):
    """
    Allocates hospitals to regions based on population proportions.

    Args:
        regions (dict): Dictionary with region names as keys and populations as values.
        total_hospitals (int): Total number of unique hospitals to allocate.
        seed (int, optional): Random seed for reproducibility.

    Returns:
        dict: Mapping of regions to the number of hospitals allocated.
    """
    if seed is not None:
        random.seed(seed)

    total_population = sum(regions.values())
    allocation = {}
    fractional_allocation = {}

    # Initial allocation based on floor of proportional hospitals
    for region, population in regions.items():
        proportion = population / total_population
        exact_allocation = proportion * total_hospitals
        allocated = floor(exact_allocation)
        allocation[region] = allocated
        fractional_allocation[region] = exact_allocation - allocated

    # Calculate remaining hospitals to allocate
    allocated_total = sum(allocation.values())
    remaining = total_hospitals - allocated_total

    if remaining > 0:
        # Sort regions by highest fractional allocation
        sorted_regions = sorted(fractional_allocation.items(), key=lambda x: x[1], reverse=True)
        for region, frac in sorted_regions:
            if remaining == 0:
                break
            allocation[region] += 1
            remaining -= 1

    return allocation

def assign_hospitals_to_regions(hospitals, allocation, seed=None):
    """
    Assigns hospitals to regions based on the allocation.

    Args:
        hospitals (list): List of unique hospital names.
        allocation (dict): Mapping of regions to the number of hospitals to assign.
        seed (int, optional): Random seed for reproducibility.

    Returns:
        dict: Mapping of hospital names to their assigned regions.
    """
    if seed is not None:
        random.seed(seed)

    random.shuffle(hospitals)
    mapping = {}
    current_index = 0
    for region, count in allocation.items():
        assigned_hospitals = hospitals[current_index:current_index + count]
        for hospital in assigned_hospitals:
            mapping[hospital] = region
        current_index += count
    return mapping

def standardize_hospital_names(df):
    """
    Cleans and standardizes hospital names to reduce the number of unique entries.

    Args:
        df (DataFrame): Pandas DataFrame containing the 'Hospital' column.

    Returns:
        DataFrame: DataFrame with a new 'Hospital_cleaned' column.
    """
    # Remove leading/trailing spaces and standardize case
    df['Hospital_cleaned'] = df['Hospital'].astype(str).str.strip().str.title()

    # Replace common terms for consistency
    replacements = {
        "Hôpital": "Hospital",
        "Clinique": "Clinic",
        "Health Center": "Health Centre",
        "University Hospital": "University Hospital",
        # Add more replacements as needed
    }

    for old, new in replacements.items():
        df['Hospital_cleaned'] = df['Hospital_cleaned'].str.replace(old, new, regex=False)

    # Manual corrections (example)
    manual_replacements = {
        "St Pierre Hospital": "St. Pierre Hospital",
        "Central Health Center": "Central Health Centre",
        # Add more manual corrections as needed
    }

    df['Hospital_cleaned'] = df['Hospital_cleaned'].replace(manual_replacements)

    return df

def main():
    # -------------------------------
    # Configuration: Define Morocco's 12 Regions and their Populations
    # (Population figures are approximate and should be updated with actual data if available)
    # -------------------------------
    regions_population = {
        "Tanger-Tetouan-Al Hoceima": 4000000,
        "Oriental": 4300000,
        "Fès-Meknès": 4500000,
        "Rabat-Salé-Kénitra": 4300000,
        "Béni Mellal-Khénifra": 3700000,
        "Casablanca-Settat": 7000000,
        "Marrakech-Safi": 5300000,
        "Drâa-Tafilalet": 2500000,
        "Souss-Massa": 3600000,
        "Guelmim-Oued Noun": 1000000,
        "Laâyoune-Sakia El Hamra": 600000,
        "Dakhla-Oued Ed-Dahab": 300000
    }

    # -------------------------------
    # Argument Parsing
    # -------------------------------
    parser = argparse.ArgumentParser(description='Map hospitals to Moroccan regions based on population.')
    parser.add_argument('input_csv', type=str, help='Path to the input CSV file.')
    parser.add_argument('-o', '--output', type=str, default='updated_dataset.csv', help='Path for the output CSV file.')
    parser.add_argument('-s', '--seed', type=int, default=42, help='Random seed for reproducibility.')
    args = parser.parse_args()

    input_file = args.input_csv
    output_file = args.output
    seed = args.seed

    # -------------------------------
    # Step 1: Read the Dataset
    # -------------------------------
    try:
        df = pd.read_csv(input_file)
        print(f"Dataset '{input_file}' loaded successfully.")
    except FileNotFoundError:
        print(f"Error: The file '{input_file}' was not found.")
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: The file '{input_file}' is empty.")
        sys.exit(1)
    except pd.errors.ParserError:
        print(f"Error: The file '{input_file}' could not be parsed.")
        sys.exit(1)

    # -------------------------------
    # Step 2: Verify Required Columns Exist
    # -------------------------------
    required_columns = ["Hospital"]
    for column in required_columns:
        if column not in df.columns:
            print(f"Error: Required column '{column}' is missing from the dataset.")
            sys.exit(1)

    # -------------------------------
    # Step 3: Clean and Standardize Hospital Names
    # -------------------------------
    df = standardize_hospital_names(df)
    print("Hospital names have been standardized.")

    # -------------------------------
    # Step 4: Extract Unique Hospitals After Cleaning
    # -------------------------------
    unique_hospitals = df['Hospital_cleaned'].dropna().unique().tolist()
    num_hospitals = len(unique_hospitals)
    print(f"Number of unique hospitals after cleaning: {num_hospitals}")

    # -------------------------------
    # Step 5: Allocate Hospitals to Regions Proportionally
    # -------------------------------
    try:
        allocation = allocate_hospitals_proportional(
            regions=regions_population,
            total_hospitals=num_hospitals,
            seed=seed
        )
        print("Hospital allocation per region based on population:")
        for region, count in allocation.items():
            print(f"  {region}: {count} hospitals")
    except ValueError as ve:
        print(f"Allocation Error: {ve}")
        sys.exit(1)

    # -------------------------------
    # Step 6: Assign Hospitals to Regions
    # -------------------------------
    mapping = assign_hospitals_to_regions(
        hospitals=unique_hospitals.copy(),
        allocation=allocation,
        seed=seed
    )
    print("Hospitals have been assigned to regions successfully.")

    # -------------------------------
    # Step 7: Map Hospitals to Regions in Dataset
    # -------------------------------
    df['Region'] = df['Hospital_cleaned'].map(mapping)
    print("Region column has been added to the dataset.")

    # -------------------------------
    # Step 8: Verify Mapping Completeness
    # -------------------------------
    missing_regions = df['Region'].isnull().sum()
    if missing_regions > 0:
        print(f"Warning: {missing_regions} entries have hospitals that were not mapped to any region.")
        # Assign 'Unknown' to these entries
        df['Region'].fillna('Unknown', inplace=True)
        print("Assigned 'Unknown' to unmapped hospitals.")

    # -------------------------------
    # Step 9: Save the Updated Dataset
    # -------------------------------
    try:
        df.to_csv(output_file, index=False)
        print(f"Updated dataset saved to '{output_file}'.")
    except Exception as e:
        print(f"Error: Could not save the updated dataset. {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
