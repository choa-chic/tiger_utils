"""
url_patterns.py - URL construction and dataset/type constants for TIGER/Line downloads
"""

# State FIPS codes
STATES = {
    '01': 'Alabama', '02': 'Alaska', '04': 'Arizona', '05': 'Arkansas',
    '06': 'California', '08': 'Colorado', '09': 'Connecticut', '10': 'Delaware',
    '11': 'District of Columbia', '12': 'Florida', '13': 'Georgia', '15': 'Hawaii',
    '16': 'Idaho', '17': 'Illinois', '18': 'Indiana', '19': 'Iowa',
    '20': 'Kansas', '21': 'Kentucky', '22': 'Louisiana', '23': 'Maine',
    '24': 'Maryland', '25': 'Massachusetts', '26': 'Michigan', '27': 'Minnesota',
    '28': 'Mississippi', '29': 'Missouri', '30': 'Montana', '31': 'Nebraska',
    '32': 'Nevada', '33': 'New Hampshire', '34': 'New Jersey', '35': 'New Mexico',
    '36': 'New York', '37': 'North Carolina', '38': 'North Dakota', '39': 'Ohio',
    '40': 'Oklahoma', '41': 'Oregon', '42': 'Pennsylvania', '44': 'Rhode Island',
    '45': 'South Carolina', '46': 'South Dakota', '47': 'Tennessee', '48': 'Texas',
    '49': 'Utah', '50': 'Vermont', '51': 'Virginia', '53': 'Washington',
    '54': 'West Virginia', '55': 'Wisconsin', '56': 'Wyoming',
    '60': 'American Samoa', '66': 'Guam', '69': 'Commonwealth of the Northern Mariana Islands',
    '72': 'Puerto Rico', '78': 'United States Virgin Islands'
}

# FIPS codes for the 50 US states only
FIFTY_STATE_FIPS = {
    '01', '02', '04', '05', '06', '08', '09', '10', '11', '12', '13', '15', '16', '17', '18', '19',
    '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35',
    '36', '37', '38', '39', '40', '41', '42', '44', '45', '46', '47', '48', '49', '50', '51', '53',
    '54', '55', '56'
}
TERRITORY_FIPS = set(STATES.keys()) - FIFTY_STATE_FIPS

DATASET_TYPES = {
    'EDGES': 'All Lines (roads, railroads, etc.)',
    'ADDR': 'Address Ranges',
    'FACES': 'Topological Faces (polygons)',
    'FEATNAMES': 'Feature Names',
    'PLACE': 'Places (cities, towns)',
    'COUSUB': 'County Subdivisions',
    'TRACT': 'Census Tracts',
    'BG': 'Block Groups',
    'TABBLOCK20': 'Tabulation Blocks (2020)',
    'ZCTA520': 'ZIP Code Tabulation Areas (2020)',
    'COUNTY': 'Counties',
    'STATE': 'States',
    'CD118': 'Congressional Districts (118th)',
    'SLDL': 'State Legislative Districts (Lower)',
    'SLDU': 'State Legislative Districts (Upper)',
    'UNSD': 'Unified School Districts',
    'ELSD': 'Elementary School Districts',
    'SCSD': 'Secondary School Districts',
}

COUNTY_LEVEL_TYPES = ['EDGES', 'ADDR', 'FEATNAMES']

def get_county_list(state_fips: str, year: int = 2025) -> list:
    """
    Get list of county FIPS codes for a given state.
    For 2025, we'll use a comprehensive list approach.
    """
    # County FIPS codes should always be odd (001, 003, ..., 199)
    return [f"{i:03d}" for i in range(1, 200, 2)]

def construct_url(year: int, state_fips: str, county_fips: str, dataset_type: str) -> str:
    """
    Construct the download URL for a TIGER/Line file.
    URL pattern: https://www2.census.gov/geo/tiger/TIGER{year}/{TYPE}/
                 tl_{year}_{statefips}{countyfips}_{type}.zip
    """
    base_url = f"https://www2.census.gov/geo/tiger/TIGER{year}"
    dir_part = dataset_type.upper()
    file_part = dataset_type.lower()
    if dataset_type in ['EDGES', 'ADDR', 'FACES', 'FEATNAMES']:
        url = f"{base_url}/{dir_part}/tl_{year}_{state_fips}{county_fips}_{file_part}.zip"
    elif dataset_type in ['PLACE', 'COUSUB', 'TRACT', 'BG']:
        url = f"{base_url}/{dir_part}/tl_{year}_{state_fips}_{file_part}.zip"
    elif dataset_type == 'COUNTY':
        url = f"{base_url}/COUNTY/tl_{year}_{state_fips}_county.zip"
    elif dataset_type == 'STATE':
        url = f"{base_url}/STATE/tl_{year}_us_state.zip"
    else:
        url = f"{base_url}/{dir_part}/tl_{year}_{state_fips}_{file_part}.zip"
    return url
