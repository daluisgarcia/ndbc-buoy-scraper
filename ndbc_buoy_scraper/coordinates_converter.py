import re


def convert_coordinates(text):
    """
    Extracts latitude and longitude from a string in the format 
    '48.126 N 163.355 E (48°7'34" N 163°21'18" E)' and converts them to decimal degrees.
    """
    
    # 1. Defining the pattern: look for a number followed by an optional space and then a direction (N, S, E, W).
    # Explanation of the Regex: (\d+\.\d+) captures the number, \s* ignores spaces, ([NSEW]) captures the direction.
    pattern = r"(\d+\.\d+)\s*([NSEW])"
    
    # 2. Searching for all matches in the text
    matches = re.findall(pattern, text)
    
    if len(matches) < 2:
        raise ValueError("No valid coordinates found. Text provided: " + text)

    # Process the Latitude (first match)
    lat_val = float(matches[0][0])
    lat_dir = matches[0][1]
    # If it's South (S), the value is negative
    latitude = -lat_val if lat_dir == 'S' else lat_val

    # Process the Longitude (second match)
    lon_val = float(matches[1][0])
    lon_dir = matches[1][1]
    # If it's West (W), the value is negative
    longitude = -lon_val if lon_dir == 'W' else lon_val

    return latitude, longitude


def is_valid_coordinates(text: str | None) -> bool:
    """
    Validates if the input text contains valid coordinates in the expected format.
    """
    if text is None:
        return False

    pattern = r"(\d+\.\d+)\s*([NSEW])"
    matches = re.findall(pattern, text)
    
    return len(matches) >= 2


if __name__ == "__main__":
    input = "37.227 N 76.479 W (37°13'36\" N 76°28'43\" W)"
    lat, lon = convert_coordinates(input)

    print(f"Original text: {input}")
    print(f"Decimal latitude: {lat}")
    print(f"Decimal longitude: {lon}")
