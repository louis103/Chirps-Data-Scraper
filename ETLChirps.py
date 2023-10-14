import os
import requests
from urllib.parse import urljoin
import gzip
import geopandas as gpd
import rasterio
from rasterio import mask

# Define the download folder for the compressed files
download_folder = r"CHIRPS_10_DAY"

# URL of the web directory containing the compressed files
base_url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_dekad/tifs/"

# Create a directory to save the downloaded and extracted files
if not os.path.exists(download_folder):
    os.makedirs(download_folder)

# Create a separate folder to store the extracted GeoTIFF files
geotiff_folder = os.path.join(download_folder, "geotiff_files_extended")
if not os.path.exists(geotiff_folder):
    os.makedirs(geotiff_folder)
    
# Load the name of the last successfully processed .tif.gz file (if any)
last_processed_file = "chirps-v2.0.2003.10.1.tif.gz"  # Update this with the actual last processed file

# Send an HTTP GET request to the base URL
response = requests.get(base_url)

# Check if the request was successful
if response.status_code == 200:
    # Parse the HTML content of the page
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(response.content, "html.parser")

    # Iterate through the links
    for link in soup.find_all('a'):
        href = link.get('href')
        if href and href.endswith(".tif.gz"):
            # Build the full URL of the compressed file
            file_url = urljoin(base_url, href)

            # Get the file name from the URL
            file_name = os.path.basename(file_url)
            
            # Check if this file has been processed before
            if file_name <= last_processed_file:
                print(f"Skipping {file_name} (already processed)")
                continue

            # Specify the path to save the downloaded compressed file
            compressed_file_path = os.path.join(download_folder, file_name)

            # Download the compressed file
            with open(compressed_file_path, 'wb') as f:
                response = requests.get(file_url, stream=True)
                if response.status_code == 200:
                    for chunk in response.iter_content(chunk_size=1024):
                        f.write(chunk)
                else:
                    print(f"Failed to download {file_name}")
                    continue

            print(f"Downloaded: {file_name}")

            # Extract the compressed file
            try:
                # Define the path to save the extracted GeoTIFF
                geotiff_file_path = os.path.join(geotiff_folder, file_name.replace(".gz", ""))

                # Open the compressed file using gzip and save the decompressed content as the GeoTIFF
                with gzip.open(compressed_file_path, 'rb') as gz_file:
                    with open(geotiff_file_path, 'wb') as tif_file:
                        tif_file.write(gz_file.read())

                print(f"Extracted and saved GeoTIFF: {geotiff_file_path}")

                # Load the shapefile of Kenya
                kenya_shapefile = r"C:\CHIRPS_10DAY_KENYA\Kenya_Shapefile\kenya.shp"
                kenya_gdf = gpd.read_file(kenya_shapefile)

                # Create an output folder for the clipped rasters
                output_folder = r"C:\CHIRPS_10DAY_KENYA\CHIRPS_10_DAY\ten_day_cropped_kenya"
                if not os.path.exists(output_folder):
                    os.makedirs(output_folder)

                # Open the GeoTIFF file
                with rasterio.open(geotiff_file_path) as src:
                    # Clip the raster to the extent of Kenya
                    clipped_data, clipped_transform = mask.mask(src, kenya_gdf.geometry, crop=True)

                    # Create a new metadata dictionary for the clipped raster
                    clipped_meta = src.meta.copy()
                    clipped_meta.update({
                        "height": clipped_data.shape[1],
                        "width": clipped_data.shape[2],
                        "transform": clipped_transform
                    })

                    # Create an output file name based on the input file
                    output_path = os.path.join(output_folder, file_name.replace(".gz", ""))

                    # Save the clipped raster to the output folder
                    with rasterio.open(output_path, "w", **clipped_meta) as dst:
                        dst.write(clipped_data)

                print(f"Clipped and saved: {output_path}")

                # Remove the downloaded compressed file
                os.remove(compressed_file_path)
                
                # Remove the extracted GeoTIFF file
                os.remove(geotiff_file_path)

            except Exception as e:
                print(f"Failed to process {file_name}: {str(e)}")

    print("All GeoTIFF files extracted, masked, and saved.")
else:
    print("Failed to retrieve the web page.")
