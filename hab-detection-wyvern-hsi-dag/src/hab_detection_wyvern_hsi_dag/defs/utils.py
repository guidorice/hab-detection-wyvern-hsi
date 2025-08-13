import os
from pathlib import Path

import numpy as np
from dagster import ConfigurableResource
from pystac_client import Client


class STACResource(ConfigurableResource):
    """
    Dagster resource which wraps pystac_client.
    """
    catalog_url: str

    def get_client(self):
        return Client.open(self.catalog_url)


def file_size(path: Path) -> str:
    """
    Calculate file size in human readable format.
    """
    size_bytes = os.path.getsize(path)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def scale_to_8bit(arr: np.ndarray) -> np.ndarray:
    """
    Scale a numpy array to 8-bit range [1-255] using percentile-based contrast stretching.
    Uses 0 as nodata value.

    Parameters:
        arr (numpy.ndarray): Input array with potential NaN values

    Returns:
        numpy.ndarray: 8-bit unsigned integer array with NaN values replaced by 0 (nodata)
    """
    # Create a copy to avoid modifying the input
    arr_copy = arr.copy()
    # Create a mask for valid values
    valid_mask = ~np.isnan(arr_copy)
    # Initialize output array with nodata value (0)
    output = np.zeros_like(arr_copy, dtype=np.uint8)
    
    # Process only if we have valid data
    if np.any(valid_mask):
        # Calculate percentiles only on valid data
        min_val = np.nanpercentile(arr_copy, 2)  # 2nd percentile
        max_val = np.nanpercentile(arr_copy, 98)  # 98th percentile
        
        # Apply scaling only to valid data
        if max_val > min_val:  # Avoid division by zero
            # Scale to 1-255 range (0 is reserved for nodata)
            scaled_valid = (arr_copy[valid_mask] - min_val) / (max_val - min_val) * 254 + 1
            scaled_valid = np.clip(scaled_valid, 1, 255)
            output[valid_mask] = scaled_valid.astype(np.uint8)
    
    return output
