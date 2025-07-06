import cairo
import pandas as pd
import numpy as np
from PIL import Image
from skimage.metrics import structural_similarity as ssim

def compute_pixel_difference_percentage(image_path_1, image_path_2, tolerance=0):
    """
    Compute the percentage of different pixels between two images.

    Parameters
    ----------
    image_path_1 : str
        Path to the first image.
    image_path_2 : str
        Path to the second image.
    tolerance : int, optional
        Tolerance level for pixel comparison (default is 0, meaning exact match).
        Pixels with absolute difference <= tolerance are considered the same.

    Returns
    -------
    float
        Percentage of different pixels in the range [0, 100], where 0 means 
        the images are identical and 100 means all pixels are different.
    """
    # Read the two images
    img1 = Image.open(image_path_1).convert('L')  # convert to grayscale
    img2 = Image.open(image_path_2).convert('L')  # convert to grayscale
    
    # Convert images to NumPy arrays
    arr1 = np.array(img1, dtype=np.uint8)
    arr2 = np.array(img2, dtype=np.uint8)
    
    # Make sure the images are the same size
    if arr1.shape != arr2.shape:
        raise ValueError("Images must have the same dimensions for pixel difference calculation.")
    
    # Calculate absolute difference between pixels
    diff = np.abs(arr1.astype(np.int16) - arr2.astype(np.int16))
    
    # Count pixels that differ by more than the tolerance
    different_pixels = np.sum(diff > tolerance)
    
    # Calculate total number of pixels
    total_pixels = arr1.size
    
    # Calculate percentage of different pixels
    percentage_different = (different_pixels / total_pixels) * 100
    
    return percentage_different

def compute_ssim(image_path_1, image_path_2):
    """
    Compute the Structural Similarity Index (SSIM) between two images using
    scikit-image's standard implementation.

    Parameters
    ----------
    image_path_1 : str
        Path to the first image.
    image_path_2 : str
        Path to the second image.

    Returns
    -------
    float
        SSIM value in the range [-1, 1], where 1 means the images are identical.
    """
    # Read the two images
    img1 = Image.open(image_path_1).convert('L')  # convert to grayscale
    img2 = Image.open(image_path_2).convert('L')  # convert to grayscale
    
    # Convert images to NumPy arrays
    arr1 = np.array(img1, dtype=np.uint8)
    arr2 = np.array(img2, dtype=np.uint8)
    
    # Make sure the images are the same size
    if arr1.shape != arr2.shape:
        raise ValueError("Images must have the same dimensions for SSIM calculation.")
    
    # Use scikit-image's SSIM implementation
    # data_range specifies the data range of the input images (255 for 8-bit)
    ssim_value = ssim(arr1, arr2, data_range=255)
    
    return ssim_value

def plot(df, measure, name, width, height, queryFrom, queryTo):
    """
    Creates a simple line plot from a pandas DataFrame column using the cairo library.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The DataFrame containing the data to plot.
    measure : str
        The column name in 'df' whose values we want to plot on the y-axis.
    name : str
        The name to use for the output PNG file.
    width : int
        The width in pixels of the resulting image.
    height : int
        The height in pixels of the resulting image.
    queryFrom : int
        The starting unix timestamp from which to plot.
    queryTo : int
        The ending unix timestamp for the plot.
    
    Returns:
    --------
    None
        A PNG file named '<name>.png' is saved to the current directory.
    """
    
    # Create a new image surface (ARGB32 means we can have an alpha channel, plus RGB).
    # The final output will be width x height pixels in size.
    surface = cairo.ImageSurface(cairo.FORMAT_ARGB32, width, height)
    
    # Create a drawing context on the surface.
    ctx = cairo.Context(surface)
    
    # Set the antialiasing mode to NONE.
    # This can make pixel rendering clearer if we want a more "pixel-perfect" look.
    ctx.set_antialias(cairo.Antialias.NONE)
    
    # Fill the entire surface with white.
    ctx.set_source_rgb(1, 1, 1)  # White color
    ctx.paint()                  # Paint the entire surface
    
    # By default, the cairo coordinate system places (0,0) at the top-left corner.
    # We'll move the origin to the bottom-left corner instead:
    ctx.translate(0, height)  # Move origin up to the bottom-left of the image.
    ctx.scale(1, -1)          # Flip the y-axis so it increases going upwards.
    
    # Now, set our drawing color to black for the plot line.
    ctx.set_source_rgb(0, 0, 0)
    ctx.set_line_width(1)
    
    # Calculate how many "data units" each pixel on the x-axis will span.
    # This is determined by the difference in the timestamps and the total width.
    pixelInterval = (queryTo - queryFrom) // width
    
    # Move the drawing cursor to the first data point in the DataFrame.
    ctx.move_to(
        np.floor((df['timestamp'].iloc[0] - queryFrom) / pixelInterval),
        np.floor(height * ((df[measure].iloc[0] - df[measure].min()) / (df[measure].max() - df[measure].min())))
    )
    
    # Iterate through the remaining rows in the DataFrame and draw lines
    # to each subsequent point.
    for i in range(1, len(df)):
        ctx.line_to(
            np.floor((df['timestamp'].iloc[i] - queryFrom) / pixelInterval),
            np.floor(height * ((df[measure].iloc[i] - df[measure].min()) / (df[measure].max() - df[measure].min())))
        )
    
    # Actually draw the lines that were defined with move_to and line_to.
    ctx.stroke()
    
    # Finally, write the rendered image to a PNG file.
    surface.write_to_png(name + '.png')

# Example DataFrame
df = pd.DataFrame({
    'timestamp': np.arange(10) * 1000,   # Simulate timestamps spaced by 1000
    'mydata': np.random.rand(10) * 100   # Random values from 0 to 100
})


if __name__ == "__main__":

    plot(df, measure='mydata', name='image_a', width=400, height=200, queryFrom=0, queryTo=9000)
    plot(df, measure='mydata', name='image_b', width=400, height=200, queryFrom=0, queryTo=9000)
    
    # Example usage:
    image1 = "image_a.png"
    image2 = "image_b.png"
    ssim_score = compute_ssim(image1, image2)
    print(f"SSIM between {image1} and {image2}: {ssim_score:.4f}")
    diff_percentage = compute_pixel_difference_percentage(image1, image2, tolerance=10)
    print(f"Percentage of different pixels between {image1} and {image2}: {diff_percentage:.4f}%")