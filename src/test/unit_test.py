import pytest
from dotenv import load_dotenv
import os 
import shutil
storage_name = os.getenv("STORAGE_NAME")
from pyspark.sql import SparkSession
from main.main import Task1

load_dotenv()
storage_name = os.getenv("STORAGE_NAME")

task = Task1(hotels_df_path='test/raw_hotels_df.csv', weather_df_path = 'test/raw_weather.parquet') 

# Create spark session to load test dataframes
spark = SparkSession.builder \
    .appName("Testing") \
    .getOrCreate()

hotels_with_geohash = spark.read.csv('test/hotels_with_geohash.csv', header=True)
weather_with_geohash = spark.read.parquet('test/weather_with_geohash.parquet')

# To test functionality to load hotels dataframe
def test_for_load_hotels_df(data_path = 'test/raw_hotels_df.csv'):
    df = task.load_hotels_df(path = data_path)
    actual_count = df.count()
    expected_count = 500
    assert actual_count == expected_count, f"Expected {expected_count} rows with null values, but found {actual_count}"

# To test functionality to load weather dataframe
def test_for_load_weather_df(data_path='test/raw_weather.parquet'):
    df = task.load_weather_df(path = data_path)
    actual_count = df.count()
    expected_count = 10000
    assert actual_count == expected_count, f"Expected {expected_count} rows with null values, but found {actual_count}"

def test_assign_coordinates(data_path='test/raw_hotels_df.csv'):
    # Test dataset of hotels with null values.
    hotels_df_with_null_values = spark.read.csv(data_path, header=True)
   
    # Fill NULL values using method assign_coordinates()
    hotels_df_without_null_values = task.assign_coordinates(hotels_df_with_null_values)
    actual_null_count = hotels_df_without_null_values.where((hotels_df_without_null_values.rawDfLatitude.isNull()) | (hotels_df_without_null_values.rawDfLongitude.isNull())).count()
    expected_null_count = 0
    assert actual_null_count == expected_null_count, f"Expected {expected_null_count} rows with null values, but found {actual_null_count}"

#To test the functionality of the merge_ordinates method, it should join the raw hotels dataframe with the subset of the hotels dataframe containing filled null values. This will substitute the null values (latitude and longitude) in the raw hotels dataframe with the corresponding values from the subset.
def test_merge_ordinates(filled_coordinate_data = 'test/hotels_with_filled_coordinates.csv', raw_data = 'test/raw_hotels_df.csv'):
    hotels_df_with_filled_coordinates = spark.read.csv(filled_coordinate_data, header=True)
    raw_hotels_df = spark.read.csv(raw_data, header=True)

    merged_hotels_df = task.merge_ordinates(hotels_df_with_filled_coordinates, raw_hotels_df)

    # After merge latitude and longitude of hotels in raw dataframe with id=42949672960 should be eaqual to 34.3435757 and -79.1660976 respectively.

    actual_latitude = merged_hotels_df.where(merged_hotels_df.Id==42949672960).select('Latitude').first()[0]
    actual_longitude = merged_hotels_df.where(merged_hotels_df.Id==42949672960).select('Longitude').first()[0]

    expected_latitude = '34.3435757'
    expected_longitude = '-79.1660976'

    assert actual_latitude==expected_latitude, f"Expected {expected_latitude} rows with null values, but found {actual_latitude}"

    assert actual_longitude==expected_longitude, f"Expected {expected_longitude} rows with null values, but found {actual_longitude}"


# assign_goehash_to_df method should return a tuple conatining weather and hotels dataframe with new column which contains geohash for particular latitude and longitude
def test_assign_goehash_to_df(weather_df_path = 'test/raw_weather.parquet', merged_hotels_df_path = 'test/merged_hotels_df.csv'):
    merged_hotels_df = spark.read.csv(merged_hotels_df_path, header = True)
    raw_weather_df = spark.read.parquet(weather_df_path, header = True)
    tuple_with_dataframes = task.assign_goehash_to_df(raw_weather_df, merged_hotels_df)

    weather_df = tuple_with_dataframes[0]
    hotels_df = tuple_with_dataframes[1]

    # Check weather dataframe contains column 'Weather_Geohash' and hotels datafrme contains 'Hotel_Geohash'
    column_to_check_weather_df = 'Weather_Geohash'
    column_to_check_hotels_df = 'Hotel_Geohash'

    is_column_present_in_weather_df = any(field.name == column_to_check_weather_df for field in weather_df.schema)
    is_column_present_in_hotel_df = any(field.name == column_to_check_hotels_df for field in hotels_df.schema)

    assert True==is_column_present_in_weather_df, f"Column {column_to_check_weather_df} is not present in weather dataframe"
    assert True==is_column_present_in_hotel_df, f"Column {is_column_present_in_hotel_df} is not present in weather dataframe"

def test_get_final_df(tuple_with_dataframes = (weather_with_geohash, hotels_with_geohash)):
    weather_df_with_geohash = tuple_with_dataframes[0]
    hotels_df_with_geohash = tuple_with_dataframes[1]

    # Let's get final dataframe by applying "get_final_df" method
    final_df = task.get_final_df(tuple_with_dataframes)

    # To confirm that the geohash column is returned after performing a left join between the weather DataFrame and the hotels DataFrame, we will validate this by checking how many times certain geohash values are repeated in the geohash column.

    # Group final datframe by "geohash column and find number of counts"
    df_groupby_geohash = final_df.groupBy(final_df.geohash).count()

    # Lets choose two random geohash codes and validate homany times its repeating.
    actual_count_of_9g88 = df_groupby_geohash.where(df_groupby_geohash.geohash == '9g88').select('count').collect()[0]['count']
    actual_count_of_9emv = df_groupby_geohash.where(df_groupby_geohash.geohash == '9emv').select('count').collect()[0]['count'] 

    expected_count_of_9g88 = 3
    actual_count_of_9emv = 2
    assert actual_count_of_9g88 == expected_count_of_9g88, f"Expected {expected_count_of_9g88} rows with null values, but found {actual_count_of_9g88}"

def test_save_to_storage(save_path = 'test/output/'):
    # Clean directories
    if  os.path.exists(save_path):
        shutil.rmtree(save_path)
    # create save path
    os.makedirs(save_path)

    task.save_to_storage(save_path = save_path)

    
    # Lets check its saved properly. We need to start spark session again since our "save_to_storgae_method" stop spark session after write.
    spark = SparkSession.builder \
    .appName("Loading saved data") \
    .getOrCreate()
    df = spark.read.parquet(save_path)

    actual_count = df.count()
    expected_count = 534
    # Test is final data is written to specidied folder.
    assert os.path.exists(save_path), f"Save path '{save_path}'  does not exist."
    assert actual_count == expected_count, f"Expected {expected_count} rows with null values, but found {actual_count}"

 
   



    

    











