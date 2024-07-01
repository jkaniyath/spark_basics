from pyspark.sql import SparkSession
from pyspark.sql.types import * 
from dotenv import load_dotenv
import os 
from opencage.geocoder import OpenCageGeocode
from pyspark.sql.window import Window
from pyspark.sql.functions import * 
import pygeohash as geohash

load_dotenv()
storage_name = os.getenv("STORAGE_NAME")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
tenant_id = os.getenv("TENANT_ID")
key = os.getenv("KEY")


class Task1:
    def __init__(self, az_storage_name=storage_name, az_client_id = client_id, az_client_secret = client_secret, az_tenant_id = tenant_id, open_cage_key = key,  hotels_df_path=f'abfs://data@{storage_name}.dfs.core.windows.net/hotels/*.csv', weather_df_path = f'abfs://data@{storage_name}.dfs.core.windows.net/weather/'):
        self.hotels_path = hotels_df_path
        self.weather_path = weather_df_path
        self.storage_name = az_storage_name
        self.client_id = az_client_id
        self.client_secret = az_client_secret
        self.tenant_id = az_tenant_id
        self.key = open_cage_key
        self.spark = self.get_spark_session()

    def get_spark_session(self):
        return SparkSession.builder \
                        .appName("Task 1") \
                        .config(f"fs.azure.account.auth.type.{self.storage_name}.dfs.core.windows.net", "OAuth") \
                        .config(f"spark.hadoop.fs.azure.account.oauth.provider.type.{self.storage_name}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider") \
                        .config(f"fs.azure.account.oauth2.client.id.{self.storage_name}.dfs.core.windows.net", self.client_id) \
                        .config(f"fs.azure.account.oauth2.client.secret.{self.storage_name}.dfs.core.windows.net", self.client_secret) \
                        .config(f"fs.azure.account.oauth2.client.endpoint.{self.storage_name}.dfs.core.windows.net", f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token") \
                        .config("spark.sql.broadcastTimeout", 600) \
                        .getOrCreate()  

    # Schema enforcment for hotel dataframe
    def define_hotel_schema(self):
        schema = StructType([
        StructField("Id", LongType(), True),
        StructField("Name", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Latitude", DoubleType(), True),
        StructField("Longitude", DoubleType(), True)
        ])
        return schema
    # Schema enforcment for weather dataframe
    def define_weather_schema(self):
        schema = StructType([
        StructField("lng", DoubleType(), True),
        StructField("lat", DoubleType(), True),
        StructField("avg_tmpr_f", DoubleType(), True),
        StructField("avg_tmpr_C", DoubleType(), True),
        StructField("wthr_date", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True)
        ])
        return schema

    # Load hotels dataframe
    def load_hotels_df(self, path):
        hotel_schema =  self.define_hotel_schema()
        spark = self.get_spark_session()
        df = spark.read.format('csv').schema(hotel_schema) \
                    .option('header', True) \
                    .load(path)
        return df 
    
    # Load weather dataframe dataframe
    def load_weather_df(self, path):
        schema = self.define_weather_schema()
        spark = self.get_spark_session()
        df = spark.read.format('parquet').schema(schema) \
                    .option('header', True) \
        .load(path)
        return df
    
    # Function to assign coordinates for hotels without latitude and longitude by using Opencage geocoder.
    def assign_coordinates(self, hotels_df):
        # get hotels dataframe
        geocoder = OpenCageGeocode(key)

        # Create a DataFrame from hotels that don't have any coordinates
        raw_filtered_df = hotels_df.where((hotels_df.Latitude.isNull()) | (hotels_df.Longitude.isNull()))

        # Create User defined functions to fill latitude and longitude using Opencage geocoder.
        def lat_geo_coding(address, city, country):
            query = address + ', ' + city + ', ' + country
            lat = geocoder.geocode(query)[0]['geometry']['lat']
            return lat 

        def lng_geo_coding(address, city, country):
            query = address + ', ' + city + ', ' + country
            lng =  geocoder.geocode(query)[0]['geometry']['lng']
            return lng 
        # Register above created UDF
        get_lat_udf = udf(lat_geo_coding, DoubleType())
        get_lng_udf = udf(lng_geo_coding, DoubleType())

        filtered_df = raw_filtered_df.withColumn('Latitude', get_lat_udf(raw_filtered_df.Address, raw_filtered_df.City, raw_filtered_df.Country)) \
                        .withColumn('Longitude', get_lng_udf(raw_filtered_df.Address, raw_filtered_df.City, raw_filtered_df.Country)) \
                        .select('Id', 'Latitude', 'Longitude') \
                        .withColumnRenamed('Id', 'rawDfId') \
                        .withColumnRenamed('Latitude', 'rawDfLatitude') \
                        .withColumnRenamed('Longitude', 'rawDfLongitude')

        return filtered_df
    
    # Function to fill NULL values for hotels coordinates by using filtered dataframe which we created in above function using  Opencage.
    def merge_ordinates(self, filterd_hotels_df, raw_hotels_df):
        # Left join hotels dataframe with filtered dataframe to get coordinates and create final hotels datframe with coordinates.
        # We can use broadcast join since the size of filtered dataframe is very small compared to raw_hotels dataframe to perform faster join and avoid shuffling. 
        join_df = raw_hotels_df.join(broadcast(filterd_hotels_df), raw_hotels_df.Id == filterd_hotels_df.rawDfId, "left")
        # Fill null values from left table using coalesce function
        result = (join_df.withColumn('Latitude', coalesce('Latitude', 'rawDfLatitude'))   
                .withColumn('Longitude', coalesce('Longitude', 'rawDfLongitude')) 
                .select('Id','Name','Country','City','Address', 'Latitude','Longitude'))

        return result 
    
    # Function to assign geohash to both weather and hotels dataframe using pygeohash and return them in a tuple.
    def assign_goehash_to_df(self, raw_weather_df, raw_hotels_df):
        # Convert to DoubleType before apply geohash encode
        raw_weather_df = raw_weather_df.withColumn("lat", raw_weather_df.lat.cast(DoubleType()))\
                                      .withColumn("lng", raw_weather_df.lng.cast(DoubleType()))
        
        raw_hotels_df = raw_hotels_df.withColumn("Latitude", raw_hotels_df.Latitude.cast(DoubleType()))\
                                     .withColumn("Longitude", raw_hotels_df.Longitude.cast(DoubleType()))
        
        # User defiend function to assign geohash.
        def func_to_assign_geohash(latitude, longitude, precesion=4):
            geohash_code = geohash.encode(latitude, longitude, precesion)
            return geohash_code
        # Register udf
        df_geohash_func = udf(lambda latitude, longitude: func_to_assign_geohash(latitude, longitude),  StringType())
        # Create weather dataframe with geohash
        weather_df_with_geohash = raw_weather_df.withColumn('Weather_Geohash', df_geohash_func(raw_weather_df.lat, raw_weather_df.lng))
         # Create hotel dataframe with geohash
        hotel_df_with_geohash = raw_hotels_df.withColumn('Hotel_Geohash', df_geohash_func(raw_hotels_df.Latitude, raw_hotels_df.Longitude))

        return (weather_df_with_geohash, hotel_df_with_geohash)
    
    def get_final_df(self, tuple_with_dataframes):
        # Tuple contains weather DataFrame at index 0 and the hotels DataFrame at index 1
        raw_weather_df = tuple_with_dataframes[0]
        hotels = tuple_with_dataframes[1]
        
        """
        To avoid data duplication and prevent duplicate rows, we need to remove geohashes that appear 
        multiple times on a single date. This ensures each geohash is represented only once per date, 
        thereby preventing data multiplication when left-joining with the hotel dataframe.
        """
        # repartiton raw weather dataframe before remove duplicates to improve performance
        repartitoned_weather_df = raw_weather_df.repartition( 'wthr_date', 'Weather_Geohash')
        # remove duplicate geohashes that appear  multiple times on a single date.
        unique_weather_df = repartitoned_weather_df.dropDuplicates(["wthr_date", "Weather_Geohash"])

        # Left join weather_df with hotels df
        joined_df = unique_weather_df.join(broadcast(hotels), unique_weather_df.Weather_Geohash==hotels.Hotel_Geohash, 'left')
        # List of columns to remove from final dataframe.
        columns_to_drop = ['Row_Num','Hotel_Geohash']
        # Remove dupilcate geohash column of hotels plus row_num column which we dont need.
        joined_df = joined_df.withColumnRenamed('Weather_Geohash', 'geohash' ).drop(*columns_to_drop)

        return joined_df
    
    # Save left joined weather and hotels dataframe to azure storage
    def save_to_storage(self, save_path):
        # get both hotels and weather dataframes
        raw_hotels_df = self.load_hotels_df(path=self.hotels_path)
        raw_weather_df = self.load_weather_df(path=self.weather_path)

        """
         Get all rows from the raw hotels dataframe with missing Latitude and Longitude, 
         then fill in the missing values and return the filtered dataframe. (We filter the dataframe to make the process faster by working with a smaller dataframe)
        """
        filtered_hotels_df = self.assign_coordinates(raw_hotels_df)

        # Join raw hotels dataframe with filterd hotels datframe to get values for miising cordinates.
        hotels_with_filled_coordinates = self.merge_ordinates(raw_hotels_df= raw_hotels_df, filterd_hotels_df=      filtered_hotels_df)

        # Assign geohash to both hotels and weather dataframes
        tuple_with_dataframes = self.assign_goehash_to_df(raw_weather_df=raw_weather_df, raw_hotels_df=hotels_with_filled_coordinates)

        # Get final joined weather and hotels dataframe
        final_df = self.get_final_df(tuple_with_dataframes= tuple_with_dataframes)

        final_df.write.option('header', True) \
                .partitionBy('year', 'month', 'day') \
                .mode('overwrite') \
                .parquet(save_path)
        self.spark.stop()
      

if __name__ == '__main__':
    task = Task1()
    task.save_to_storage(save_path = f"abfs://data@{storage_name}.dfs.core.windows.net/output/")

    

