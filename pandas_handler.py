from database_handler import read_data_as_dataframe
import pandas as pd
from lookups import Sources, InputTypes, ErrorHandling
from logging_handler import show_error_message 

def dataframes_cleansed():
    cleaned_dataframes = {}
    try:
        df = read_data_as_dataframe(InputTypes.CSV, Sources.listings_source.value)
        columns_to_drop = [
            'scrape_id', 'source', 'host_acceptance_rate', 'host_thumbnail_url', 'host_picture_url', 
            'host_listings_count', 'host_has_profile_pic', 'neighbourhood_group_cleansed', 'bathrooms', 'calendar_updated',
            'number_of_reviews_ltm', 'number_of_reviews_l30d', 'calendar_last_scraped', 'review_scores_accuracy',
            'review_scores_cleanliness', 'review_scores_checkin', 'review_scores_communication', 'review_scores_location',
            'review_scores_value', 'host_about', 'neighborhood_overview',
            'minimum_minimum_nights', 'maximum_minimum_nights', 'minimum_maximum_nights', 'maximum_maximum_nights',
            'minimum_nights_avg_ntm', 'maximum_nights_avg_ntm', 'license', 'host_verifications', 'property_type',
            'availability_60', 'availability_90', 'calculated_host_listings_count',
            'calculated_host_listings_count_entire_homes', 'calculated_host_listings_count_private_rooms',
            'calculated_host_listings_count_shared_rooms', 'neighbourhood'
        ]
        df = df.drop(columns=columns_to_drop)

        column_mapping = {
            'last_scraped': 'booking_date', 'name' : 'listing_name', 'id': 'listing_id', 'neighbourhood_cleansed': 'neighbourhood_view',
            'room_type': 'property_type', 'bathrooms_text': 'bathrooms', 'description' : 'listing_description', 'first_review' : 'listing_date'
        }
        df = df.rename(columns=column_mapping)

        df['listing_name'] = df['listing_name'].str.split('·').str.get(0).str.strip()

        df['price'] = df['price'].str.replace('[\$,]', '', regex=True)

        df['price'] = pd.to_numeric(df['price'])

        df['listing_description'] = df['listing_description'].str.replace('<br />', '').str.replace('<b>', '').replace('</b>', '')

        columns_to_fill = [
            'listing_description', 'host_name', 'host_since', 'host_identity_verified', 'host_location',
            'host_response_time', 'host_response_rate', 'host_neighbourhood'
        ]
        df[columns_to_fill] = df[columns_to_fill].fillna('unspecified')

        df['host_total_listings_count'] = df['host_total_listings_count'].fillna(1)
        df['review_scores_rating'].fillna(df['review_scores_rating'].mean(), inplace=True)
        df['reviews_per_month'].fillna(0, inplace=True)
        df['host_is_superhost'] = df['host_is_superhost'].fillna('unspecified')
        df['last_review'] = pd.to_datetime(df['last_review'], errors='coerce').fillna(0)

        df[['bathrooms', 'bedrooms', 'beds']] = df[['bathrooms', 'bedrooms', 'beds']].fillna(0)
        df['host_since'] = pd.to_datetime(df['host_since'], errors='coerce')

        df['listing_id'] = df['listing_id'].astype('object')

        df['booking_date'] = pd.to_datetime(df['booking_date'])

        df['listing_date'] = pd.to_datetime(df['listing_date'])

        df['price'] = pd.to_numeric(df['price'])

        print("First dataframe was executed")

        df_2 = read_data_as_dataframe(InputTypes.CSV, Sources.reviews_source.value)
        df_2.rename(columns={'date':'booking_date', 'id':'review_id'}, inplace=True)
        df_2['comments'] = df_2['comments'].fillna('unspecified')
        df_2['listing_id'] = df_2['listing_id'].astype('object')
        df_2['booking_date'] = df_2['booking_date'].apply(pd.to_datetime)
        df_2['comments'] = df_2['comments'].replace('<br/>', '', regex=True)
        print("Second dataframe was executed!")

        cleaned_dataframes = {
            'cleaned_df1' : df,
            'cleaned_df2' : df_2
        }    

    except Exception as error:
        show_error_message(ErrorHandling.PANDAS_HANDLER_ERROR.value, str(error))
    finally:
        return cleaned_dataframes