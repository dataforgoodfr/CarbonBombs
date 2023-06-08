import pandas as pd

# 
def manual_update_carbon_bombs(df_cb):
    #print(list(df_carbon_bombs.columns))
    
    # ChatGPT
    # Bab (Gasco) seems to be the company and not the carbon bomb unit
    # in September 2021, Bab Gasco is owned by the Abu Dhabi National Oil Company (ADNOC)
    # Bab Gasco operates in the Bab field, which is located in the onshore region of Abu Dhabi, United Arab Emirates (UAE). 
    # The Bab field is situated in the western part of Abu Dhabi, near the city of Al Dhafra. 
    # The Bab Field, also known as the Bab Oil Field, is located in Abu Dhabi, United Arab Emirates. 
    # The coordinates for the Bab Field are approximately:
    # The Bab Oil Field is located in the western region of Abu Dhabi, United Arab Emirates. 
    # Its precise coordinates are approximately latitude 24.0133° N and longitude 52.6375° E. 
    # The oil field covers a considerable area within this region.
    # Carbon bomb ref to modifiy
    cb_ref='Bab (Gasco)' 
    # TODO: maybe have more generic column names like carbon_bomb_name, country, parent_company, 
    # without source suffixes to be able to do this replace
    cb_data = {
        'Carbon_bomb_name_source_CB': 'Bab Oil Field',
        'Country_source_CB': 'United Arab Emirates',
        'Latitude': 24.0133,
        'Longitude': 52.6375,
        'Latitude_longitude_operator_source': 'ChatGPT',
        'Operators_source_GEM': 'Bab Gasco',
        'Parent_company_source_GEM': 'Abu Dhabi National Oil Company (ADNOC) (100%)',
    }
    
    matching_cb = df_cb[df_cb['Carbon_bomb_name_source_CB'] == cb_ref]
    
    if matching_cb is not None and matching_cb.shape[0] == 1:
        # Update the matching row with the new data
        df_cb.loc[matching_cb.index, list(cb_data.keys())] = pd.DataFrame(cb_data, index=[0])[list(cb_data.keys())].values
    else:
        print(f"ERROR: None or Several matching row(s) for {cb_ref} !!!!" )

    #print(df_cb.loc[matching_cb.index]) 
    return df_cb
    
# For test    
# if __name__ == '__main__':
#     # Main function
#     import os
#     ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '.'))
#     df_carbon_bombs = pd.read_csv(os.path.join(ROOT_DIR, 'data_cleaned', 'carbon_bombs_informations.csv'))
#     df_carbon_bombs = manual_match_carbon_bombs(df_carbon_bombs)
#     #df.to_csv(os.path.join(ROOT_DIR, 'data_cleaned', 'carbon_bombs_informations_updated.csv'),index=False)
     
