from flask import Flask, render_template, request, redirect, url_for
import pandas as pd
from datetime import datetime
import os
import numpy as np

app = Flask(__name__)

# Define the port
port = 5000

ROOT_DIR = os.path.realpath(os.path.join(os.path.dirname(__file__), '.'))
# Define the working directory
app.root_path = ROOT_DIR + '/app/cbot'

# #################################################################################################

# CHECK UNIQUENESS
check_unique=True

## CARBON BOMBS
file = 'carbon_bombs_informations.csv'
unique_column='Carbon_bomb_name_source_CB'

## OR BANKS
# file = 'bank_informations.csv'
# unique_column='Bank Name'

## OR COMPANIES
# file = 'company_informations.csv'
# unique_column='Company_name'

# #################################################################################################


# Define the file paths for the original CSV and the modified CSV with tracking data
original_path = os.path.join(ROOT_DIR, 'data_cleaned', file)
modified_path = os.path.join(ROOT_DIR, 'data_sources', f'_CBOT_MODIFIED_{file}')

# Function to load data from original CSV file with ID (1-based index)
def init_data(original=True):  
    # global original_path,final_path
    # if (os.path.exists(final_path)):
    #     original_path = final_path     
    data = pd.read_csv(original_path if original else modified_path)
    # Add ID column if not present
    if data is not None and original and 'ID' not in data.columns:
        data.insert(0, 'ID', range(1, len(data) + 1)) 
    data['ID'] = data['ID'].astype(int)
    return data

# Function to load data from the CSV file
def combine_data(final=False, to_combine_data=None):
    original_data, modified_data = load_data()
        
    if (final) :  
       modified_data =  modified_data.drop_duplicates(subset='ID', keep='last') 
        
    combined_data = pd.concat([original_data, to_combine_data if to_combine_data is not None else modified_data], ignore_index=True)
    
    # print(f'COMBINE DATA MODIFIED DATA\n{modified_data}')   

    combined_data = combined_data.astype(str)
    #print(f'COMBINED DATA\n{combined_data}')
    combined_data['ID'] = combined_data['ID'].astype(int)
    combined_data = combined_data.drop_duplicates(subset='ID', keep='last')
    #print(f'COMBINED DATA drop_duplicates\n{combined_data}')    
    combined_data = combined_data.replace(['', 'nan'], np.nan)
    combined_data = combined_data.dropna(how='all', subset=combined_data.columns[1:])
    #print(f'COMBINED DATA dropna\n{combined_data}')    

    combined_data = combined_data.sort_values('ID')
    combined_data = combined_data.reset_index(drop=True)    
    # print(f'COMBINED DATA last\n{combined_data}')

    return combined_data, original_data, modified_data
    
# Function to load data from the CSV file
def load_data():
    original_data = init_data(True)
    original_data['ID'] = original_data['ID'].astype(int)
    original_data['ID'] = original_data['ID'].map('{:.0f}'.format)
    # print(f'!!!!!!!!!load_data original_data\n {original_data.head()}')
    
    try:
        modified_data = init_data(False)     
    except FileNotFoundError:
        modified_data = pd.DataFrame(columns=original_data.columns)
        
    modified_data['ID'] = modified_data['ID'].astype(int)
    modified_data['ID'] = modified_data['ID'].map('{:.0f}'.format)
    # print(f'COMBINED DATA\n{combined_data.head(1)}')
    return original_data, modified_data

# Function to save data to the modified CSV file with tracking data
def save_data(data):
    # print(f"MODIFIED DATA TO BE SAVED:\n {data}"  )
    data = data.fillna('')
    data.to_csv(modified_path, index=False)
    
def merge_data(data):
    data = data.fillna('')
    data.to_csv(original_path, index=False)

@app.route('/')
def index():
    combined_data, original_data, _ = combine_data(True)
    combined_data['ID'] = combined_data['ID'].astype(str)
    original_data['ID'] = original_data['ID'].astype(str)
    exploded_data = pd.merge(combined_data, original_data.add_prefix('original_'), left_on='ID', right_on='original_ID', how='outer')
    # print(exploded_data)   
    exploded_data['ID'] = exploded_data['ID'].fillna('0')
    exploded_data['ID'] = exploded_data['ID'].astype(int)
    exploded_data['original_ID'] = exploded_data['original_ID'].fillna('0')
    exploded_data['original_ID'] = exploded_data['original_ID'].astype(int)

    exploded_data = exploded_data.sort_values(by=['ID', 'original_ID'], key=lambda x: (x == 0), ascending=False)
    exploded_data = exploded_data.replace(['', 'nan'], np.nan)
    exploded_data = exploded_data.fillna('')

    return render_template('index.html', data=exploded_data, file=file, unique_column=unique_column, check_unique=check_unique)

@app.route('/add', methods=['POST'])
def add(to_index=True):
    combined_data, _, modified_data = combine_data()
    
    new_row = {header: request.form[header] for header in request.form.keys() if header in modified_data.columns}
    modified_data['ID']=modified_data['ID'].astype(int)
    
    new_row['ID']=combined_data['ID'].astype(int).max()+1
    # print('new_row[ID]\n'+str(new_row['ID']))
    
    new_row['date_created'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    new_row['date_modified'] = new_row['date_created']
  
    modified_data = modified_data.append(new_row, ignore_index=True)
    modified_data = modified_data.replace(['', 'nan'], np.nan)
    
    save_data(modified_data)
    if to_index:
        return redirect(url_for('index'))

@app.route('/merge', methods=['POST'])
def merge():
    # print(f'MERGING !!!!!!!!!!!!!!!!!!!!!!!!')
    update(True)
    return redirect(url_for('index'))

@app.route('/update', methods=['POST'])
def update(merge=False):
    
    id = request.form.get('original_ID') if request.form.get('ID') == '0' else request.form.get('ID')
    # print(f'ID:{id}')
    
    delete = request.form.get('ID') == '0' and request.form.get('original_ID') != '0'
    # if delete:
    #      print(f'!!!!!!!!!!! delete:{id}')    

    combined_data, original_data, modified_data  = combine_data(merge)
    # print(f'update data:\n{combined_data}')
    
    if (delete and merge):
        # print(f'!!!!!!!!!!! DELETE AND MERGE:{id}') 
        original_data = original_data[original_data['ID'].astype(int) != int(id)]
        merge_data(original_data)
        return redirect(url_for('index'))
    
    combined_data, _, modified_data  = combine_data(merge)
    # print(f'update data:\n{combined_data}')
    
    selected_row = None
    selected_row_index = combined_data.index[combined_data['ID'].astype(int) == int(id)]
    # print(f'selected_row_index:\n{selected_row_index}')
    if len(selected_row_index) != 1: 
        selected_row_index = modified_data.index[modified_data['ID'].astype(int) == int(id)]
        if len(selected_row_index) != 1: 
            return redirect(url_for('index'))
        selected_row = modified_data.loc[selected_row_index[0]]
        
    selected_row = selected_row if selected_row is not None else combined_data.loc[selected_row_index[0]]
    updated_row = {header: request.form.get(header, selected_row[header]) for header in combined_data.columns}
    updated_row['ID']=id
    if (not delete):
        updated_row['date_modified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        updated_row['date_created'] = updated_row.get('date_created', updated_row['date_modified'])

    # print(f'UPDATED:\n{updated_row}')
    if merge:
        to_combine_data = pd.DataFrame(updated_row, index=[0])
        combined_data, _, modified_data  = combine_data(False, to_combine_data)
        modified_data = modified_data[~modified_data['ID'].isin(to_combine_data['ID'])]
        merge_data(combined_data)

    modified_data=pd.concat([modified_data, pd.DataFrame(updated_row, index=[0])], ignore_index=True)   
    save_data(modified_data)
    
    return redirect(url_for('index'))

@app.route('/revert', methods=['POST'])
def revert(revert=False):
    return delete(True)
       
@app.route('/delete', methods=['POST'])
def delete(revert=False):
    _, original_data, modified_data  = combine_data()
    
    id = request.form.get('original_ID') if request.form.get('ID') == '0' else request.form.get('ID')

    revert = revert or request.form.get('ID') == '0'
    if revert:
        # print(f' ############# RESTORE:\n{restore}') 
        # print(f'--------------------------------------------------- \nORIGINAL DATA :\n{original_data}')
        
        deleted_row = original_data[original_data['ID'].astype(int) == int(id)]
        deleted_row['date_modified'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        deleted_row['date_created'] = deleted_row.get('date_created', deleted_row['date_modified']) 
    else:      
        deleted_row = pd.DataFrame(columns=modified_data.columns, index=[0])
        
    deleted_row['ID']=int(id)
    deleted_row.fillna(np.nan)
    # print(f'!!!!!!!!!!!!!DELETED:\n{deleted_row}')    
    modified_data=pd.concat([modified_data, deleted_row], ignore_index=True)  
    modified_data = modified_data.reset_index(drop=True)      
       
    save_data(modified_data)
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(port=port, debug=True)
    
