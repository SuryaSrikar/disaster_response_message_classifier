import sys
import numpy as np
import pandas as pd
from sqlalchemy import create_engine


def load_data(messages_filepath, categories_filepath):
    #Load the message file
    messages = pd.read_csv(messages_filepath)
    #Load the Categories file from CSV
    categories = pd.read_csv(categories_filepath)
    #Merge the file based on id
    df = pd.merge(messages,categories,on='id')
    return df


def clean_data(df):
    #Split the categories column with ;
    categories = df['categories'].str.split(';',expand=True)
    # select the first row of the categories dataframe
    row = categories.loc[:0,:]
    # use this row to extract a list of new column names for categories.
    # one way is to apply a lambda function that takes everything 
    # up to the second to last character of each string with slicing
    category_colnames = row.apply(lambda x: x.str.split('-')[0][0])
    # rename the columns of `categories`
    categories.columns = category_colnames
    for column in categories:
        # set each value to be the last character of the string
        categories[column] = categories[column].str.split('-')[0][1]
        # convert column from string to numeric
        categories[column] = pd.to_numeric(categories[column], downcast='signed')
    # drop the original categories column from `df`
    df.drop(columns=['categories'],inplace=True)
    # concatenate the original dataframe with the new `categories` dataframe
    df = df.join(categories)
    # drop duplicates
    df.drop_duplicates(inplace=True)
    return df


def save_data(df, database_filename):
    engine = create_engine('sqlite:///{}'.format(database_filename))
    df.to_sql('InsertTableName', engine, index=False)  

def main():
    if len(sys.argv) == 4:

        messages_filepath, categories_filepath, database_filepath = sys.argv[1:]

        print('Loading data...\n    MESSAGES: {}\n    CATEGORIES: {}'
              .format(messages_filepath, categories_filepath))
        df = load_data(messages_filepath, categories_filepath)

        print('Cleaning data...')
        df = clean_data(df)
        
        print('Saving data...\n    DATABASE: {}'.format(database_filepath))
        save_data(df, database_filepath)
        
        print('Cleaned data saved to database!')
    
    else:
        print('Please provide the filepaths of the messages and categories '\
              'datasets as the first and second argument respectively, as '\
              'well as the filepath of the database to save the cleaned data '\
              'to as the third argument. \n\nExample: python process_data.py '\
              'disaster_messages.csv disaster_categories.csv '\
              'DisasterResponse.db')


if __name__ == '__main__':
    main()