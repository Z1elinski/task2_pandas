import csv
import py7zr
import os
import pandas
import unidecode


def upload_password(path):
    # function uploads password from txt file
    try:
        with open(path, 'r') as file:
            password = file.read()
    except FileNotFoundError as e:
        print(e)
    return password


def open_zip_file(path, password):
    # function extracts files from 7zip archive
    with py7zr.SevenZipFile(path, 'r', password=password) as archive:
        archive.extractall(path=os.getcwd() + r'\data')


def finding_csv_file():
    # returns first csv file from directory
    for file in os.listdir(os.getcwd() + r'\data'):
        if file.endswith(".csv"):
            return file


# def normalize_csv_file(path):
#     # loads csv and saves it normalized version to transformed directory
#     if not os.path.exists(dataPath + r'\transformed'):
#         os.makedirs(dataPath + r'\transformed')
#     save_file_path = dataPath + r'\transformed\books_nor.csv'
#     try:
#         with open(path, 'r', encoding='utf-8') as file:
#             save_file = open(save_file_path, 'w', newline='\n')
#             reader = csv.reader(file, delimiter=',')
#             writer = csv.writer(save_file, delimiter=',')
#             for row in reader:
#                 row = ','.join(row).lower()
#                 row = unidecode.unidecode(row)
#                 row = row.split(',')
#                 writer.writerow(row)
#             save_file.close()
#     except FileNotFoundError as e:
#         print(e)


def normalize_return_df(path):
    # loads csv and returns normalized df
    if not os.path.exists(dataPath + r'\transformed'):
        os.makedirs(dataPath + r'\transformed')
    try:
        with open(path, 'r', encoding='utf-8') as file:
            temp = []
            reader = csv.reader(file, delimiter=',')
            for row in reader:
                row = ','.join(row).lower()
                row = unidecode.unidecode(row)
                row = row.split(',')
                temp.append(row)

            dict_temp = {a[0]: list(a[1:]) for a in zip(*temp)}
            df = pandas.DataFrame(dict_temp)
            df['ratings_count'] = df['ratings_count'].astype(int)
            df['bookid'] = df['bookid'].astype(int)

            return df
    except FileNotFoundError as e:
        print(e)


def df_to_parquet(df):
    if not os.path.exists(dataPath + r'\transformed'):
        os.makedirs(dataPath + r'\transformed')
    save_file_path = dataPath + r'\transformed\books_nor.parquet'
    df.to_parquet(save_file_path)
    print('parquet file created')


if __name__ == "__main__":
    dataPath = os.getcwd() + r'\data'
    passwordPath = os.path.join(dataPath, 'pass.txt')
    archivePath = os.path.join(dataPath, 'books_data.7z')
    archivePassword = upload_password(passwordPath)
    open_zip_file(archivePath, archivePassword)
    csvFilePath = os.path.join(dataPath, finding_csv_file())
    # normalize_csv_file(csvFilePath)

    booksDataFrame = normalize_return_df(csvFilePath)
    # print(booksDataFrame)
    df_to_parquet(booksDataFrame)

    excelPath = dataPath + r'\transformed\books_nor.xlsx'
    excelWriter = pandas.ExcelWriter(excelPath)
    # dfa is a dataframe for task 5a
    dfa = booksDataFrame.copy()
    dfa = dfa.sort_values('ratings_count', ascending=False)
    dfa = dfa.head(200)
    dfa = dfa.sort_values('average_rating', ascending=False)
    dfa = dfa.head(50)
    dfa.to_excel(excelWriter, sheet_name='Top 50 books')

    # dfb is a dataframe for task 5b
    dfb = booksDataFrame.copy()
    dfb = dfb[dfb['publication_date'] < '1/1/2000']
    dfb = dfb.sort_values('average_rating', ascending=False)
    dfb = dfb.drop(columns='isbn13')
    dfb.to_excel(excelWriter, sheet_name='Best books before 2000s')
    # print(dfb.keys())

    # dfc is a dataframe for task 5c
    dfc = booksDataFrame.copy()
    dfc = dfc[dfc['publisher'].str.contains("university" or "academic")]
    dfc = dfc.sort_values('average_rating', ascending=False)
    dfc = dfc.drop(columns='isbn13')
    dfc.to_excel(excelWriter, sheet_name='University Published')
    # print(dfc.keys())

    # dfd is a dataframe for task 5d
    dfd = booksDataFrame.copy()
    dfd = dfd[dfd["  num_pages"] < '100']
    dfd = dfd[dfd["ratings_count"] > 1000]
    dfd = dfd.sort_values('average_rating', ascending=False)
    dfd = dfd.head(50)
    dfd.to_excel(excelWriter, sheet_name='Best short stories')

    # dfe is a dataframe for task 5e
    dfe = booksDataFrame.copy()
    dfeGrouped = dfe.groupby('authors', as_index=False)['ratings_count'].sum()
    dfeGrouped = dfeGrouped.sort_values('ratings_count', ascending=False)
    # print(dfeGrouped)
    mostRatedAuthor = dfeGrouped['authors'].iloc[0]
    # print(mostRatedAuthor)
    dfe = dfe[dfe['authors'] == mostRatedAuthor]
    dfe = dfe.sort_values('average_rating', ascending=False)
    dfe.to_excel(excelWriter, sheet_name='Most popular author')

    excelWriter.save()
