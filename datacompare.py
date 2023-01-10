#!/usr/bin/python
# -*- coding: UTF-8 -*-


import pandas as pd
import numpy as np
import datacompy
from  yattag import Doc
import os
from config import Config
import shutil
from datetime import datetime
import logging

logging.basicConfig(format =  "%(levelname)s:%(message)s" ,level=logging.INFO)

def generate_html_report(snippet):
    """ 
    generate a full html page with given html snippet 
    return the html content
    """
    doc, tag, text = Doc().tagtext()
    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        with tag('head'):
            with tag('style'):
                css = """
                    body {
                    font-family: 'lato', sans-serif;
                    }

                    h2 {
                    font-size: 26px;
                    margin: 20px 0;
                    text-align: center;
                    }

                    h2 label {
                       font-size: 18px; 
                    }

                    h3 {
                    font-size: 18px;
                    text-align: left;
                    }

                    .container {
                    font-family: Arial, Helvetica, sans-serif;
                    border-collapse: collapse;
                    width: 70%;
                    }

                    .container td, .container th {
                    border: 1px solid #ddd;
                    padding: 8px;
                    }

                    .container tr:nth-child(even){background-color: #f2f2f2;}

                    .container tr:hover {background-color: #ddd;}

                    .container th {
                    padding-top: 12px;
                    padding-bottom: 12px;
                    text-align: left;
                    background-color: #4CAF50;
                    color: white;
                    }

                    .container .hl{
                        background-color: #f5f576;
                    }     
                    .container .jc{
                        background-color: #76f5d2;
                    } 
                    .container .hd{
                        background-color: #098FF5;
                    } 
                """
                doc.asis(css)

        with tag('body'):
            doc.asis(snippet)

    return doc.getvalue()

def create_html_snippet(groupbyresult , title):
    """ 
    create table for group by each column
    """
    doc, tag, text = Doc().tagtext()
    with tag('h2'):
        text(title)
    for groupbyitem in groupbyresult:
        column_name = groupbyitem[0]
        size_series = groupbyitem[1]
        #line('label' ,column_name )
        with tag('h3'):
            text('GROUP BY \'' + column_name + '\'')
        with tag('table' , klass = 'container'):
            with tag('tr'):
                with tag('td'):
                    text('value')
                with tag('td'):
                    text('row count')
            size_series = size_series.sort_values(ascending = False)
            if len(size_series) >= 10 :
                size_series = size_series.take(range(10))
            for i in range(len(size_series)):
                with tag('tr'):
                    with tag('td'):
                        text(str(size_series.index[i]))
                    with tag('td'):
                        text(size_series.to_list()[i])
        doc.stag('br')
        doc.stag('br')
    return doc.getvalue()


def write_html_file(html_generation_dir , filename, content):
    """ 
    write out the html page in a local directory 
    if the file has existed , then the original file will be backed up 
    and rename it with timestamp as suffix 
    """
    file_path = html_generation_dir + '/' + filename
    bk_file_path = html_generation_dir + '/' + filename + datetime.now().strftime('%Y-%m-%d_%H_%M') + '.html'
    if os.path.exists(file_path):
        shutil.copy2(file_path , bk_file_path)
    file = open(html_generation_dir + '/' + filename, "w+")
    file.seek(0, 0)
    file.writelines(content)
    logging.info('%s is generated' , file_path)




def dataframe_explode(df , column_name , delimiter):
    """
    convert a row to multiple rows against given column and delimiter
    """
    df[column_name] = df[column_name].map(lambda x : str(x).split(delimiter))
    return df.explode(column_name)


def crate_diff_html_snippet(compareresult,max_show_rows, title , section):
    """
    create html snippet for compare page
    """
    iiq_unq_rows = compareresult.df1_unq_rows
    bq_unq_rows = compareresult.df2_unq_rows
    intersect_diff_rows = compareresult.all_mismatch()
    join_columns = compareresult.join_columns

    doc, tag, text = Doc().tagtext()
    with tag('h2'):
        text(title)
        with tag('label'):
            text('   for ' + section)
    with tag('ul'):
        with tag('li'):
            with tag('strong'):
                text('IIQ unique rows : '  )
        with tag('li'):
            if len(iiq_unq_rows) == 0 :
                text('Empty')
            else :
                doc.asis(create_snippet_from_df(iiq_unq_rows , max_show_rows , join_columns))
    doc.stag('br')
    doc.stag('br')
    with tag('ul'):
        with tag('li'):
            with tag('strong'):
                text('BQ unique rows : '  )
        with tag('li'):
            if len(bq_unq_rows) == 0 :
                text('Empty')
            else :
                doc.asis(create_snippet_from_df(bq_unq_rows , max_show_rows , join_columns))
    doc.stag('br')
    doc.stag('br')
    with tag('ul'):
        with tag('li'):
            with tag('strong'):
                text('intersect different rows : '  )
        with tag('li'):
            if len(intersect_diff_rows) == 0 :
                text('Empty')
            else :
                doc.asis(create_snippet_from_df(intersect_diff_rows , max_show_rows ,  join_columns ,highligh = True ))
    doc.stag('br')
    doc.stag('br')
    return doc.getvalue()

def create_snippet_from_df(df ,max_show_rows , join_columns , highligh = False):
    """
    create html snippet based on a dataframe
    """
    original_row_count = len(df)
    if len(df) > max_show_rows:
        df = df.take(range(max_show_rows))
    doc, tag, text = Doc().tagtext()
    df_columns = df.columns
    df_array = df.values
    with tag('table' , klass = 'container'):
        with tag('tr'):
            for column in df_columns:
                with tag('td' , klass = 'hd'):
                    text(column)
        result = detect_highlight_part(df_array, join_columns , highligh)
        for i in range(len(df_array)):
            with tag('tr'):
                for j in range(len(df_columns)):
                    if result[i][j]:
                        with tag('td'):
                            text(str(df_array[i][j]))
                    elif result[i][j] == None:
                        with tag('td', klass = 'jc'):
                            text(str(df_array[i][j]))
                    else:
                        with tag('td', klass = 'hl'):
                            text(str(df_array[i][j]))
    with tag("h3"):
        text("Total Row Count " + str(original_row_count))
    return doc.getvalue()


def detect_highlight_part(arr, cols , highlight):
    """
    highlight the difference parts in compare page
    return the index list, 
    if the part need highlight, the value will be False in the list
    """
    result = []
    skips = len(cols)
    if highlight :
        for k in range(len(arr)):
            line = []
            for i in range(len(arr[k])):
                if i < skips:
                    line.append(None)
                elif (i - skips) % 2 == 0 :
                    if arr[k][i] == arr[k][i+1] or  bool(pd.isna(arr[k][i]) and  pd.isna(arr[k][i+1])):
                        line.append(True)
                        line.append(True)
                    else :
                        line.append(False)
                        line.append(False)
                else : 
                    pass
            result.append(line)
    else :
        for k in range(len(arr)):
            line = []
            for i in range(len(arr[k])):
                if len(arr[k]) - skips <= i < len(arr[k]):
                    line.append(None)                   
                else : 
                    line.append(True)
            result.append(line)
    return result



def get_files_in_folder(path):
    """
    pandas read all csv files in the folder
    """
    dfs = []
    for (root , dirs , files ) in os.walk(path):
        for file in files:
            if file.endswith('csv'):
                dfs.append(pd.read_csv(path + '/'  + file , encoding = 'cp1252' ,encoding_errors = 'ignore'))
    return pd.concat(dfs ,ignore_index = True)


def exclude_columns_when_compare(excldue_columns , df):
    """
    exclude the columns from the dataframe
    """
    df.drop(columns = excldue_columns ,inplace = True)


def write_download_files(compare , options , html_generation_dir):
    """
    write out the download files
    """
    iiq_unq_rows = compare.df1_unq_rows
    bq_unq_rows = compare.df2_unq_rows
    intersect_diff_rows = compare.all_mismatch()
    for option in options:
        if eval(options[option]):
            if option == 'download_unique_iiq':
                filename = html_generation_dir + '/' + 'unique_iiq.csv'
                iiq_unq_rows.to_csv(filename , index=False ,mode = 'w')
            if option == 'download_unique_bq':
                filename = html_generation_dir + '/' + 'unique_bq.csv'
                bq_unq_rows.to_csv(filename , index=False ,mode = 'w')
            if option == 'download_intersect_rows':
                filename = html_generation_dir + '/' + 'intersect_rows.csv'
                intersect_diff_rows.to_csv(filename , index=False ,mode = 'w')



if __name__ == '__main__':
    # a section starnd for a test scenario which configured in config.json
    # choose your secion ,
    section = 'denied_roles'
    # section = 'kpi_report'
    logging.info( 'compare data for %s', section)

    # instantiate an object for Config
    config = Config(section)
    IIQ_FILE_SOURCE=config.get_from_iiq('file_source')
    BQ_FILE_SOURCE=config.get_from_bq('file_source')
    logging.info('IIQ source data path: %s', IIQ_FILE_SOURCE)
    logging.info('BQ source data path: %s', BQ_FILE_SOURCE)

    # if data path is directory then read all files in the folder else read the single file
    if os.path.isdir(IIQ_FILE_SOURCE):
        df1 = get_files_in_folder(IIQ_FILE_SOURCE)
    else :
        df1 = pd.read_csv(IIQ_FILE_SOURCE,encoding = 'cp1252' ,encoding_errors = 'ignore')

    if os.path.isdir(BQ_FILE_SOURCE):
        df2 = get_files_in_folder(BQ_FILE_SOURCE)
    else :
        df2 = pd.read_csv(BQ_FILE_SOURCE,encoding = 'cp1252' ,encoding_errors = 'ignore')

    #fetch the renaming mapping, rename BQ dataframe  
    rename_mapping = config.get_from_bq('rename_mapping')
    df2.rename(columns = rename_mapping, inplace = True  )

    #explode the rows using the column and delimiter
    df1_explode_columns = config.get_from_iiq("explode_columns")
    df2_explode_columns = config.get_from_bq("explode_columns")
    if len(df1_explode_columns) > 0:
        for d in df1_explode_columns:
            df1 = dataframe_explode(df1 ,d , df1_explode_columns[d] )
    
    if len(df2_explode_columns) > 0:
        for d in df2_explode_columns:
            df2 = dataframe_explode(df2 ,d , df2_explode_columns[d] )

    #exclude columns
    df1_excldue_columns = config.get_from_bq("exclude_columns")
    df2_excldue_columns = config.get_from_bq("exclude_columns")
    exclude_columns_when_compare(df1_excldue_columns,df1)
    exclude_columns_when_compare(df2_excldue_columns,df2)

    #set type for datetime column
    df1_assign_type = config.get_from_iiq("assign_type")
    df2_assign_type = config.get_from_bq("assign_type")
    if len(df1_assign_type) > 0 :
        for d in df1_assign_type:
            if df1_assign_type[d] == 'datetime' :
                df1[d]= pd.to_datetime(df1[d])
            if df1_assign_type[d] == 'int' :
                df1[d]= pd.to_numeric(df1[d])
            if df1_assign_type[d] == 'str' :
                df1 = df1.astype({d :'str' })  
    if len(df2_assign_type) > 0 :
        for d in df2_assign_type:
            if df2_assign_type[d] == 'datetime' :
                df2[d]= pd.to_datetime(df2[d])
            if df2_assign_type[d] == 'int' :
                df2[d]= pd.to_numeric(df2[d])
            if df2_assign_type[d] == 'str' :
                df2 = df2.astype({d :'str' })  

    #final columns
    df1_columns = [ i for i  in df1.columns ]
    df2_columns = [ i for i  in df2.columns ]

    #group by each columns 
    df1_groupbyresult = [ ( x , df1.groupby(x).size() ) for x in df1_columns ]
    df2_groupbyresult = [  (x  ,df2.groupby(x).size() ) for x in df2_columns ]

    #fetch the html generation directory
    html_generation_dir=config.get_from_compare('html_generation_dir')
    # create html snippet for IIQ groupby result 
    content = create_html_snippet(df1_groupbyresult , 'IIQ data report')
    html = generate_html_report(content)
    filename = 'IIQ-data-report.html'
    write_html_file(html_generation_dir , filename , html )

    # create html snippet for BQ groupby result 
    content2 = create_html_snippet(df2_groupbyresult , 'BQ data report')
    html2 = generate_html_report(content2)
    filename2 = 'BQ-data-report.html'
    write_html_file(html_generation_dir , filename2 , html2 )

    # fetch join columns config
    join_columns = config.get_from_compare('join_columns')
    # fetch max show rows config
    max_show_rows = config.get_from_compare('max_show_rows')
    # fetch download config
    download_options = {'download_unique_iiq' : config.get_from_compare('download_unique_iiq'),
    'download_unique_bq' : config.get_from_compare('download_unique_bq'),
    'download_intersect_rows' : config.get_from_compare('download_intersect_rows')}

    # compare the df1 and df2
    compare = datacompy.Compare(df1, df2 , join_columns= join_columns , df1_name = 'IIQ' , df2_name = 'BQ')
    # write out the download files
    write_download_files(compare, download_options , html_generation_dir)
    diff_content = crate_diff_html_snippet(compare ,max_show_rows , "IIQ/BQ compare report" , section)
    diff_html = generate_html_report(diff_content)
    filename3 = 'compare-report.html'
    # write out the compare report html
    write_html_file(html_generation_dir , filename3 , diff_html )
    logging.info('data compare end')
