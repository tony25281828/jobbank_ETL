import os
import re

import bs4
import numpy as np
import requests
import pandas as pd
from bs4 import BeautifulSoup
import sys
import datetime
import time
import pymysql


TAIWAN_CITY = ['台北市', '新北市', '基隆市', '桃園市', '新竹市', '新竹縣', '苗栗縣', '台中市',
               '南投縣', '彰化縣', '雲林縣', '嘉義縣', '台南市', '高雄市', '屏東縣', '宜蘭縣',
               '花蓮縣', '台東縣', '澎湖縣', '金門縣', '連江縣']


def download_data(url, job_number, jobInfoList, search_job_num=0):

    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.131 Safari/537.36'
    }
    # 如果連線失敗，自動重新連線
    isNotConnected = True
    while isNotConnected:
        try:
            req = requests.get(url, headers=headers)
            isNotConnected = False
        except:
            print('無法連上網路伺服器，請稍後再試')
            print('1分鐘後重新嘗試連線')
            time.sleep(60)

    # 用BeautifulSuop獲取資訊以便使用以tag搜索功能
    root = BeautifulSoup(req.text, 'lxml')
    job_basic_infos = root.find_all('div', class_='item__job')
    for jobNum in range(len(job_basic_infos)):

        # 第幾份工作
        item = job_basic_infos[jobNum]

        # 先查看工作地點，如果不是在台灣，直接跳過
        # 工作地點
        try:
            location = item.find('i', class_='item__job-prop-item item__job-prop-workcity').get('aria-label')
        except:
            location = ''
        if location[0:3] not in TAIWAN_CITY:
            continue

        search_job_num -= 1
        job_number += 1
        if job_number%2000 == 0:
            print('正在下載第' + str(job_number) + '份工作...')

        # 工作職位名稱
        try:
            job_title = item.find('a', class_='item__job-info--link item__job-position0--link').get('title')
        except:
            job_title = None
        # company_info，用來擷取company_name、company_cat、address
        try:
            company_info = item.find('a', class_='item__job-info--link item__job-organ--link').get('title')
            company_info = re.sub(r'[^\w]', ' ', company_info)
        except:
            company_info = None
        # 從company_info擷取company_name
        try:
            company_name = company_info[company_info.index('公司名稱') + 5:company_info.index('行業類別') - 1]
            company_name = re.sub(r'[^\w]', ' ', company_name)
        except:
            company_name = None
        # 從company_info擷取company_cat
        try:
            company_cat = company_info[company_info.index('行業類別') + 5:company_info.index('公司住址') - 1]
            company_cat = re.sub(r'[^\w]', ' ', company_cat)
        except:
            company_cat = None
        # 公司地址
        try:
            address = company_info[company_info.index('公司住址') + 5:]
        except:
            address = None
        # 薪資待遇
        try:
            salary = item.find('i', class_='item__job-prop-item item__job-prop-salary').get('aria-label')
        except:
            salary = None
        # 工作經驗
        try:
            working_exp = item.find('i', class_='item__job-prop-item item__job-prop-experience').get('aria-label')
        except:
            working_exp = None
        # 學歷
        try:
            education = item.find('i', class_='item__job-prop-item item__job-prop-grade').get('aria-label')
        except:
            education = None
        # 相關科系
        try:
            # 因為html5有新寫法，'data-*'。這種新寫法的class都一樣，所以使用attrs來搜尋
            degree = item.find('span', attrs={'data-e': '相關科系'}).text
        except:
            degree = None
        # 駕照種類
        try:
            driver_license = item.find('span', attrs={'data-e': '駕照'}).text
        except:
            driver_license = None
        # 自備的交通工具
        try:
            vehicles = item.find('span', attrs={'data-b': ' 自備'}).text
        except:
            vehicles = None

        # 將此工作資訊轉成一個list
        jobInfo = [job_title, company_name, company_cat, location, address,
                   salary, working_exp, education, degree, driver_license, vehicles]
        # 將此工作加入所有工作的list
        jobInfoList.append(jobInfo)
        if search_job_num == 0:
            print('下載完畢...')
            return jobInfoList
    # 尋找下一頁的url
    try:
        url = 'https://www.1111.com.tw' + root.find('div', class_='srh-footer__content-item srh-footer__content-nextpage').a.get('href')+'&ps=100'
    except:
        print('已下載' + str(len(jobInfoList)) + '筆資料...')
        return jobInfoList
    req.close()

    if url is not None:
        return download_data(url=url, job_number=job_number, jobInfoList=jobInfoList, search_job_num=search_job_num)




def clean_data(job_list, column_names):
    # ==========================================
    # STEP 1: CONVERT LIST TO PANDAS DATAFRAME
    # * Convert a list to df with columns
    # * Insert a new column with date
    # ==========================================
    df = pd.DataFrame(job_list, columns=column_names)
    today = str(datetime.datetime.today().date())
    df.insert(0, 'date', today)

    # =============================
    # STEP 2: FILL NULL WITH N/A
    # =============================
    print('初步處理NULL...')
    df = df.fillna('N/A')
    df.to_csv('test.csv', index=False)

    # ====================================================
    # STEP 3: RUN ALL COLS AND REMOVE SPACES FROM RIGHT
    # ====================================================
    print('移除右方空格...')
    for x in range(len(df.columns)):
        df.iloc[:, x] = df.iloc[:, x].str.rstrip()

    # ===================================
    # STEP 4: EDIT company_cat COLUMN
    # ===================================
    print('處理company_cat column...')
    # Replace all space with ','
    df['company_cat'] = df['company_cat'].str.replace(' ', ',')

    # ====================================================================================
    # STEP 5: EDIT salary COLUMN
    #
    # In this step, salary will be separated into three columns:
    #   * pay_type indicates how to pay including 'negotiable', 'monthly' and 'daily'
    #   * lower_limit indicates lower limit
    #   * upper_limit indicates hieher limit
    #
    # ====================================================================================
    print('處理薪資...')
    # =====Insert a new column called pay_type======
    df.insert(6, 'pay_type', None)

    # =====Add values of pay_type for salary contains '面議'=====
    negoType = df['salary'].str.contains('面議')
    df.loc[negoType, 'pay_type'] = 'negotiable'

    # =====Add values of pay_type for salary contains '月薪'=====
    monthType = df['salary'].str.contains('月薪')
    df.loc[monthType, 'pay_type'] = 'monthly'

    # =====Add values of pay_type for salary contains '日薪'=====
    dayType = df['salary'].str.contains('日薪')
    df.loc[dayType, 'pay_type'] = 'daily'

    # =====Add values of pay_type for salary contains '年薪'=====
    yearType = df['salary'].str.contains('年薪')
    df.loc[yearType, 'pay_type'] = 'year'

    # =====Add two columns for low salary and high salary to show salary limit=====
    df.insert(7, 'lower_limit', 0)
    df.insert(8, 'upper_limit', 0)

    # =====Add lower limit and upper limit values for 'negotiable' pay_type=====
    df.loc[negoType, 'lower_limit'] = 40000
    df.loc[negoType, 'upper_limit'] = 'N/A'

    # =====Add lower limit and upper limit values for 'monthly' pay_type=====
    # Remove all Chinese characters. Only numbers are left.
    temp_df = df.loc[monthType, 'salary'].str.replace('月薪', '').str.replace('萬', '').str.replace('元', '').str.replace(
        '以上', '')
    # Separate lower limit and upper limit
    pay_range = temp_df.str.split('~', n=1, expand=True)
    # Check if there is any result after searching
    if len(pay_range) != 0:
        # Fill all lower_limit values
        df.loc[monthType, 'lower_limit'] = pay_range[0]

        # Find all values that contains ',', remove them and divided by 10000
        df_with_string = df['lower_limit'].str.contains(',', na=False)
        df.loc[df_with_string, 'lower_limit'] = pd.to_numeric(df.loc[df_with_string, 'lower_limit'].str.replace(',', ''),
                                                              errors='coerce') / 10000
        # Convert all decimal values into integer
        df.loc[monthType, 'lower_limit'] = pd.to_numeric(df.loc[monthType, 'lower_limit'], errors='coerce') * 10000

        # Fill all upper_limit values
        df.loc[monthType, 'upper_limit'] = pay_range[1]
        # Find all values that contains ',', remove them and divided by 10000
        df_with_string = df['upper_limit'].str.contains(',', na=False)
        df.loc[df_with_string, 'upper_limit'] = pd.to_numeric(df.loc[df_with_string, 'upper_limit'].str.replace(',', ''),
                                                              errors='coerce') / 10000
        # Convert all decimal values into integer
        df.loc[monthType, 'upper_limit'] = pd.to_numeric(df.loc[monthType, 'upper_limit'], errors='coerce') * 10000


    # =====Add lower limit and upper limit values for 'daily' pay_type=====
    # Remove all Chinese characters. Only numbers are left
    temp_df = df.loc[dayType, 'salary'].str.replace('日薪', '').str.replace('萬', '').str.replace('元', '').str.replace(
        '以上', '')
    # Separate lower limit and upper limit
    pay_range = temp_df.str.split('~', n=1, expand=True)
    if len(pay_range) != 0:
        # Fill all lower_limit values
        df.loc[dayType, 'lower_limit'] = pay_range[0]
        # Find all values that contains ',', remove them and divided by 1000
        df_with_string = df['lower_limit'].str.contains(',', na=False)
        df.loc[df_with_string, 'lower_limit'] = pd.to_numeric(df.loc[df_with_string, 'lower_limit'].str.replace(',', ''),
                                                              errors='coerce') / 1000
        # Convert all decimal values into integer
        df.loc[dayType, 'lower_limit'] = pd.to_numeric(df.loc[dayType, 'lower_limit'], errors='coerce') * 1000

        # Fill all upper_limit values
        df.loc[dayType, 'upper_limit'] = pay_range[1]
        # Find all values that contains ',', remove them and divided by 10000
        df_with_string = df['upper_limit'].str.contains(',', na=False)
        df.loc[df_with_string, 'upper_limit'] = pd.to_numeric(df.loc[df_with_string, 'upper_limit'].str.replace(',', ''),
                                                              errors='coerce') / 10000
        # Convert all decimal values into integer
        df.loc[dayType, 'upper_limit'] = pd.to_numeric(df.loc[dayType, 'upper_limit'], errors='coerce') * 10000

    # =====Add lower limit and upper limit values for 'year' pay_type=====
    # Remove all Chinese characters. Only numbers are left
    temp_df = df.loc[yearType, 'salary'].str.replace('年薪', '').str.replace('萬', '').str.replace('元', '').str.replace(
        '以上', '')
    # Separate lower limit and upper limit
    pay_range = temp_df.str.split('~', n=1, expand=True)
    if len(pay_range) != 0:
        # Fill all lower_limit values
        df.loc[yearType, 'lower_limit'] = pay_range[0]
        # Fill all upper_limit values
        df.loc[yearType, 'upper_limit'] = pay_range[1]

    # Drop NULL (None)
    df = df.dropna(subset=['pay_type'])

    # ====================================================================================
    # STEP 6: EDIT city COLUMN
    #
    # In this step, city column will be renamed as location first
    # Then location will be separated into two columns:
    #   * city indicates the city
    #   * district indicates district
    #
    # ====================================================================================
    print('處理城市...')
    # Rename
    df = df.rename(columns={'city': 'location'})
    # Insert city and district columns
    df.insert(5, 'city', None)
    df.insert(6, 'district', None)
    # Fill values into city and district columns
    df['city'] = df['location'].str[0:3]
    df['district'] = df['location'].str[3:]

    # ====================================================================================
    # STEP 7: EDIT working_exp COLUMN
    #
    # In this step, working_experience will change:
    #   * 經驗不拘 to 0
    #   * Other experience will change to the minimum requirement
    #
    # ====================================================================================
    print('處理工作經驗...')
    df['working_exp'] = df['working_exp'].str.replace(r'經驗不拘|半|無工作經驗可', '0', regex=True)
    df['working_exp'] = df['working_exp'].str.replace(r'年工作經驗以(上|下)', '', regex=True)


    # =======================
    # STEP 8: FINAL CLEAN
    # =======================
    print('最後處理...')
    # Drop four columns
    df = df.drop(columns=['location', 'salary'])
    # Fill NULL
    df = df.fillna('N/A')
    # Return result
    return df





def upload_data(dataFrame):
    # Enter your credential csv file
    # This csv file should contains your database name, endpoint, username and password
    aws_credential = pd.read_csv('', index_col='')
    conn = pymysql.connect(
        host=aws_credential.loc['<db name>']['endpoint'],
        port=3306,
        user=aws_credential.loc['<db name>']['username'],
        password=aws_credential.loc['db name']['password'],
        charset='utf8',
        db='<db name>',
        local_infile=True
    )
    dataFrame.to_csv('temp.csv', index=False)
    with conn.cursor() as cursor:
        # MySQL inline insert data
        sql_load_inline = '''
        LOAD DATA LOCAL INFILE 'temp.csv'
        INTO TABLE jobbank
        FIELDS TERMINATED BY ','
        ENCLOSED BY '"'
        LINES TERMINATED BY '\n'
        IGNORE 1 ROWS;
        '''
        print('準備上傳...')
        cursor.execute(sql_load_inline)
        conn.commit()
    os.remove('temp.csv')
    print('上傳完畢...')






URL = 'https://www.1111.com.tw/search/job?ts=1&d0=&c0=&ps=100&page='
COLNAMES = ['job_title', 'company_name', 'company_cat', 'location',
            'address', 'salary', 'working_exp', 'education', 'degree',
            'driver_license', 'vehicles']

print('===== 下載資料 =====')
jobList = download_data(URL, 0, [])
print('\n ===== 資料清洗 =====')
jobDf = clean_data(job_list=jobList, column_names=COLNAMES)
print('\n ===== 資料上傳 =====')
upload_data(dataFrame=jobDf)




