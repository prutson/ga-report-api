from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import pandas as pd

#Criando credencial
credentials = ServiceAccountCredentials.from_json_keyfile_name(r<local_do_arquiv>, ['https://www.googleapis.com/auth/analytics.readonly']) 


def initialize_analyticsreporting():
    analytics = build('analyticsreporting', 'v4', credentials=credentials)
    return analytics
    
#Executando a consulta
def get_report_visao_geral(analytics):
    return analytics.reports().batchGet(body={'reportRequests': [{
        'viewId': VIEW_ID,
        'dateRanges': [{'startDate': data_inicio, 'endDate': data_fim}],
        'dimensions': [
            {'name': 'ga:date'},
            {'name': 'ga:pagePath'}
        ], 
        'metrics': [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:transactions'}
        ],
        "filtersExpression":"ga:sourceMedium==(direct) / (none)",
        'pageSize': pageSize,
        'samplingLevel':'Large',
        'pageToken': pageToken
    }]}).execute()


#Organizando consulta para dataframe
def response(response):
    report = response.get('reports', [])[0] # expected just one report
    # headers
    header_dimensions = report.get('columnHeader', {}).get('dimensions', [])
    header_metrics = [value['name'] for value in report.get('columnHeader', {}).get('metricHeader', {}).get('metricHeaderEntries', [])]
    headers = header_dimensions + header_metrics
    headers = list(map((lambda x: x.split(':', 1)[-1]), headers)) # removes "ga:" from each column
    # values
    values = []
    rows = report.get('data', {}).get('rows', [])
    for row in rows:
        values_dimensions = row.get('dimensions', [])
        values_metrics = row.get('metrics', [])[0].get('values', [])
        values.append(values_dimensions + values_metrics)
    # to dataframe
    df = pd.DataFrame(columns=headers, data=values)
    return df

#Pegando o token da consulta
def get_PT(response):
    for report in response.get('reports', []):
        columnHeader = report.get('columnHeader', {})
        dimensionHeaders = columnHeader.get('dimensions', [])
        metricHeaders = columnHeader.get('metricHeader',
                {}).get('metricHeaderEntries', [])
        pageToken = report.get('nextPageToken', None)
    return pageToken


#Criando parametros
VIEW_ID = <id_da_conta>
data_inicio = '30daysAgo'
data_fim = 'today'
pageSize = 1000
pageToken = 'unknown'

#Realizando consulta
def run_data(pageToken = 'unknown'):
    analytics = initialize_analyticsreporting()
    response_visao_geral = get_report_visao_geral(analytics)
    df = response(response_visao_geral)
    df['date'] = df['date'].astype('datetime64')
    df['sessions'] = df['sessions'].astype('int64')
    df['transactions'] = df['transactions'].astype('int64')
    pageToken = get_PT(response_visao_geral)
    return df, pageToken


df = run_data()[0]

#looping para pegar demais paginas
while run_data()[1]:
    pageToken = run_data()[1]
    df2 = run_data(pageToken = pageToken)[0]
    df = pd.concat([df, df2]).reset_index(drop=True)
    
    
    
