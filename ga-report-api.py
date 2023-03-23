from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build
import pandas as pd

#Criando credencial
credentials = ServiceAccountCredentials.from_json_keyfile_name(r<local_do_arquiv>, ['https://www.googleapis.com/auth/analytics.readonly']) 


#Criando parametros
VIEW_ID = <id_da_conta>
data_inicio = '30daysAgo'
data_fim = 'today'


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
            {'name': 'ga:landingPagePath'}
        ], 
        'metrics': [
            {'expression': 'ga:sessions'},
            {'expression': 'ga:transactions'},
            {'expression': 'ga:transactionRevenue'},
        ],
        "filtersExpression":"ga:sourceMedium==(direct) / (none)"
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
    

#Realizando consulta
analytics = initialize_analyticsreporting()
response_visao_geral = get_report_visao_geral(analytics)
df = response(response_visao_geral)
