from google.cloud import bigquery
import os
def avgtraffic_threshold_envvar(request):
  client = bigquery.Client()
  env_avg_traffic=os.environ.get('avg_traffic')
  query= """
  SELECT location,average_traffic,latest_measurement
  FROM `pg-us-e-app-588206.sample_data.table2`
  WHERE average_traffic>=@average_traffic
  """
  job_config = bigquery.QueryJobConfig(
  query_parameters=[
  bigquery.ScalarQueryParameter("average_traffic", "INT64", env_avg_traffic)
  ]
  )
  table_ref=client.dataset("sample_data").table("table4")
  job_config.destination=table_ref
  job_config.create_disposition = 'CREATE_IF_NEEDED'
  job_config.write_disposition = 'WRITE_TRUNCATE'
  print("env_avg_traffic: {}".format(env_avg_traffic))
  query_job = client.query(query, job_config=job_config)
  print("The Substituted Query: {}".format(query))
  #print("The Query response is: {}".format(query_job))
  results=query_job.result()
  if results.total_rows == 0:
    print ("There are no records with average Traffic > threshold.")
  else:
    for row in query_job:
      print(row)
    destination_uri="gs://poc-cloudfunctions/AvgTraffic_threshold.csv"
    dataset_ref=client.dataset("sample_data", project="pg-us-e-app-588206")
    table_ref=dataset_ref.table("table4")
    extract_job=client.extract_table(
    table_ref,
    destination_uri)
    extract_job.result()
  return f'The Query ran successfully!'
