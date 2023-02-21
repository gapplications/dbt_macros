import pickle5 as pickle
import networkx as nx
import json
import pandas as pd
import os
from google.cloud import bigquery
from google.cloud import storage
from google.cloud.storage import blob
import sys

client = storage.Client(project='project-name')
bucket = client.get_bucket('dbt-artifacts-storage-bucket')

#list of repositories
repositories = []

thismodule = sys.modules[__name__]

for repo in repositories:
    blob = bucket.get_blob(repo + '/prod/graph.gpickle')

    graph = blob.download_as_string()
    if repo == '1':
        setattr(thismodule, 'global_graph', pickle.loads(graph))
    else:
        setattr(thismodule, 'global_graph', nx.compose(global_graph, pickle.loads(graph)))

all_manifests = {}

for repo in repositories:
    blob = bucket.get_blob(repo + '/prod/manifest.json')

    manifest = blob.download_as_string()
    
    foo = repo.replace("-", "_")
    all_manifests[foo] = manifest
#     setattr(thismodule, repo + '_manifest', manifest)

attr_dict = {}

# Assign attributes to each node in the global graph, including the source_joining_key, used for creating missing edges
for node in global_graph.nodes():
    node_attr_dict = {}
    file_type = node.split(".")[0]
    source_repository = node.split(".")[1]
    if file_type == 'source':
        source_reference = node.split(".")[2]
        file_name = node.split(".")[3]
    else:
        file_name = node.split(".")[2]

    
    if file_type == 'source' and source_reference in repositories:
        source_joining_key = (source_reference + '_' + file_name)
    elif file_type == 'model':
        source_joining_key = (source_repository + '_' + file_name)
    else:
        source_joining_key = (source_repository + '_' + file_name + '_' + file_type + '_' + source_reference)


    for variable in ["file_type", "source_repository", "source_reference", "file_name", "source_joining_key"]:
        node_attr_dict[variable] = eval(variable)
    attr_dict[node] = node_attr_dict
nx.set_node_attributes(global_graph, attr_dict)

# Find all missing edges across repos and build new edge
for node_r, attributes_r in global_graph.nodes(data=True):
    for node, attributes in global_graph.nodes(data=True):
        if node != node_r and attributes['source_joining_key'] == attributes_r['source_joining_key'] and attributes_r['resource_type'] == 'source':
            global_graph.add_edge(node, node_r)
            print('added edge from ' + node + ' to ' + node_r)
            

end_of_line_df = pd.DataFrame([])

for node, attributes in global_graph.nodes(data=True):
    if len(list(global_graph.successors(node))) == 0 and attributes['resource_type'] == 'model':
        repo = attributes['source_repository']
        manifest_json = json.loads(all_manifests[repo])
        node_name = node
        print(node_name)
        database = manifest_json['nodes'][node]['database']
        schema = manifest_json['nodes'][node]['schema']
        alias = manifest_json['nodes'][node]['alias']
        df = pd.DataFrame({'node_name': node_name
                           , 'database': database
                           , 'schema': schema
                           , 'alias': alias}, index=[0])
        end_of_line_df = end_of_line_df.append(df, ignore_index=True)

full_tables_df = pd.DataFrame([])
for index, row in end_of_line_df.iterrows():
    print(row['alias'])
    # Get the current max history_id in the table
    table_usage_sql = f"""
    SELECT 
      COUNT(DISTINCT q.job_id) AS query_count
      , COUNT(DISTINCT q.user_email) AS user_count


    FROM `query_logs_table` q 
    WHERE created_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
      AND CONTAINS_SUBSTR(q.query, '""" + row['alias'] +  """')
      AND CONTAINS_SUBSTR(q.query, '""" + row['database'] +  """')
      AND CONTAINS_SUBSTR(q.query, '""" + row['schema'] +  """')
      AND NOT CONTAINS_SUBSTR(q.query, '"app": "dbt", ')

    """

    client = bigquery.Client()

    job_config = bigquery.QueryJobConfig(use_query_cache=False)

    # Start the query, passing in the extra configuration.
    query_job = client.query(table_usage_sql,
        job_config=job_config,
    )  # Make an API request.

    for r in query_job.result():
        query_count = r[0]
        user_count = r[1]

    end_of_line_df['query_count'] = query_count
    end_of_line_df['user_count'] = user_count
    
    table_df = pd.DataFrame({'database': row['database']
                                 , 'schema': row['schema']
                                 , 'alias': row['alias']
                                 , 'node_name': row['node_name']
                                 , 'query_count': query_count
                                 , 'user_count': user_count}, index=[0])
    full_tables_df = full_tables_df.append(table_df, ignore_index=True)

tables_to_delete = full_tables_df.loc[(full_tables_df['user_count'] == 0) & (full_tables_df['query_count'] == 0)].reset_index()

pd.set_option('display.max_rows', None)
pd.options.display.max_colwidth = 100
print(tables_to_delete)