# Supporting functions

import subprocess
import re
import math  

def writeTableLab(df, lab_name, table_name):
  
  '''
  df: your DataFrame
  lab_name: name of the data lab where you want to store the table. The function works only with DataLabs
  table_name: name you want to give to the table
  '''

  
  disc_file_path = lab_name[4:]
  path = "/data/lab/"  + disc_file_path + "/db/" + table_name
  
  df.write.format("parquet").mode("overwrite").option("path", path)\
    .saveAsTable(lab_name + "." + table_name)

    # Compute the optimal partition size
  cmd = "hdfs dfs -du -s " + path
  process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  output = process.communicate()
  data_sizes = re.findall(r"[0-9]+", str(output[0]))
  data_size = int(data_sizes[0]) / (1024 ** 3)  # folder size in GB
  optimal_num_partition = math.ceil(data_size / 1.1)
  

    # Save the data optimally repartitioned in the final location
  df.repartition(optimal_num_partition).write.format("parquet").mode("overwrite")\
    .option("path", path) \
    .saveAsTable(lab_name + "." + table_name)

  # Invlidate the metadata
  cmd = "impala-shell -i $IMPALA_HOST:21000 -k --ssl -q 'INVALIDATE METADATA {0}.{1}'".format(
      lab_name, table_name)
  process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  output = process.communicate()
  
  success_message = "Table saved in " + lab_name + "." + table_name

  return success_message
  

  
def deleteTable(spark_session, lab_name, table_name):
  
  '''
  spark_session: your spark session
  lab_name: name of the data lab where the table you want to delete is stored
  table_name: name of the table
  '''
    
  spark_session.sql("DROP TABLE IF EXISTS " + lab_name + "." + table_name)
  disc_file_path = lab_name[4:]
  cmd = "hdfs dfs -rm -r -skipTrash " + '/data/lab/' + disc_file_path + '/db/' + table_name
  process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  output = process.communicate()
  cmd = "impala-shell -i $IMPALA_HOST:21000 -k --ssl -q 'INVALIDATE METADATA {0}.{1}'".format(lab_name, table_name)
  process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
  output = process.communicate()
  
  return table_name + ' deleted'
  
