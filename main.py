import base64
import cloud_storage_utils
import csv
import hashlib
import json
import lxml
import os
import sys
import tempfile
from datetime import datetime

sys.path.append('./gexf')
from _gexf import Gexf, GexfImport

"""
Function for creating gexf graph file

Example:
  Run with params:
  {
    "creator": "Name of creator",
    "name": "Name of the graph"
  }
  Also supports param "version" - name of the result file version, example:
  {
    "version": "foo"
  }
  will write into resilt file named: parsed-output-foo.csv
"""

class MainClient:
  DESTINATION_TEMPLATE = 'parsed-output-{}.gexf'

  def __init__(self, input_file, version, cloud_storage_client, creator, name):
    self.version = version
    self.input_file = input_file
    self.cloud_storage_client = cloud_storage_client
    self.gexf = Gexf(creator, name)
    self.graph = self.gexf.addGraph("directed", "static", name)

  def md5(self, string):
    return hashlib.md5(string.encode('utf-8')).hexdigest()

  def start(self):
    DESTINATION_NAME = self.DESTINATION_TEMPLATE.format(self.version)
    input_content = self.cloud_storage_client.download_string(self.input_file)
    rows = [x for x in csv.DictReader([x for x in input_content.splitlines() if len(x) > 0])]
    used_nodes = {}
    used_edges = {}
    last_node_id = 1
    last_edge_id = 1
    for line in rows:
      source_url = line['SourceURL']
      target_url = line['TargetURL']
      if not used_nodes.get(source_url):
        used_nodes[source_url] = last_node_id
        self.graph.addNode(str(last_node_id), source_url)
        last_node_id += 1

      if not used_nodes.get(target_url):
        used_nodes[target_url] = last_node_id
        self.graph.addNode(str(last_node_id), target_url)
        last_node_id += 1

      edge_md5 = self.md5('{}{}'.format(source_url, target_url))
      if not used_edges.get(edge_md5):
        used_edges[edge_md5] = last_edge_id
        self.graph.addEdge(str(last_edge_id), str(used_nodes[source_url]), str(used_nodes[target_url]))
        last_edge_id += 1
    log(self.gexf.getXML())
    result_str = lxml.etree.tostring(self.gexf.getXML(), pretty_print=True,
                  encoding='utf-8', xml_declaration=True).decode('utf-8')

    self.cloud_storage_client.upload_string(result_str, DESTINATION_NAME)

def log(message):
  sys.stdout.write(message)

def main(event):
  original_payload = event.get_json()

  creator = original_payload.get('creator') if original_payload.get(
      'creator') is not None else 'Gexf Google cloud'
  name = original_payload.get('name') if original_payload.get(
      'name') is not None else 'Url graph file'

  version = original_payload.get('version')

  if not version:
    version = original_payload['version'] = datetime.now(
    ).strftime('%Y%m%d%H%M%S')

  BUCKET_NAME = os.environ['BUCKET_NAME']
  KEYWORDS_FILE_PATH = os.environ['KEYWORDS_FILE_PATH']

  cloud_storage_client = cloud_storage_utils.CloudStorageService(BUCKET_NAME)

  main_client = MainClient(input_file=KEYWORDS_FILE_PATH,
                          cloud_storage_client=cloud_storage_client,
                          version=version,
                          creator=creator,
                          name=name)
  main_client.start()
