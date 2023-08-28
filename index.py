import yaml
import sys
import hashlib
import json
import click

from prettytable import PrettyTable
from loguru import logger

class Compressor:
  
  def __init__(self, min_count, min_size) -> None:
    self.min_count = min_count
    self.min_size = min_size
    self.hash_to_path = {}
    self.hash_to_size = {}
    self.hash_to_content = {}
    
  def get_path(self, parent, key, type = 'dict'):
    if type == 'dict':
      p = f"{parent}.{key}"
    else:
     p = f"{parent}.[{key}]"
     
    if p[0] == '.':
      return p[1:]

    return p

  def process_item(self, path, value):
    #logger.debug("Path {path} is {type}", path=path, type=type(value))
    
    if (str(value).startswith('__COMPRESSED__/')):
      return
      
    self.process_items(path, value)

    value_serialized = json.dumps(value, sort_keys=True).encode('utf-8')

    try:
      vash = hashlib.sha256(value_serialized).hexdigest()
    except TypeError:
      logger.info("Cannot get hash at path {path}", path=path)
      return

    if vash not in self.hash_to_path:
      self.hash_to_path[vash] = [];
      self.hash_to_content[vash] = value
      self.hash_to_size[vash] = sys.getsizeof(value_serialized)

    self.hash_to_path[vash].append(path)

  def process_items(self, parent_key, obj):
    if (type(obj) is list):
      key = 0
      for value in obj:
        k_path = self.get_path(parent_key, key, 'list')
        self.process_item(k_path, value)
        key += 1
    elif (type(obj) is dict):
      for key, value in obj.items():
        k_path = self.get_path(parent_key, key, 'dict')
        self.process_item(k_path, value)
    
  def find_candidates(self, count=10):
    """Return content-hash top X candidates

    Args:
        count (int): Candidates count to return
    Returns:
        str: The content-hash list
    """
    def list_candidates_sort(hash):
      return self.hash_to_size[hash] * len(self.hash_to_path[hash])
    
    def list_candidates_filter(hash):
      return self.hash_to_size[hash] >= self.min_size and len(self.hash_to_path[hash]) >= self.min_count

    l = list(filter(list_candidates_filter, self.hash_to_size.keys()))
    
    l.sort(key=list_candidates_sort, reverse=True)
    
    return l[:count]
  
  def find_candidate(self):
    """Return content-hash top candidate

    Returns:
        str: The content-hash
    """
    c = self.find_candidates(1)
    return c[0]
  
  def print_results(self, count = 1):
    """print a pretty table

    Args:
        count (int, optional): Candidates count to display. Defaults to 10.
    """
    c = self.find_candidates(count=count)
    x = PrettyTable()
    x.field_names = ["hash", "occurences", "size", "paths", "content"]
    x.align = "l"
    
    for l in c:
      content = json.dumps(self.hash_to_content[l], sort_keys=True)
      x.add_row([
        l,
        len(self.hash_to_path[l]),
        self.hash_to_size[l],
        "\n".join(self.hash_to_path[l][:10]),
        content[:128]
      ], divider=True)
      
    print(x)

class Replacer:
  
  def __init__(self, data) -> None:
    self.data = data
    
  def replace_path(self, path, content):
    """Replace data at path with content

    Args:
        path (str): The path with dot notation
        content (any): The content
    """
    cursor_data = self.data
    path_components = path.split(".")
    
    for path_c in path_components[:-1]:
      if type(cursor_data) is dict:
        cursor_data = cursor_data[path_c]
      elif type(cursor_data) is list:
        cursor_data = cursor_data[int(path_c[1:-1])]
        
    if type(cursor_data) is dict:
      cursor_data[path_components[-1]] = content
    elif type(cursor_data) is list:
      cursor_data[int(path_components[-1][1:-1])] = content


@click.command()
@click.option("--file", prompt="yaml file path", help="")
@click.option("--candidates", help="Just print candidates", default=False, is_flag=True)
@click.option("--min-count", help="Occurences count filter", default=2)
@click.option("--min-size", help="Content size filter", default=10)
def main(file, candidates, min_count, min_size):
  with open(file, 'r') as file:
    origin_yaml = yaml.safe_load(file)
    
    anchors = {}
    
    for i in range(0, 1000):
      compressor = Compressor(min_count, min_size)

      compressor.process_items('', origin_yaml)
      
      l_candidates = compressor.find_candidates()
      
      if len(l_candidates) == 0:
        break

      if candidates:
        compressor.print_results()
       
      c_hash = l_candidates[0]
      c_paths = compressor.hash_to_path[c_hash]
      
      replacer = Replacer(origin_yaml)
      
      for c_path in c_paths[1:]:
        anchors[c_path] = compressor.hash_to_content[c_hash]
        replacer.replace_path(c_path, f"__COMPRESSED__/{c_hash}" )
    
    replacer = Replacer(origin_yaml)
    
    for anchor_path, anchor_content in anchors.items():
      logger.debug("Create anchor at {path}", path=anchor_path)
      replacer.replace_path(anchor_path, anchor_content)

    if not candidates:
      print(yaml.dump(origin_yaml, default_flow_style=False))
  
if __name__ == '__main__':
  main()