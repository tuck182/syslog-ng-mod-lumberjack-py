from pprint import pprint

def flatten(d, parent_key='', sep='.'):
  items = []
  for k, v in d.items():
    new_key = parent_key + sep + k if parent_key else k
      
    try:
      items.extend(flatten(v, new_key, sep=sep).items())
    except AttributeError:
      items.append((new_key, v))
  return dict(items)

def strlen(str):
  if isinstance(str, unicode):
      return len(str)
  return len(str)
