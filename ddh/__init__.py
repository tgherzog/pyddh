import taxonomy
import dataset

ddh_host = ''

def load(host, session_key, session_value):
  global ddh_host

  ddh_host = host
  taxonomy.load(host)
  dataset.load(host, session_key, session_value)
