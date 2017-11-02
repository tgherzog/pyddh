import taxonomy
import dataset
import util

ddh_host = ''

def load(host, user=None, pswd=None):
  global ddh_host

  ddh_host = host
  taxonomy.load(host)
  dataset.load(host, user, pswd)
