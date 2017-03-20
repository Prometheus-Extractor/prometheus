"""
This script takes a google evaluation dataset and adds wikidata id to it.
"""

import json
import requests
import sys


def freebase2wikidata(mid):
    """
    Takes a freebad mid and queries Wikidata for its QID.
    Returns False on error.
    """
    url = "https://query.wikidata.org/sparql"
    query = """
      SELECT DISTINCT ?sub
      WHERE
      {
        ?sub wdt:P646 "%s";
      }
    """ % mid
    data = {
      "query": query,
      "format": "json"
    }

    try:
        r = requests.post(url, data=data).json()
        wduri = r['results']['bindings'][0]['sub']['value']
        wdid = wduri.split('/')[-1]
        return wdid
    except Exception:
        return False

def convertItem(item, relations_id):
    item['wd_sub'] = freebase2wikidata(item['sub'])
    item['wd_obj'] = freebase2wikidata(item['obj'])
    item['wd_pred'] = relations_id
    return item

if __name__ == '__main__':

    if len(sys.argv) != 4:
        print("convert.py infile outfile relation_id")
        exit()

    relation_id = sys.argv[3]
    infile = open(sys.argv[1], 'r')
    outfile = open(sys.argv[2], 'w')

    lines = infile.readlines()
    infile.close()

    print("Converting %d items" % len(lines))
    for i in range(len(lines)):
        try:
            item = json.loads(lines[i])
        except Exception:
            print('Error when reading: %s' % lines[i])
            continue

        wd_item = convertItem(item, relation_id)
        if wd_item is not False:
            outfile.write(json.dumps(wd_item) + '\n')
        else:
            print('Error when writing: %s' % item)
        if (i + 1) % 50 == 0:
            print(i + 1)

    outfile.close()
