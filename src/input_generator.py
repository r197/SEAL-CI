import argparse
import CPL
import csv
import os
import shutil
import json

fileExt = ".json"
originator = 'test'
output_path = '../input'
graph_path = output_path + '/graphs/'
macro_edge_file = output_path + '/synthetic_edges.csv'

db_connection = CPL.cpl_connection()
bundle_index = 0
object_index = 0
obj_to_bundle = {}
bundle_connections = set()


def create_macro_edges_file():
    with open(macro_edge_file, mode='w') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['graph_1', 'graph_2'])
        map(writer.writerow, bundle_connections)


def create_data_file(bundle, tag):
    global object_index
    global bundle_index

    new_obj_indices = {}
    object_index = 0
    data = {}
    data['label'] = tag
    data['features'] = {}

    def process_bundle_connection(b):
        if b < bundle_index:
            a = (b, bundle_index)
        else:
            a = (bundle_index, b)
        bundle_connections.add(a)

    def process_object(x):
        global object_index
        obj_type = x.string_properties(originator, 'type')[0][2]

        if x.id not in obj_to_bundle:
            obj_to_bundle[x.id] = {bundle_index}
        else:
            map(process_bundle_connection, obj_to_bundle[x.id])
            obj_to_bundle[x.id].add(bundle_index)

        if x.id not in new_obj_indices:
            new_obj_indices[x.id] = object_index
            object_index += 1
        new_index = new_obj_indices[x.id]
        data['features'][str(new_index)] = [obj_type]

    def process_edge(x):
        src = x.base.id
        dest = x.other.id
        if (src not in new_obj_indices) or (dest not in new_obj_indices):
            raise Exception('Error while processing, object id not in new index map')
        return [new_obj_indices[src], new_obj_indices[dest]]

    objects = db_connection.get_bundle_objects(bundle)
    relations = db_connection.get_bundle_relations(bundle)

    map(process_object, objects)
    data['edges'] = map(process_edge, relations)

    with open(graph_path + str(bundle_index) + fileExt, 'w') as f:
        json.dump(data, f, indent=4, sort_keys=True, ensure_ascii=False)
    bundle_index += 1


def read_bundle_csv(bundle_file):
    def process_csv(x):
        return {'obj': db_connection.lookup_object(originator, x[0], CPL.BUNDLE), 'tag': x[1]}

    with open(bundle_file, 'r') as csvFile:
        reader = csv.reader(csvFile)
        bundles = map(process_csv, reader)
    csvFile.close()
    return bundles


def main():
    parser = argparse.ArgumentParser(description='generating input for SEAL-CI')
    parser.add_argument('-b', '--bundles', dest='bundleFile', help='csv file containing bundle Ids')
    args = parser.parse_args()

    if not args.bundleFile:
        print('Must specify a csv of bundle Ids and tags')
        return
    else:
        bundles = read_bundle_csv(args.bundleFile)

    if not os.path.exists(output_path):
        os.makedirs(graph_path)
    else:
        shutil.rmtree(output_path)
        os.makedirs(graph_path)

    map(lambda b: create_data_file(b['obj'], int(b['tag'])), bundles)
    create_macro_edges_file()

main()
db_connection.close()

