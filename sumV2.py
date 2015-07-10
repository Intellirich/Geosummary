__author__ = 'riccardo'


import dbf
from collections import defaultdict
import csv
import os
import shutil


def read_table(in_table):
    table = dbf.Table(in_table).open()
    fields = table.field_names
    return table, fields


def createKeyValueList(in_table_path, in_field, in_stat_field):
    in_table = os.path.splitext(os.path.basename(in_table_path))[0]
    table, fields = read_table(in_table)
    keyValueList = []
    for row in table:
        t = []
        t.append(row[in_field])
        t.append(row[in_stat_field])
        keyValueList.append(t)
    return keyValueList


def sum_stat(in_table, in_field, in_stat_field):
    dictList = createKeyValueList(in_table, in_field, in_stat_field)
    sum_area = defaultdict(float)
    for k, v in dictList:
        sum_area[k] += v
    return sum_area
    #print sum_area


def mean_stat(in_table, in_field, in_stat_field):
    dictList = createKeyValueList(in_table, in_field, in_stat_field)
    sum_area = sum_stat(in_table, in_field, in_stat_field)
    counting = defaultdict(int)
    mean = defaultdict(float)
    for k, v in dictList:
        counting[k] += 1
        area = sum_area[k]
        count = counting[k]
        mean[k] = area/count
    #print mean
    return mean


def min_stat(in_table, in_field, in_stat_field):
    dictList = createKeyValueList(in_table, in_field, in_stat_field)
    minDict = defaultdict(float)
    genDict = defaultdict(list)
    for k, v in dictList:
        genDict[k].append(v)
    for k, v in genDict.iteritems():
        minDict[k] = min(v)
    #print minDict
    return minDict


def max_stat(in_table, in_field, in_stat_field):
    dictList = createKeyValueList(in_table, in_field, in_stat_field)
    maxDict = defaultdict(float)
    genDict = defaultdict(list)
    for k, v in dictList:
        genDict[k].append(v)
    for k, v in genDict.iteritems():
        maxDict[k] = max(v)
    #print maxDict
    return maxDict


def choose_stat(func_string):
    func_list = func_string.split()
    for func in func_list:
        function_dict = {'sum': sum_stat, 'mean': mean_stat, 'min': min_stat, 'max': max_stat}
        #print function_dict, function_dict[func](table, sum_field, stat_field)
        return function_dict, function_dict[func](table, sum_field, stat_field)


def choose_output(out_format):
    format_dict = {'csv': write_csv, 'dbf': write_dbf_table}
    return format_dict[out_format]



def clean_dict(stat_string):
    stat_list = stat_string.split()
    for stat in stat_list:
        function_dict, func = choose_stat(stat)
        stat_dict = function_dict[stat](table, sum_field, stat_field)
        for k in stat_dict.keys():
            new_k = k.rstrip()
            stat_dict[new_k] = stat_dict.pop(k)
        #print stat_dict
        return stat_dict


def write_csv(fnx_string, field, table, out_path):
    fnx_list = fnx_string.split()
    for fnx in fnx_list:
        dct = clean_dict(fnx)
        tbl, fields = read_table(table)
        i_field = fields.index(field)
        complete_path = os.path.join(out_path, 'summary.csv')
        with open(complete_path, 'rb') as in_csv, open(complete_path, 'wb') as out_csv:
            reader = csv.reader(in_csv)
            writer = csv.writer(out_csv)
            for fnx in fnx_list:
                writer.writerow([fields[i_field], fnx])
                for row in reader:
                    for key, value in dct.items():
                        writer.writerow([key, value])
                    writer.writerow(row + value)
    return out_csv


def write_dbf_table(fnx, field, table, out_path):
    dct = clean_dict(fnx)
    fields_spec = '{0} C(20); {1} N(19,5)'.format(field, fnx)
    new_table = dbf.Table(fnx, field_specs=fields_spec).open()
    l = ()
    for k, v in dct.items():
        l2 = ()
        l2 += (k, )
        l2 += (v, )
        l += (l2, )
        new_table.append(l2)
    new_table.close()
    curr_path = os.path.join(os.getcwd(), fnx + '.dbf')
    new_path = os.path.join(out_path, fnx + '.dbf')
    shutil.move(curr_path, new_path)






if __name__ == "__main__":
    # table = raw_input("Write the path to your dbf table: ")
    # sum_field = raw_input("Select the field to summarize: ")
    # stat_field = raw_input("Select the field on which make statistics: ")
    table = "07_confini"
    sum_field = "descrizion"
    stat_field = "area"
    save_path = "/Users/riccardo/Documents/progetti/summarize"
    #save_path = raw_input("where do you want to save your file: ")
    createKeyValueList(table, sum_field, stat_field)
    funx = raw_input('Which statistics do you want to compute? ')
    choose_stat(funx)
    clean_dict(funx)
    # write_csv(funx, sum_field, table, save_path)
    # write_dbf_table(funx, sum_field, save_path)
    output_format = raw_input("Which kind of table do you want?: ")
    fmt = choose_output(output_format)
    fmt(funx, sum_field, table, save_path)
