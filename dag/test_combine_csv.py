import combine_csv
import csv
import os
import shutil
from os.path import exists
from pathlib import Path



def test_preprocessing():
    f_name = '../data/process_time_log.csv'
    combined_file = '../data/Combined.csv'
    na_file = '../data/NA Preview.csv'
    ap_file = '../data/Asia Prod 4.csv'

    if exists(f_name):
        os.remove(f_name)
    if exists(na_file):
        os.remove(na_file)
    if exists(combined_file):
        os.remove(combined_file)
    if exists(ap_file):
        os.remove(ap_file)

    assert combine_csv.preprocessing(path='../data', run_log_file='process_time_log.csv') == ['../data/process_time_log.csv',
                                                                                             '../data/Asia Prod 2.csv',
                                                                                             '../data/Asia Prod 3.csv',
                                                                                             '../data/Asia Prod 1.csv',
                                                                                             '../data/NA Prod.csv']
def test_files_to_process():
    f_name = '../data/process_time_log.csv'
    combined_file = '../data/Combined.csv'
    na_file = '../data/NA Preview.csv'
    ap_file = '../data/Asia Prod 4.csv'
    
    if exists(f_name):
        os.remove(f_name)
    if exists(na_file):
        os.remove(na_file)
    if exists(combined_file):
        os.remove(combined_file)
    if exists(ap_file):
        os.remove(ap_file)

    assert combine_csv.files_to_process(path='../data', last_run=0.0) == ['../data/Asia Prod 2.csv', 
                                                                          '../data/Asia Prod 3.csv', 
                                                                          '../data/Asia Prod 1.csv', 
                                                                          '../data/NA Prod.csv']
    assert combine_csv.files_to_process(path='', last_run=0.0) == None



def test_read_from_csv():
    assert combine_csv.read_from_csv(['../data/process_time_log.csv',
                                    '../data/Asia Prod 2.csv',
                                    '../data/Asia Prod 3.csv',
                                    '../data/Asia Prod 1.csv',
                                    '../data/NA Prod.csv',
                                    '../data/NA_Preview.csv']) == {'6.6.6.6': 'Asia Prod',
                                                                 '7.7.7.7': 'Asia Prod',
                                                                 '8.8.8.8': 'Asia Prod',
                                                                 '9.9.9.9': 'Asia Prod',
                                                                 '10.10.10.10': 'Asia Prod',
                                                                 '4.4.4.4': 'Asia Prod',
                                                                 '5.5.5.5': 'Asia Prod',
                                                                 '1.1.1.1': 'NA Prod',
                                                                 '2.2.2.2': 'NA Prod',
                                                                 '3.3.3.3': 'NA Prod'
                                                                 }

def test_sort_values():
    test_dict = {'6.6.6.6': 'Asia Prod',
                 '7.7.7.7': 'Asia Prod',
                 '8.8.8.8': 'Asia Prod',
                 '9.9.9.9': 'Asia Prod',
                 '10.10.10.10': 'Asia Prod',
                 '4.4.4.4': 'Asia Prod',
                 '5.5.5.5': 'Asia Prod',
                 '1.1.1.1': 'NA Prod',
                 '2.2.2.2': 'NA Prod',
                 '3.3.3.3': 'NA Prod'
                 }
    assert combine_csv.sort_values(test_dict) == [('1.1.1.1', 'NA Prod'),
                                                 ('2.2.2.2', 'NA Prod'),
                                                 ('3.3.3.3', 'NA Prod'),
                                                 ('4.4.4.4', 'Asia Prod'),
                                                 ('5.5.5.5', 'Asia Prod'),
                                                 ('6.6.6.6', 'Asia Prod'),
                                                 ('7.7.7.7', 'Asia Prod'),
                                                 ('8.8.8.8', 'Asia Prod'),
                                                 ('9.9.9.9', 'Asia Prod'),
                                                 ('10.10.10.10', 'Asia Prod')]

def test_write_to_csv(tmp_path: Path):
    output_path = tmp_path / "output.csv"
    test_data = [('1.1.1.1', 'NA Prod'),
                 ('2.2.2.2', 'NA Prod'),
                 ('3.3.3.3', 'NA Prod'),
                 ('4.4.4.4', 'Asia Prod'),
                 ('5.5.5.5', 'Asia Prod'),
                 ('6.6.6.6', 'Asia Prod'),
                 ('7.7.7.7', 'Asia Prod'),
                 ('8.8.8.8', 'Asia Prod'),
                 ('9.9.9.9', 'Asia Prod'),
                 ('10.10.10.10', 'Asia Prod')]

    expected_output = [{'Source Ip': '1.1.1.1', 'Environment': 'NA Prod'},
                     {'Source Ip': '2.2.2.2', 'Environment': 'NA Prod'},
                     {'Source Ip': '3.3.3.3', 'Environment': 'NA Prod'},
                     {'Source Ip': '4.4.4.4', 'Environment': 'Asia Prod'},
                     {'Source Ip': '5.5.5.5', 'Environment': 'Asia Prod'},
                     {'Source Ip': '6.6.6.6', 'Environment': 'Asia Prod'},
                     {'Source Ip': '7.7.7.7', 'Environment': 'Asia Prod'},
                     {'Source Ip': '8.8.8.8', 'Environment': 'Asia Prod'},
                     {'Source Ip': '9.9.9.9', 'Environment': 'Asia Prod'},
                     {'Source Ip': '10.10.10.10', 'Environment': 'Asia Prod'}]

    combine_csv.write_to_csv(test_data, output_path)

    with open(output_path, "r") as f:
        reader = csv.DictReader(f)
        result = [dict(row) for row in reader]

    assert result == expected_output



def test_na_preview(tmp_path: Path):
    output_path = tmp_path / "Combined.csv"
    na_file = '../data/NA Preview.csv'
    if exists(na_file):
        os.remove(na_file)

    test_data = [('1.1.1.1', 'NA Prod'),
                 ('2.2.2.2', 'NA Prod'),
                 ('3.3.3.3', 'NA Prod'),
                 ('4.4.4.4', 'Asia Prod'),
                 ('5.5.5.5', 'Asia Prod'),
                 ('6.6.6.6', 'Asia Prod'),
                 ('7.7.7.7', 'Asia Prod'),
                 ('8.8.8.8', 'Asia Prod'),
                 ('9.9.9.9', 'Asia Prod'),
                 ('10.10.10.10', 'Asia Prod')]

    combine_csv.write_to_csv(test_data, output_path)

    with open(na_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Source IP', 'Count', 'Events per Second'])
        writer.writerow(['11.11.11.11', '2', '0'])


    data_path = '../data'
    csv_files = combine_csv.preprocessing(path=data_path, run_log_file='process_time_log.csv')
    combined_dict = combine_csv.read_from_csv(csv_files)
    value_pairs = combine_csv.sort_values(c_dict=combined_dict)
    combine_csv.write_to_csv(value_pairs, output_path)


    expected_output = [{'Source Ip': '1.1.1.1', 'Environment': 'NA Prod'},
                     {'Source Ip': '2.2.2.2', 'Environment': 'NA Prod'},
                     {'Source Ip': '3.3.3.3', 'Environment': 'NA Prod'},
                     {'Source Ip': '4.4.4.4', 'Environment': 'Asia Prod'},
                     {'Source Ip': '5.5.5.5', 'Environment': 'Asia Prod'},
                     {'Source Ip': '6.6.6.6', 'Environment': 'Asia Prod'},
                     {'Source Ip': '7.7.7.7', 'Environment': 'Asia Prod'},
                     {'Source Ip': '8.8.8.8', 'Environment': 'Asia Prod'},
                     {'Source Ip': '9.9.9.9', 'Environment': 'Asia Prod'},
                     {'Source Ip': '10.10.10.10', 'Environment': 'Asia Prod'},
                     {'Source Ip': '11.11.11.11', 'Environment': 'NA Preview'}]

    with open(output_path, "r") as f:
        reader = csv.DictReader(f)
        result = [dict(row) for row in reader]

    assert result == expected_output



def test_asia_prod(tmp_path: Path):
    output_path = tmp_path / "Combined.csv"
    ap_file = '../data/Asia Prod 4.csv'
    if exists(ap_file):
        os.remove(ap_file)

    test_data = [('1.1.1.1', 'NA Prod'),
                 ('2.2.2.2', 'NA Prod'),
                 ('3.3.3.3', 'NA Prod'),
                 ('4.4.4.4', 'Asia Prod'),
                 ('5.5.5.5', 'Asia Prod'),
                 ('6.6.6.6', 'Asia Prod'),
                 ('7.7.7.7', 'Asia Prod'),
                 ('8.8.8.8', 'Asia Prod'),
                 ('9.9.9.9', 'Asia Prod'),
                 ('10.10.10.10', 'Asia Prod')]

    combine_csv.write_to_csv(test_data, output_path)

    curpath = os.path.abspath(os.curdir)
    print('Current dir is: {}'.format(curpath))

    with open(ap_file, 'w') as f:
        writer = csv.writer(f)
        writer.writerow(['Source IP', 'Count', 'Events per Second'])
        writer.writerow(['11.11.11.11', '2', '0'])

    data_path = '../data'
    csv_files = combine_csv.preprocessing(path=data_path, run_log_file='process_time_log.csv')
    combined_dict = combine_csv.read_from_csv(csv_files)
    value_pairs = combine_csv.sort_values(c_dict=combined_dict)
    combine_csv.write_to_csv(value_pairs, output_path)


    expected_output = [{'Source Ip': '1.1.1.1', 'Environment': 'NA Prod'},
                     {'Source Ip': '2.2.2.2', 'Environment': 'NA Prod'},
                     {'Source Ip': '3.3.3.3', 'Environment': 'NA Prod'},
                     {'Source Ip': '4.4.4.4', 'Environment': 'Asia Prod'},
                     {'Source Ip': '5.5.5.5', 'Environment': 'Asia Prod'},
                     {'Source Ip': '6.6.6.6', 'Environment': 'Asia Prod'},
                     {'Source Ip': '7.7.7.7', 'Environment': 'Asia Prod'},
                     {'Source Ip': '8.8.8.8', 'Environment': 'Asia Prod'},
                     {'Source Ip': '9.9.9.9', 'Environment': 'Asia Prod'},
                     {'Source Ip': '10.10.10.10', 'Environment': 'Asia Prod'},
                     {'Source Ip': '11.11.11.11', 'Environment': 'Asia Prod'}]

    with open(output_path, "r") as f:
        reader = csv.DictReader(f)
        result = [dict(row) for row in reader]

    assert result == expected_output


