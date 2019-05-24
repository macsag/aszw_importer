import xlrd
import requests
import logging
import sys
from collections import namedtuple
from aszw_configuration import file_in, file_out

RcdData = namedtuple('RcdData',
                     ['rcd_id', 'rcd_author', 'rcd_title', 'rcd_publisher', 'rcd_pub_year', 'rcd_number_of_pages'])


def open_xlsx_file(xlsx_file):
    return xlrd.open_workbook(xlsx_file)


def get_list_of_row_values(book):
    list_of_row_values = []

    for sheet_name in book.sheet_names():
        sheet = book.sheet_by_name(sheet_name)

        for nrow in range(1, sheet.nrows):  # offset - exclude column labels
            row_values = sheet.row_values(nrow)
            list_of_row_values.append(row_values)

    return list_of_row_values


def prepare_query_for_data_bn(row_values):
    # float to int and int to str casting is made to sanitise the publication year value
    query = f'http://data.bn.org.pl/api/bibs.json?title={row_values[2]}&publicationYear={str(int(row_values[3]))}' \
        f'&author={row_values[1]}'
    return query


def get_json_from_data_bn(query):
    r = requests.get(query)
    json_chunk = r.json()
    logging.debug(f'Succesfully downloaded: {query}')
    return json_chunk


def get_data_from_json_chunk(json_chunk):
    rcds_data = []

    for rcd in json_chunk['bibs']:
        rcd_id = rcd['id']
        rcd_author = rcd['author']
        rcd_title = rcd['title']
        rcd_publisher = rcd['publisher']
        rcd_pub_year = rcd['publicationYear']
        rcd_number_of_pages = get_number_of_pages(rcd)

        rcds_data.append(RcdData(rcd_id, rcd_author, rcd_title, rcd_publisher, rcd_pub_year, rcd_number_of_pages))

    return rcds_data


def disambiguate_and_check_bib_records(rcd_data, row_values):
    if row_values[4]:
        number_of_pages_from_xlsx = int(row_values[4])
        for rcd in rcd_data:
            if str(number_of_pages_from_xlsx) in rcd.rcd_number_of_pages:
                return [rcd.rcd_id]
            if str(number_of_pages_from_xlsx - 1) in rcd.rcd_number_of_pages:
                return [rcd.rcd_id]
            if str(number_of_pages_from_xlsx + 1) in rcd.rcd_number_of_pages:
                return [rcd.rcd_id]
        else:
            return []
    else:
        return []


def get_number_of_pages(rcd):
    for field in rcd['marc']['fields']:
        if '300' in field:
            return field['300']['subfields'][0]['a']


def get_marc_data_from_data_bn(records_ids):
    records_ids_length = len(records_ids)

    if records_ids_length <= 100:
        ids_for_query = '%2C'.join(str(record_id) for record_id in records_ids)
        query = f'http://data.bn.org.pl/api/bibs.marc?id={ids_for_query}&limit=100'

        result = bytearray(requests.get(query).content)
        logging.debug("Downloaded in iso_format: {}".format(query))

        return result


def write_to_file(marc_file, marc_bytearray):
    with open(marc_file, 'ab') as fp:
        fp.write(marc_bytearray)


def main_processing_loop(xlsx_file, marc_file):
    row_list = get_list_of_row_values(open_xlsx_file(xlsx_file))
    counter = 0

    for row in row_list:
        try:
            prepared_query = prepare_query_for_data_bn(row)
        except:
            continue
        downloaded_json_chunk = get_json_from_data_bn(prepared_query)
        data_from_json_chunk = get_data_from_json_chunk(downloaded_json_chunk)
        try:
            rec_ids = disambiguate_and_check_bib_records(data_from_json_chunk, row)
        except:
            continue  # some records don't have field 300 with number of pages

        if rec_ids:
            marc_data = get_marc_data_from_data_bn(rec_ids)
            write_to_file(marc_file, marc_data)
            logging.info(f'{row} || '
                         f'{[str(rcd_data) for rcd_data in data_from_json_chunk if rec_ids[0] == rcd_data.rcd_id]}')
            counter += 1
        else:
            logging.info(
                f'{row} || No records found.')

    with open('aszw_log_stats.txt', 'w', encoding='utf-8') as fp:
        fp.write(f'Odnaleziono {counter} z {str(len(row_list))} rekordów.')


if __name__ == '__main__':

    logging.root.addHandler(logging.StreamHandler(sys.stdout))
    logging.root.addHandler(logging.FileHandler('aszw_log_records.txt', encoding='utf-8'))
    logging.root.setLevel(level=logging.INFO)

    xlsx_file_in = file_in
    marc_file_out = file_out

    main_processing_loop(xlsx_file_in, marc_file_out)
