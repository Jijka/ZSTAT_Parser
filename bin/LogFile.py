from datetime import *
import time
import threading
import os
from tqdm import tqdm

__author__ = 'i.akhaltsev'


class LogFile(object):
    def __init__(self, log_file_path, separator=';'):
        self.log_file_path = log_file_path
        self.separator = separator
        self.newline = "\n"
        self.datetime_from = datetime(1840, 12, 31, 0, 0, 0)
        self.output_dir = './Output/'
        if not (os.path.exists(self.output_dir)):
            os.mkdir(self.output_dir)

    def writer(self, lines, output_file, read_file_name):
        print('Запись файла для ' + output_file)
        with open(self.output_dir + read_file_name + '_' + output_file + '.out', 'w') as file:
            for line in lines:
                file.write(line)

    def __open(self):
        self.file = open(self.log_file_path, 'r')
        print(self.file.name + ' is opened')
        self.File_Output_Ops = self.__Open_New_File_For_Write(os.path.basename(self.file.name) + '_OPS.out')
        self.File_Output_ISO = self.__Open_New_File_For_Write(os.path.basename(self.file.name) + '_ISO.out')

    def __close(self, file):
        file.close()
        print(file.name + ' was closed')
        self.__close(self.File_Output_Ops)
        self.__close(self.File_Output_ISO)

    def __Open_New_File_For_Write(self, name):
        print(name + ' is opened')
        if not (os.path.exists(self.output_dir)):
            os.mkdir(self.output_dir)
        return open(self.output_dir + name, 'w')

    def decode_date(self, date_time):
        if ',' in date_time:
            split_datetime = date_time.split(',')
            d = int(split_datetime[0])
            t = int(split_datetime[1])
        else:
            d = int(date_time[0:5])
            t = int(date_time[5:10])
        return str(self.datetime_from + timedelta(days=d, seconds=t))

    def __ISO8583_Row_Builder(self, split_line):
        l = [self.decode_date(split_line[5]), split_line[4], split_line[6], split_line[8]]
        return self.separator.join(l) + self.newline

    def __OPS_Row_Builder(self, split_line):
        l = [self.decode_date(split_line[8]), split_line[2], split_line[6], split_line[9]]
        return self.separator.join(l)

    def parse(self):
        time1 = time.time()
        with open(self.log_file_path, 'r') as read_file:
            split_lines_ops = []
            split_lines_iso = []
            for line in tqdm(read_file):
                if line.count('\t') < 7:
                    continue
                split_line = line.split('\t')
                if 'ISO8583' in split_line:
                    split_lines_iso.append(self.__ISO8583_Row_Builder(split_line))
                else:
                    split_lines_ops.append(self.__OPS_Row_Builder(split_line))
        time2 = time.time()
        # init threads
        t1 = threading.Thread(target=self.writer,
                              args=(split_lines_ops, 'OPS', os.path.basename(read_file.name)))
        t2 = threading.Thread(target=self.writer,
                              args=(split_lines_iso, 'ISO', os.path.basename(read_file.name)))
        # start threads
        t1.start()
        t2.start()

        # join threads to the main thread
        t1.join()
        t2.join()
        end = time.time()
        print('Parsing: ', time2 - time1)
        print('Writing: ', end - time2)
        print('ALL: ', end - time1)
        print('Парсинг успешно завершен!')
