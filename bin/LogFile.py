from datetime import *
import time
import threading
import os

__author__ = 'i.akhaltsev'


class LogFile(object):
    def __init__(self, log_file_path, separator=';'):
        self.log_file_path = log_file_path
        self.separator = separator
        self.newline = "\n"
        self.datetime_from = datetime(1840, 12, 31, 0, 0, 0)
        # self.time_from = time.time() (1,1,0,0,0)
        self.output_dir = './Output/'
        if not (os.path.exists(self.output_dir)):
            os.mkdir(self.output_dir)

    def writer(self, lines, output_file, read_file_name):
        print('Поток запущен ' + output_file)
        with open(self.output_dir + read_file_name + '_' + output_file + '.out', 'w') as file:
            for line in lines:
                file.write(line)

    def __Open(self):
        self.file = open(self.log_file_path, 'r')
        print(self.file.name + ' is opened')
        self.File_Output_Ops = self.__Open_New_File_For_Write(os.path.basename(self.file.name) + '_OPS.out')
        self.File_Output_ISO = self.__Open_New_File_For_Write(os.path.basename(self.file.name) + '_ISO.out')

    def __Close(self, file):
        file.close()
        print(file.name + ' was closed')
        self.__Close(self.File_Output_Ops)
        self.__Close(self.File_Output_ISO)

    def __Open_New_File_For_Write(self, name):
        print(name + ' is opened')
        if not (os.path.exists(self.output_dir)):
            os.mkdir(self.output_dir)
        return open(self.output_dir + name, 'w')

    def decode_date(self, date_time):
        split_datetime = date_time.split(',')
        if len(split_datetime) < 2:
            date = int(date_time[0:5])
            t = int(date_time[5:10])
        else:
            date = int(split_datetime[0])
            t = int(split_datetime[1])
        return str(self.datetime_from + timedelta(days=int(date), seconds=int(t)))

    def __ISO8583_Row_Builder(self, split_line):
        l = []
        l.append(self.decode_date(split_line[5]))
        l.append(split_line[4])
        l.append(split_line[6])
        l.append(split_line[8])
        return self.separator.join(l) + self.newline

    def __OPS_Row_Builder(self, split_line):
        l = []
        l.append(self.decode_date(split_line[8]))
        l.append(split_line[2])
        l.append(split_line[6])
        l.append(split_line[9])
        return self.separator.join(l)

    def Parse(self):
        # self.__Open()
        # t1 = time.time()
        # split_lines = [x.split('\t') for x in self.file]

        # t2 = time.time()
        # OPS = map(lambda x: self.__OPS_Row_Builder(x), filter(lambda x: len(x) > 9, split_lines))
        # for line in OPS:
        #    self.File_Output_Ops.write(line)
        # print('OPS: ', time.time() - t2)

        # t3 = time.time()
        # ISO = map(lambda x: self.__ISO8583_Row_Builder(x), filter(lambda x:'ISO8583' in x, split_lines))
        # for line in ISO:
        #    self.File_Output_ISO.write(line)
        # print('ISO: ',time.time() - t3)

        # print('ALL: ',time.time() - t1)
        time1 = time.time()
        with open(self.log_file_path, 'r') as read_file:
            split_lines = [x.split('\t') for x in read_file if x.count('\t') > 6]
            split_lines_ops = (self.__OPS_Row_Builder(line) for line in split_lines if not ('ISO8583' in line))
            split_lines_iso = (self.__ISO8583_Row_Builder(line) for line in split_lines if 'ISO8583' in line)

            # for split_line in split_lines_OPS:
            # ops_file.write(self.__OPS_Row_Builder(split_line))
            # with open(self.output_dir + os.path.basename(read_file.name) + '_ISO.out', 'w') as iso_file:
            #    for split_line in split_lines_ISO:
            #        iso_file.write(self.__ISO8583_Row_Builder(split_line))


            # with open(self.output_dir + os.path.basename(read_file.name) + '_ISO.out', 'w') as iso_file:
            #    with open(self.output_dir + os.path.basename(read_file.name) + '_OPS.out', 'w') as ops_file:
            #        split_lines_ISO = [iso_file.write(self.__ISO8583_Row_Builder(line)) if 'ISO8583' in line else ops_file.write(self.__OPS_Row_Builder(line)) for line in split_lines]
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

        print('ALL: ', time.time() - time1)
        print('Парсинг успешно завершен!')
