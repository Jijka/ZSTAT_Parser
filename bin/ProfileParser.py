import sys

import LogFile

__author__ = 'i.akhaltsev'


def main(argv):
    print('Importing Log...')
    print('Script Path ', str(argv[1]))
    log_file_path = str(argv[1])
    log_file = LogFile.LogFile(log_file_path)
    log_file.Parse()


if __name__ == '__main__':
    # try:
    main(sys.argv)
    # except:
    #   print("Sorry:", sys.exc_info(), ":", sys.exc_value)
