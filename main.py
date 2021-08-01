import logging
import os.path
import csv
from threading import Thread
import lichess.api
from lichess.format import SINGLE_PGN
import converter.pgn_data

log = logging.getLogger("lichess download")
logging.basicConfig(level=logging.INFO)

# THESE ARE THE ONLY INPUT VARIABLES FOR THE DOWNLOAD PROCESS
_names_list = ["thibault"]
_output_csv_name = "pgn_games"


class FileLoaderThread(Thread):
    """
    Files are downloaded from Lichess at the same time
    """

    def __init__(self, name):
        Thread.__init__(self)
        self.name = name

    def run(self):
        try:
            log.info("starting download for: " + self.name)
            pgn = lichess.api.user_games(self.name, format=SINGLE_PGN)
            file_name = get_pgn_filename(self.name)
            with open(file_name, 'w') as f:
                f.write(pgn)
            log.info("download complete for: " + self.name)
        except Exception as e:
            log.error("error downloading for:" + self.name)
            log.error(e)


def get_pgn_filename(name):
    # returns the the pgn file name to save the lichess data as
    return name + ".pgn"


def download_files():
    # Goes through all the listed lichess ids and starts the downloads
    thread_list = []
    for name in _names_list:
        th = FileLoaderThread(name)
        thread_list.append(th)
        th.start()
    for th in thread_list:
        th.join()


def __get_size(filename):
    if os.path.isfile(filename):
        st = os.stat(filename)
        return st.st_size
    else:
        return 0


def check_files_exist():
    # this checks that all the files we expect from lichess have been created
    log.info("validating downloads...")
    for name in _names_list:
        file_name = get_pgn_filename(name)
        if not os.path.isfile(file_name):
            log.info("Expected file : " + file_name + " not found")
            return False
        else:
            if __get_size(file_name) == 0:
                log.info("Expected file : " + file_name + " has no data!")
                return False
    log.info("validation complete")
    return True


def merge_files_to_csv():
    # this merges all the pgn files into a csv file
    files = []
    for name in _names_list:
        files.append(get_pgn_filename(name))
    pgn_data = converter.pgn_data.PGNData(files, _output_csv_name)
    result = pgn_data.export()
    return result


def add_extra():
    # this adds two columns to the end of the csv file
    # so that if there is different pgn files in the csv data
    # they can be all filtered together, when you want to look at result
    # across all the files. The rationale is, that the different files
    # group in one csv are more than likely the same user using different user_ids

    log.info("processing data...")
    file_source = _output_csv_name + "_game_info.csv"
    if os.path.isfile(file_source):
        file_dest = _output_csv_name + "_game_info_temp.csv"
        with open(file_source, 'r') as csvinput:
            with open(file_dest, mode='w', newline='', encoding="utf-8") as csvoutput:
                writer = csv.writer(csvoutput)
                for row in csv.reader(csvinput):
                    if row[0] == "game_id":
                        writer.writerow(row + ["group_name_color", "group_name_result"])
                    else:
                        w = row[6]
                        c = "white" if w in _names_list else 'black'
                        r = "won" if row[15] in _names_list else "lost"
                        writer.writerow(row + [c, r])

        if os.path.isfile(file_dest):
            log.info("tidying up...")
            # remove the original file
            os.remove(file_source)
            # rename the temp adjusted file to the same name as the original
            os.rename(file_dest, file_source)
            log.info("processing complete")
    else:
        log.info("could not find file:" + file_source)


def download_start():
    """
    This starts the process
    1) downloads pgn files from Lichess Server
    2) groups the pgn files into a temporary csv file
    3) creates a final csv output file using the temp file adding addtional info
    """
    if _names_list:
        download_files()
        exists = check_files_exist()
        if exists:
            result = merge_files_to_csv()
            result.print_summary()
            if result.is_complete:
                add_extra()
    else:
        log.info("No names have been listed for download")


if __name__ == '__main__':
    download_start()
