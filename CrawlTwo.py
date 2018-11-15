#!/usr/bin/env python
"""
This scripts downloads WARC files from commoncrawl.org's news crawl and extracts articles from these files. Users can
define filter criteria that need to be met (see YOUR CONFIG section), otherwise an article is discarded. Currently, the
script stores the extracted articles in JSON files, but this behaviour can be adapted to your needs in the method
on_valid_article_extracted.
08/14/2017 need to change subprocess.getoutput to subprocess.call
08/14/2017 change urllib to urllib2
08/14/2017 change "from urllib.request import urlretrieve" to "from urllib import urlretrieve"
08/14/2017 make sure you install aws S3 CLI, make sure in PATH and restart your app for it to grab it
08/15.2017 to add modules use pip install
08/15/2017 needed to add news-please, which has many dependencies.  o
            ne pain is this one - pywin32-220.win-amd64-py3.4.exe
08/15/2017 started to use internal loging instead of "scrapy" is it is discontinued
10/06/2017 this does work by running in pycharm

"""
import datetime
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
import urllib
import urllib.request
import urllib.parse

from ago import human
from dateutil import parser
from hurry.filesize import size
from warcio.archiveiterator import ArchiveIterator

from newsplease import NewsPlease

__author__ = "Felix Hamborg"
__copyright__ = "Copyright 2017"
__credits__ = ["Sebastian Nagel"]


class CommonCrawl:
    # ########### YOUR CONFIG ############
    # download dir for warc files
    local_download_dir_warc = 'D:/Documents/Crawl_Data/cc_download_warc/'
    # download dir for articles
    local_download_dir_article = './cc_download_articles/'
    # hosts (if None or empty list, any host is OK)
    filter_valid_hosts = ['foxnews.com', 'cnn.com']  # example: ['elrancaguino.cl']
    # filter_valid_hosts = []

    # filter on a word
    filter_text = input('Search keyword:  ')
    print(filter_text)
    wait = input("PRESS ENTER TO CONTINUE")

    # start date (if None, any date is OK as start date), as datetime
    filter_start_date = datetime.datetime(2018, 11, 1)
    # end date (if None, any date is OK as end date)
    filter_end_date = datetime.datetime(2018, 11, 2)
    # if date filtering is string, e.g., if we could not detect the date of an article, we will discard the article
    filter_strict_date = False
    # if True, the script checks whether a file has been downloaded already and uses that file instead of downloading
    # again. Note that there is no check whether the file has been downloaded completely or is valid!
    reuse_previously_downloaded_files = True
    # continue after error
    continue_after_error = False
    # ########### END YOUR CONFIG #########

    # commoncrawl.org
    cc_base_url = 'https://commoncrawl.s3.amazonaws.com/'
    cc_news_crawl_names = None

    # logging
    logging.basicConfig(filename='crawl.log', filemode='w', level=logging.WARNING)
    logger = logging.getLogger(__name__)

    def __setup__(self):
        """
        Setup
        :return:
        """
        if not os.path.exists(self.local_download_dir_warc):
            os.makedirs(self.local_download_dir_warc)
        if not os.path.exists(self.local_download_dir_article):
            os.makedirs(self.local_download_dir_article)

        ####
        # Debug
        # print(self.filter_text)
        # wait = input("PRESS ENTER TO CONTINUE")

        # make loggers quite
        # logging.getLogger('requests').setLevel(logging.WARNING)
        # logging.getLogger('readability').setLevel(logging.WARNING)
        # logging.getLogger('PIL').setLevel(logging.WARNING)
        # logging.getLogger('newspaper').setLevel(logging.WARNING)
        # logging.getLogger('newsplease').setLevel(logging.WARNING)

    def __filter_record(self, warc_record, article=None):
        """
        Returns true if a record passes all tests: hosts, publishing date
        :param warc_record:
        :return: A tuple of (True or False) and an article (might be None)
        """
        # filter by host if list is populated - empty host lists makes the process etremely slow.
        # seems like it is caused by the date checks

        if self.filter_valid_hosts:
            url = warc_record.rec_headers.get_header('WARC-Target-URI')
            # very simple check, check if one of the required host names is contained in the url of the WARC transaction
            # better would be to extract the host name from the WARC transaction Target URI and then check for equality
            # because currently something like g.co?forward_url=facebook.com would yield a positive filter test for
            # facebook.com even though the actual host is g.co
            # The below is necessary to make sure the for loop goes thru the entire list

            x = len(self.filter_valid_hosts)
            c = 0

            for valid_host in self.filter_valid_hosts:
                c = c + 1

                if valid_host in url:
                    break
                else:
                    if valid_host not in url and c == x:
                        return False, article

        # filter by date
        if self.filter_start_date or self.filter_end_date:
            if not article:
                article = NewsPlease.from_warc(warc_record)

            publishing_date = self.__get_publishing_date(article)

            if not publishing_date:
                if self.filter_strict_date:
                    return False, article
            else:
                # here we for sure have a date
                # is article published too early?
                if self.filter_start_date:
                    if publishing_date < self.filter_start_date:
                        return False, article
                if self.filter_end_date < publishing_date:
                        return False, article

        get_desc_data = self.__get_description_data(article)

        if not get_desc_data:
            return False, article
        else:
            if self.filter_text not in get_desc_data:
                return False, article
        return True, article

    @staticmethod
    def __get_publishing_date(article):
        """
        Extracts the publishing date from the record
        :param article:
        :return:
        """

        if article.date_publish:
            # changed the below to a string. was getting error leaving it as datetime
            return parser.parse(str(article.date_publish))
        else:
            return None

    @staticmethod
    def __get_description_data(article):
        """
        Extracts the description from the record
        :param article:
        :return:
        """
        if article.description:
            return article.description
        else:
            return None

    def __get_download_url(self, name):
        """
        Creates a download url given the name
        :param name:
        :return:
        """
        print('This is the url I am printed ' + self.cc_base_url + name)

        return self.cc_base_url + name

    def __get_remote_index(self):
        """
        Gets the index of news crawl files from commoncrawl.org and returns an array of names
        :return:
        """

        cmd1 = "aws s3 ls --recursive s3://commoncrawl/crawl-data/CC-NEWS/ --no-sign-request > tmpaws.txt"
        cmd2 = ["powershell.exe", "C:\\Users\\ralphd-laptop2\\PycharmProjects\\Crawl2\\getdata.ps1"]
        # use the below for Linux as it has awk
        # cmd2 = "awk '{ print $4 }' tmpaws.txt"

        self.logger.info('executing: %s', cmd1)
        subprocess.call(cmd1, shell=True)

        # this is the call that does not work in IDE.  Need to use Popen and then use PIPE and.communicate to put data
        # in variable object
        self.logger.info('executing: %s', cmd2)
        out_data = subprocess.Popen(cmd2, shell=True, stdout=subprocess.PIPE)
        stdout_data = out_data.communicate()[0].decode('utf-8')

        lines = stdout_data.splitlines()

        return lines

    @staticmethod
    def __on_download_progress_update(blocknum, blocksize, totalsize):
        """
        Prints some download progress information
        :param blocknum:
        :param blocksize:
        :param totalsize:
        :return:
        """
        readsofar = blocknum * blocksize
        if totalsize > 0:
            s = "\r%s / %s" % (size(readsofar), size(totalsize))
            sys.stdout.write(s)
            if readsofar >= totalsize:  # near the end
                sys.stderr.write("\r")
        else:  # total size is unknown
            sys.stdout.write("\rread %s" % (size(readsofar)))

    def __download(self, url):
        """
        Download and save a file locally.
        If the file has already been downloaded, it will not redownload
        :param url: Where to download from
        :return: File path name of the downloaded file
        """
        local_filename = urllib.parse.quote_plus(url)
        local_filepath = os.path.join(self.local_download_dir_warc, local_filename)

        if os.path.isfile(local_filepath) and self.reuse_previously_downloaded_files:
            self.logger.info("found local file, not downloading again (check reuse_previously_downloaded_files to "
                             "control this behaviour)")
            return local_filepath
        else:
            self.logger.info('downloading %s, local: %s', url, local_filepath)
            urllib.request.urlretrieve(url, local_filepath, reporthook=self.__on_download_progress_update)
            self.logger.info('download completed, local file: %s', local_filepath)
            return local_filepath

    def __process_warc_gz_file(self, path_name):
        """
        Iterates all transactions in one WARC file and for each transaction tries to extract an article object.
        Afterwards, each article is checked against the filter criteria and if all are passed, the function
        on_valid_article_extracted is invoked with the article object.
        :param path_name:
        :return:
        """
        counter_article_total = 0
        counter_article_passed = 0
        counter_article_discarded = 0
        start_time = time.time()

        with open(path_name, 'rb') as stream:
            # opens a file and returns a stream 'rb' = read/binary
            for record in ArchiveIterator(stream):
                try:
                    # Every WARC record shall have a type, reported in the WARC-Type field. There are eight WARC record
                    # types: 'warcinfo', 'response', 'resource', 'request', 'metadata', 'revisit', 'conversion',
                    # and 'continuation'.
                    if record.rec_type == 'response':
                        counter_article_total += 1

                        # if the article passes filter tests, we notify the user
                        # this calls the filter function and returns a True / false and the article
                        filter_pass, article = self.__filter_record(record)

                        if filter_pass:
                            counter_article_passed += 1

                            if not article:
                                article = NewsPlease.from_warc(record)
                            self.logger.info('article pass (%s; %s; %s)', article.source_domain, article.date_publish,
                                             article.title)
                            self.on_valid_article_extracted(article)
                        else:
                            counter_article_discarded += 1

                            if article:
                                self.logger.info('article discard (%s; %s; %s)', article.source_domain,
                                                 article.date_publish,
                                                 article.title)
                            else:
                                self.logger.info('article discard (%s)',
                                                 record.rec_headers.get_header('WARC-Target-URI'))

                        if counter_article_total % 10 == 0:
                            elapsed_secs = time.time() - start_time
                            secs_per_article = elapsed_secs / counter_article_total
                            self.logger.info('statistics')
                            self.logger.warning('pass = %i, discard = %i, total = %i', counter_article_passed,
                                                counter_article_discarded, counter_article_total)
                            self.logger.warning('extraction from current WARC file started %s; %f s/article',
                                                human(start_time), secs_per_article)
                except:
                    if self.continue_after_error:
                        self.logger.error('Unexpected error: %s', sys.exc_info()[0])
                        pass
                    else:
                        raise

    def run(self):
        """
        Main execution method, which consists of: get an up-to-date list of WARC files, and for each of them: download
        and extract articles. Each article is checked against a filter. Finally, for each valid article the method
        on_valid_article_extracted will be invoked after the extraction of the article has completed.
        :return:
        """
        self.__setup__()

        self.cc_news_crawl_names = self.__get_remote_index()

        self.logger.info('found %i files at commoncrawl.org', len(self.cc_news_crawl_names))

        # iterate the list of crawl_names, that was retrieved by the get_remote_index function which populated
        # this cc_news_crawl_names object.
        # And for each: download and process it
        for name in self.cc_news_crawl_names:
            #  this will allow us to limit download data
            y = int(name[35:39])
            m = int(name[39:41])
            d = int(name[41:43])
            file_date = datetime.datetime(y, m, d)
            print(file_date)

            # filter_start_date = datetime.date(2016, 8, 26)
            print(self.filter_start_date)

            if self.filter_start_date <= file_date <= self.filter_end_date:
                download_url = self.__get_download_url(name)
                local_path_name = self.__download(download_url)
                self.__process_warc_gz_file(local_path_name)

    @staticmethod
    def __get_pretty_filepath(path, article):
        """
        Pretty might be an euphemism, but this function tries to avoid too long filenames, while keeping some structure.
        :param path:
        :param article:
        :return:
        """
        short_filename = hashlib.sha256(article.filename.encode()).hexdigest()
        # the below works but need to shorten the description as the file name becoes to long
        # short_filename = article.description

        sub_dir = article.source_domain

        final_path = path + sub_dir + '/'

        if not os.path.exists(final_path):
            os.makedirs(final_path)
        return final_path + short_filename + '.json'

    def on_valid_article_extracted(self, article):
        """
        This function will be invoked for each article that was extracted successfully from the archived data and that
        satisfies the filterwait criteria.
        :param article:
        :return:
        """

        # do whatever you need to do with the article (e.g., save it to disk, store it in ElasticSearch, etc.)
        with open(self.__get_pretty_filepath(self.local_download_dir_article, article), 'w') as outfile:
            # OLD CODE  json.dump(article, outfile, indent=4, sort_keys=True)
            json.dump(article.__dict__, outfile, default=str, indent=4, sort_keys=True)
        # ...

        return


if __name__ == '__main__':
    # here we are creating the object (https://commoncrawl.org)
    common_crawl = CommonCrawl()
    # here is where we run the function 'run' which starts the program
    common_crawl.run()
