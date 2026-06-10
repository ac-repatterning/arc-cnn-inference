"""Module foci.py"""
import datetime
import logging
import zoneinfo

import pandas as pd

import config
import src.elements.s3_parameters as s3p
import src.elements.text_attributes as txa
import src.functions.cache
import src.functions.streams


class Foci:
    """
    Retrieves the gauge stations in focus, vis-à-vis the latest weather warning.
    """

    def __init__(self, s3_parameters: s3p.S3Parameters):
        """

        :param s3_parameters: The overarching S3 parameters settings of this
                              project, e.g., region code name, buckets, etc.
        """

        self.__s3_parameters = s3_parameters
        self.__streams = src.functions.streams.Streams()
        self.__configurations = config.Config()

        # For casting the date & time fields
        self.__doublet = {'issued_date': 'ISO8601', 'modified': 'ISO8601',
                          'starting': 'ISO8601', 'ending': 'ISO8601'}

        # Time: Or datetime.datetime.now(tz=pytz.utc)
        self.__stamp = pd.Timestamp(datetime.datetime.now(), tz=zoneinfo.ZoneInfo('Europe/London'))
        logging.info('self.__stamp: %s', self.__stamp)

    def __filtering(self, warnings: pd.DataFrame) -> pd.DataFrame:
        """
        Filter out the instances outwith the time period in focus

        :param warnings:
        :return:
        """

        instances = warnings[['issued_date', 'warning_id']].drop_duplicates()
        instances.sort_values(by='issued_date', ascending=True, inplace=True)
        elements = instances.iloc[-1, :].squeeze()
        logging.info(warnings.loc[warnings['warning_id'] == elements.warning_id, :])

        conditionals = ((warnings['warning_id'] == elements.warning_id) &
                        (warnings['ending'] >= self.__stamp))

        return warnings.copy().loc[conditionals, :]

    def __casting(self, warnings: pd.DataFrame) -> pd.DataFrame:
        """
        Ascertains field types

        :param warnings:
        :return:
        """

        for key, value in self.__doublet.items():
            warnings[key] = pd.to_datetime(warnings[key], format=value, utc=True)
        logging.info(warnings)

        return warnings

    def __get_warning_signals(self) -> pd.DataFrame:
        """
        Reads the library, data file, of warnings.

        :return:
        """

        uri = f's3://{self.__s3_parameters.internal}/{self.__configurations.signals_key}'
        text = txa.TextAttributes(uri=uri, header=0)

        return self.__streams.read(text=text)

    def exc(self) -> pd.DataFrame:
        """

        :return:
        """

        warnings = self.__get_warning_signals()
        warnings = self.__casting(warnings=warnings.copy())
        warnings = self.__filtering(warnings=warnings.copy())

        if warnings.empty:
            logging.info('No warnings')
            return pd.DataFrame()

        return warnings[['catchment_id', 'ts_id']].drop_duplicates()
