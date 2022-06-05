#!/usr/bin/env python3

from io import StringIO
from datetime import timedelta
from dateutil.parser import parse

import pandas as pd
import chardet
from beancount.core import amount, data
from beancount.core.number import D
from beancount.ingest import importer
from beancount.ingest.importers.mixins import identifier
import logging

import camelot
import pandas as pd
import numpy as np
from PyPDF2 import PdfFileReader


def eprint(fp, e):
    print(f'''"{fp}", # {e}''')

class ErrorCleaning(BaseException):
    pass

def process_file(fp):
    first_page_region = "0,580,567,75"
    full_page_region = "0,730,575,75"
    columns = '95,157,451,520'

    num_pages = PdfFileReader(fp).numPages
    try:
        match num_pages:
            case 0:
                raise "empty pdf?"
            case 1:
                df = camelot.read_pdf(fp, flavor='stream', table_areas=[first_page_region], columns=[columns])[0].df
            case _:
                df_1 = camelot.read_pdf(fp, flavor='stream', pages='1', table_areas=[first_page_region], columns=[columns])
                df_rest = camelot.read_pdf(fp, flavor='stream', pages='2-end', table_areas=[full_page_region], columns=[columns])
                df = pd.concat([df_1[0].df] + [df.df for df in df_rest])
                df.reset_index(inplace=True)
                df.drop(columns=["index"], inplace=True)
    except:
        raise "broken file" + f

    return df

def post_processing(df: pd.DataFrame, fp: str, debug=False):
    df = df.replace('', np.nan)
    df.dropna(axis=1, how='all', inplace=True)
    df = df.fillna(method="ffill", axis=0)

    if debug:
        try:
            df, status = clear_footer(df)
            df = df.groupby([0,1,3,4], sort=False).aggregate(lambda x: ' '.join(x)).reset_index()
        except ErrorCleaning as e:
            return df.columns
    else:
        try:
            df, status = clear_footer(df)
        except ErrorCleaning as e:
            eprint(fp, e)
            return pd.DataFrame()
        try:
            df = df.groupby([0,1,3,4], sort=False).aggregate(lambda x: '^^^'.join(x)).reset_index()
        except:
            eprint(fp, "unable to group")
            return pd.DataFrame()

    df.columns = ["DATA TRANSAKCJI", "DATA KSIEGOWANIA", "KWOTA TRANSAKCJI", "SALDO PO TRANSAKCJI", "OPIS TRANSAKCJI"]
    df['DATA TRANSAKCJI'] = pd.to_datetime(df["DATA TRANSAKCJI"], format="%Y.%m.%d")
    df['DATA KSIEGOWANIA'] = pd.to_datetime(df["DATA KSIEGOWANIA"], format="%Y.%m.%d")

    df['status'] = status
    df['filename'] = fp

    return df

def clear_footer(df: pd.DataFrame):
    status = ""
    if df.shape[0] == 0:
        raise ErrorCleaning("empty")
    if df.shape[1] < 5:
        raise ErrorCleaning("not enough columns")

    cut_point = min(df[df[2].str.contains("Uznania|Obciążenia|Saldo końcowe")].index.tolist(), default=None)

    if cut_point:
        ndf = df.drop(index=list(range(cut_point, df.shape[0])))
        ndf = ndf.replace('', np.nan)
        ndf.dropna(axis=1, how='all', inplace=True)
    try:
        ndf.columns = list(range(5))
    except:
        status = "too many columns"
        try:
            _null = ndf
        except:
            return df, "no cutoff"
    return ndf, status

def getinbank_pdf_to_df(filepath: str) -> pd.DataFrame:
    return post_processing(process_file(filepath), filepath)

def get_narration_and_location(row):
    return row["OPIS TRANSAKCJI"], None
    
class Importer(identifier.IdentifyMixin, importer.ImporterProtocol):
    """An importer for Revolut CSV files."""

    def __init__(self, regexps="[0-9a-f]{8}_\d{6}_[0-9a-f]{14}\.\d{8}\.pdf",
                 account="Assets:PL:GetinBank", currency="PLN"):
        identifier.IdentifyMixin.__init__(self, matchers=[("filename", regexps)])
        self.account = account
        self.currency = currency

    def name(self):
        return super().name() + self.account

    def file_account(self, file):
        return self.account

    def extract(self, file, existing_entries):
        entries = []

        df = getinbank_pdf_to_df(file.name)
        for idx, row in df.iterrows():
            metakv = {}
            try:
                bal = D(
                    row["SALDO PO TRANSAKCJI"].replace(" ", "").replace(",", ".").strip()
                )
                amount_raw = D(row["KWOTA TRANSAKCJI"].replace(" ", "").replace(",", ".").strip())
                amt = amount.Amount(amount_raw, self.currency)
                balance = amount.Amount(bal, self.currency)
                book_date = row["DATA KSIEGOWANIA"].date()
                narration, location = get_narration_and_location(row)
                
            except Exception as e:
                logging.warning(e)
                continue
            if location:
                metakv["location"] = location
            metakv["description"] = row["OPIS TRANSAKCJI"]
            metakv["transaction_date"] = row["DATA TRANSAKCJI"]
            meta = data.new_metadata(file.name, 0, metakv)
            entry = data.Transaction(
                meta=meta,
                date=book_date,
                flag="*",
                payee="",
                narration=narration,
                tags=data.EMPTY_SET,
                links=data.EMPTY_SET,
                postings=[
                    data.Posting(self.account, -amt, None, None, None, None),
                    data.Posting("Expenses:FIXME", amt, None, None, None, None),
                ],
            )
            entries.append(entry)
        return entries
