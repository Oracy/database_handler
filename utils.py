import datetime
import json
import os

import pytz


def now() -> datetime:
    """
    Return datetime now at UTC timezone
    """
    return pytz.UTC.localize(datetime.datetime.utcnow())


def now_br() -> datetime:
    """
    Return datetime now at BRT - America/Sao_Paulo timezone
    """
    return now().astimezone(pytz.timezone("America/Sao_Paulo"))


def now_pt() -> datetime:
    """
    Return datetime now at PT - Portugal timezone
    """
    return now().astimezone(pytz.timezone("Portugal"))


def get_db_data(path: str = None) -> json:
    """
    Create json file at home directory or if you create in other path pass
    it by parameter:
    "~/access_information.json"
    {
        "database_name": {
            "host": "domain.net",
            "user": "domain\\user.name",
            "password": "password",
            "database": "db_name"
        },
    }

    Args:
        path: An array with table name that whould be created.
    Returns:
        A Json file with database data. For example:
    {
        "bimasterdw_bi_hadoop": {
            "host": "domain.net",
            "user": "domain\\username",
            "password": "password",
            "database": "DATABASE_NAME"
        }
    }
    """
    db_file_name: str = os.path.expanduser("~/access_information.json") if path == None else path
    with open(db_file_name, "r") as db_file:
        db_access: json = json.load(db_file)
    return db_access
