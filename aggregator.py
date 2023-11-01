import json
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

from pymongoAPI import MongoDB

load_dotenv()

GROUP_TYPES = (
    'month',
    'day',
    'hour'
)

dbase = MongoDB(host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT')),
                db_name=os.getenv('DB_NAME'),
                collection=os.getenv('COLLECTION'))


def format_data(request: str):
    """
    Функция принимает запрос пользователя в строковом виде
    и при соответствии формата отдает параметры аггрегации
    dt_from, dt_upto и group_type
    """
    try:
        request_json = json.loads(request)
        dt_from = datetime.fromisoformat(request_json["dt_from"])
        dt_upto = datetime.fromisoformat(request_json["dt_upto"])
        if dt_from >= dt_upto:
            raise Exception(
                'Начальная дата интервала должна быть раньше конечной.'
            )
        if request_json["group_type"] in GROUP_TYPES:
            group_type = request_json["group_type"]
        else:
            raise Exception(
                'Поле group_type может принимать только значения '
                '"month", "day" или "hour".'
            )
        if group_type == 'hour' and (dt_upto - dt_from) > timedelta(1, 0):
            raise Exception(
                'Почасовая аггрегация возможна на интервале не более суток.'
            )
    except json.JSONDecodeError:
        raise Exception(
            'Данные запроса не соответствуют формату json. '
            'Пример корректного запроса:\n'
            '{"dt_from":"2022-09-01T00:00:00", '
            '"dt_upto":"2022-12-31T23:59:00", "group_type":"month"}'
        )
    except KeyError as err:
        raise Exception(
            f'В запросе должно присутствовать поле {err}'
        )

    return dt_from, dt_upto, group_type


def aggregator(request):
    """
    Функция выполняет аггрегацию по заданному запросу
    """
    try:
        dt_from, dt_upto, group_type = format_data(request)
        quiery = dbase.get_aggregated(dt_from, dt_upto, group_type)

    except Exception as ex:
        msg = f'{ex}'
        return msg

    delta = dt_upto - dt_from
    if group_type == "day":
        labels = [(dt_from + timedelta(i)) for i in range(delta.days + 1)]
    elif group_type == 'month':
        current_date = dt_from
        labels = []
        while current_date <= dt_upto:
            labels.append(current_date)
            if current_date.month < 12:
                current_date = datetime(
                    year=current_date.year,
                    month=current_date.month + 1,
                    day=1
                )
            else:
                current_date = datetime(
                    year=current_date.year + 1,
                    month=1,
                    day=1
                )
    else:
        current_date = dt_from
        labels = []
        while current_date <= dt_upto:
            labels.append(current_date)
            if current_date.hour < 23:
                current_date = datetime(
                    year=current_date.year,
                    month=current_date.month,
                    day=current_date.day,
                    hour=current_date.hour + 1
                )
            else:
                current_date = datetime(
                    year=current_date.year,
                    month=current_date.month,
                    day=current_date.day + 1,
                    hour=0
                )
    labels_iso = [date.isoformat() for date in labels]
    dataset = [0] * len(labels)

    i = 0
    for elm in quiery:
        while i < len(labels) and elm["_id"] != labels[i]:
            i += 1
        dataset[i] = elm["summary"]

    return json.dumps({"dataset": dataset, "labels": labels_iso})
