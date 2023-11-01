import json
from datetime import datetime, timedelta


GROUP_TYPES = (
    'month',
    'day',
    'hour'
)


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

    except Exception as ex:
        msg = f'{ex}'
        return msg

    return f'dt_from = {dt_from}, dt_upto = {dt_upto}, group_type = {group_type}'
