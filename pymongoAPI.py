from pymongo import MongoClient
import datetime

GROUP_VALUES = {
    'month': {'day': 1, 'hour': 0},
    'day': {'day': {"$dayOfMonth": "$dt"}, 'hour': 0},
    'hour': {'day': {"$dayOfMonth": "$dt"}, 'hour': {"$hour": "$dt"}},
}


class MongoDB(object):
    """
    Объект коллекции MongoDB
    """
    def __init__(self, host: str,
                 port: int,
                 db_name: str,
                 collection: str):
        self._client = MongoClient(f'mongodb://{host}:{port}')
        self._collection = self._client[db_name][collection]

    def get_aggregated(self, dt_from: datetime, dt_upto: datetime, group_type: str):
        pipeline = [
            {
                "$match": {
                    "dt": {'$gte': dt_from, '$lte': dt_upto}
                },
            },
            {
                "$group": {
                    "_id": {
                        '$dateFromParts': {
                            'year': {"$year": "$dt"},
                            'month': {"$month": "$dt"},
                            'day': GROUP_VALUES[group_type]['day'],
                            'hour': GROUP_VALUES[group_type]['hour']
                        }
                    },
                    "summary": {"$sum": "$value"}
                }
            },
            {"$sort": {"_id": 1}},
        ]

        try:
            agg_data = self._collection.aggregate(pipeline)
            return agg_data
        except Exception as ex:
            raise Exception(
                f'Не удалось получить данные из базы: {ex}'
            )