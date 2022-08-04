import math
# from sqlalchemy.orm import query
from mongoengine.queryset.base import BaseQuerySet


# def sqlalchemy_paginator(query: query.Query, page, pagesize=None):
#     count = query.count()
#
#     if page is not None and pagesize is not None:
#         offset = (page - 1) * pagesize
#         query = query.offset(offset).limit(pagesize)
#
#     res = query.all()
#
#     return {
#         "list": list(res),
#         "total_count": count,
#         "page_count": (pagesize and math.ceil(count / pagesize)) or 1
#     }


def mongo_paginator(query: BaseQuerySet, page, pagesize=20):
    offset = (page - 1) * pagesize
    limit = page * pagesize
    res = query[offset:limit]
    count = query.count()
    return {
        "objects": list(res),
        'page_index': page,
        'page_size': pagesize,
        'total_page': math.ceil(count / pagesize),
        'total_count': count
    }


def mongo_aggregate_paginator(query, pipeline, page=1, pagesize=20):
    offset = (page - 1) * pagesize
    limit = page * pagesize
    res = query.aggregate(pipeline+[{'$limit': limit}, {'$skip': offset}])
    pipeline.append({"$count": "ny"})
    count = list(query.aggregate(pipeline))[0].get("ny", 0)
    return {
        "objects": list(res),
        'page_index': page,
        'page_size': pagesize,
        'total_page': math.ceil(count / pagesize),
        'total_count': count
    }

