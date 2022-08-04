import os
from collections import OrderedDict

os.environ["NEUTRON_STAR_MODULE"] = "payslip"

from mongoengine import base, Document
import mongoengine

OldTopLevelDocumentMetaclass = base.TopLevelDocumentMetaclass

documents = []


class TopLevelDocumentMetaclass(OldTopLevelDocumentMetaclass):
    def __new__(cls, name, bases, attrs):
        doc = super().__new__(cls, name, bases, attrs)
        if not attrs.get("_ignore"):
            documents.append(doc)
        return doc


class Doc(Document, metaclass=TopLevelDocumentMetaclass):
    _ignore = True
    meta = {
        'abstract': True,
        'allow_inheritance': True,
    }

mongoengine.Document = Doc

from app import create_app, init_web

app = create_app()
init_web(app)


def build_indexes():
    result = {}
    for doc in documents:
        meta = doc._meta
        indexes = meta.get("indexes", [])
        collection = meta.get("collection")
        if not collection:
            continue
        result[collection] = [OrderedDict(index)
                for index in indexes]
    return result


def build_script():
    return '\n'.join(f"db.getCollection('{k}').createIndexes({json.dumps(v)});"
            for k, v in build_indexes().items() if k and v
    )

if __name__ == "__main__":
    import json
    print(build_script())
