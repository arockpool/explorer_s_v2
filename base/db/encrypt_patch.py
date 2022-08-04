from mongoengine import base, Document
import mongoengine

OldTopLevelDocumentMetaclass = base.TopLevelDocumentMetaclass
OldDocumentMetaclass = base.DocumentMetaclass

documents = []


patched = set()

def to_python(self, value):
    if getattr(self, "encrypt", False):
        # TODO decrypt here
        print("decrypting.......", value)
    return self.__class__._to_python(self, value)

def to_mongo(self, value, *args, **kwargs):
    if getattr(self, "encrypt", False):
        # TODO encrypt here
        print("encrypting.......", value)
    return self.__class__._to_mongo(self, value, *args, **kwargs)


def patch_to(doc):
    for f, m in doc._fields.items():
        if m.__class__ in patched or \
                set(m.__class__.__mro__).intersection(patched):
            continue
        m.__class__._to_python= m.__class__.to_python
        m.__class__.to_python= to_python
        m.__class__._to_mongo= m.__class__.to_mongo
        m.__class__.to_mongo= to_mongo
        patched.add(m.__class__)


class TopLevelDocumentMetaclass(OldTopLevelDocumentMetaclass):
    def __new__(cls, name, bases, attrs):
        doc = super().__new__(cls, name, bases, attrs)
        patch_to(doc)
        return doc


class DocumentMetaclass(OldDocumentMetaclass):
    def __new__(cls, name, bases, attrs):
        doc = super().__new__(cls, name, bases, attrs)
        patch_to(doc)
        return doc

class Doc(Document, metaclass=TopLevelDocumentMetaclass):
    _ignore = True
    meta = {
        'abstract': True,
        'allow_inheritance': True,
    }

def patch():
    mongoengine.Document = Doc
    mongoengine.EmbeddedDocument = DocumentMetaclass


