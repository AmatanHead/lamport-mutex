import codecs
import pickle


class Serializer:
    @staticmethod
    def dump(obj):
        return pickle.dumps(obj).replace(b'\\', b'\\\\').replace(b'\n', b'\\n')

    @staticmethod
    def load(data):
        return pickle.loads(codecs.escape_decode(data)[0])
