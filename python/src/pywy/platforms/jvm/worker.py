#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
import socket
import struct
import pickle
from itertools import chain

import cloudpickle
import base64
import re
import sys
import time

class SpecialLengths(object):
    END_OF_DATA_SECTION = -1
    PYTHON_EXCEPTION_THROWN = -2
    TIMING_DATA = -3
    END_OF_STREAM = -4
    NULL = -5
    START_ARROW_STREAM = -6


def read_int(stream):
    length = stream.read(4)
    if not length:
        raise EOFError
    res = struct.unpack("!i", length)[0]
    return res


class UTF8Deserializer:
    """
    Deserializes streams written by String.getBytes.
    """

    def __init__(self, use_unicode=True):
        self.use_unicode = use_unicode

    def loads(self, stream):
        length = read_int(stream)
        if length == SpecialLengths.END_OF_DATA_SECTION:
            raise EOFError
        elif length == SpecialLengths.NULL:
            return None
        s = stream.read(length)
        return s.decode("utf-8") if self.use_unicode else s

    def load_stream(self, stream):
        try:
            while True:
                yield self.loads(stream)
        except struct.error:
            return
        except EOFError:
            return

    def __repr__(self):
        return "UTF8Deserializer(%s)" % self.use_unicode


def write_int(p, outfile):
    outfile.write(struct.pack("!i", p))


def write_with_length(obj, stream):
    serialized = obj.encode('utf-8')
    if serialized is None:
        raise ValueError("serialized value should not be None")
    if len(serialized) > (1 << 31):
        raise ValueError("can not serialize object larger than 2G")
    write_int(len(serialized), stream)
    stream.write(serialized)


class Serializer:
    def dump_stream(self, iterator, stream):
        """
        Serialize an iterator of objects to the output stream.
        """
        raise NotImplementedError

    def load_stream(self, stream):
        """
        Return an iterator of deserialized objects from the input stream.
        """
        raise NotImplementedError

    def dumps(self, obj):
        """
        Serialize an object into a byte array.
        When batching is used, this will be called with an array of objects.
        """
        raise NotImplementedError

    def _load_stream_without_unbatching(self, stream):
        """
        Return an iterator of deserialized batches (iterable) of objects from the input stream.
        If the serializer does not operate on batches the default implementation returns an
        iterator of single element lists.
        """
        return map(lambda x: [x], self.load_stream(stream))

    # Note: our notion of "equality" is that output generated by
    # equal serializers can be deserialized using the same serializer.

    # This default implementation handles the simple cases;
    # subclasses should override __eq__ as appropriate.

    def __eq__(self, other):
        return isinstance(other, self.__class__) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "%s()" % self.__class__.__name__

    def __hash__(self):
        return hash(str(self))

class FramedSerializer(Serializer):

    """
    Serializer that writes objects as a stream of (length, data) pairs,
    where `length` is a 32-bit integer and data is `length` bytes.
    """

    def dump_stream(self, iterator, stream):
        for obj in iterator:
            self._write_with_length(obj, stream)

    def load_stream(self, stream):
        while True:
            try:
                yield self._read_with_length(stream)
            except EOFError:
                return

    def _write_with_length(self, obj, stream):
        serialized = self.dumps(obj)
        if serialized is None:
            raise ValueError("serialized value should not be None")
        if len(serialized) > (1 << 31):
            raise ValueError("can not serialize object larger than 2G")
        write_int(len(serialized), stream)
        stream.write(serialized)

    def _read_with_length(self, stream):
        length = read_int(stream)
        if length == SpecialLengths.END_OF_DATA_SECTION:
            raise EOFError
        elif length == SpecialLengths.NULL:
            return None
        obj = stream.read(length)
        if len(obj) < length:
            raise EOFError
        return self.loads(obj)

    def dumps(self, obj):
        """
        Serialize an object into a byte array.
        When batching is used, this will be called with an array of objects.
        """
        raise NotImplementedError

    def loads(self, obj):
        """
        Deserialize an object from a byte array.
        """
        raise NotImplementedError


class BatchedSerializer(Serializer):

    """
    Serializes a stream of objects in batches by calling its wrapped
    Serializer with streams of objects.
    """

    UNLIMITED_BATCH_SIZE = -1
    UNKNOWN_BATCH_SIZE = 0

    def __init__(self, serializer, batchSize=UNLIMITED_BATCH_SIZE):
        self.serializer = serializer
        self.batchSize = batchSize

    def _batched(self, iterator):
        if self.batchSize == self.UNLIMITED_BATCH_SIZE:
            print("hahahhaha")
            yield list(iterator)
        elif hasattr(iterator, "__len__") and hasattr(iterator, "__getslice__"):
            n = len(iterator)
            for i in range(0, n, self.batchSize):
                toc = time.perf_counter()
                print(f"batched toc1={toc:0.4f}")
                yield iterator[i : i + self.batchSize]
        else:
            items = []
            count = 0
            for item in iterator:
                items.append(item)
                count += 1
                if count == self.batchSize:
                    yield items
                    items = []
                    count = 0
            if items:
                yield items

    def dump_stream(self, iterator, stream):
        self.serializer.dump_stream(self._batched(iterator), stream)

    def load_stream(self, stream):
        return chain.from_iterable(self._load_stream_without_unbatching(stream))

    def _load_stream_without_unbatching(self, stream):
        return self.serializer.load_stream(stream)

    def __repr__(self):
        return "BatchedSerializer(%s, %d)" % (str(self.serializer), self.batchSize)


class PickleSerializer(FramedSerializer):

    """
    Serializes objects using Python's pickle serializer:

        http://docs.python.org/2/library/pickle.html

    This serializer supports nearly any Python object, but may
    not be as fast as more specialized serializers.
    """

    def dumps(self, obj):
        return pickle.dumps(obj, pickle_protocol)

    def loads(self, obj, encoding="bytes"):
        return pickle.loads(obj, encoding=encoding)

pickle_protocol = pickle.HIGHEST_PROTOCOL
class CloudPickleSerializer(FramedSerializer):
    def dumps(self, obj):
        try:
            return cloudpickle.dumps(obj, pickle_protocol)
        except pickle.PickleError:
            raise
        except Exception as e:
            emsg = str(e)
            if "'i' format requires" in emsg:
                msg = "Object too large to serialize: %s" % emsg
            else:
                msg = "Could not serialize object: %s: %s" % (e.__class__.__name__, emsg)
#            print_exec(sys.stderr)
            raise pickle.PicklingError(msg)

    def loads(self, obj, encoding="bytes"):
        return cloudpickle.loads(obj, encoding=encoding)

#if sys.version_info < (3, 8):
CPickleSerializer = PickleSerializer
#else:
#    CPickleSerializer = CloudPickleSerializer

def dump_stream(iterator, stream):

    for obj in iterator:
        if type(obj) is str:
            print("here?2")
            write_with_length(obj, stream)
        ## elif type(obj) is list:
        ##    write_with_length(obj, stream)
    print("Termine")
    write_int(SpecialLengths.END_OF_DATA_SECTION, stream)
    print("Escribi Fin")


def process(infile, outfile):
    """udf64 = os.environ["UDF"]
    print("udf64")
    print(udf64)
    #serialized_udf = binascii.a2b_base64(udf64)
    #serialized_udf = base64.b64decode(udf64)
    serialized_udf = bytearray(udf64, encoding='utf-16')
    # NOT VALID TO BE UTF8  serialized_udf = bytes(udf64, 'UTF-8')
    print("serialized_udf")
    print(serialized_udf)
    # input to be ast.literal_eval(serialized_udf)
    func = pickle.loads(serialized_udf, encoding="bytes")
    print ("func")
    print (func)
    print(func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]))
    # func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10])"""



    # TODO First we must receive the operator + UDF
    """udf = lambda elem: elem.lower()

    def func(it):
        return sorted(it, key=udf)"""
    udf_length = read_int(infile)
    print("udf_length")
    print(udf_length)
    serialized_udf = infile.read(udf_length)
    print("serialized_udf")
    print(serialized_udf)
    #base64_message = base64.b64decode(serialized_udf + "===")
    #print("base64_message")
    #print(base64_message)
    func = pickle.loads(serialized_udf)
    #func = ori.lala(serialized_udf)
    #print (func)
    #for x in func([1, 2, 3, 4, 5, 6, 7, 8, 9, 10]): print(x)


    """print("example")
    for x in func("2344|234|efrf|$#|ffrf"): print(x)"""
    # TODO Here we are temporarily assuming that the user is exclusively sending UTF8. User has several types
    iterator = UTF8Deserializer().load_stream(infile)
    # out_iter = sorted(iterator, key=lambda elem: elem.lower())
    # out_iter = batched(func(iterator))
    ser = BatchedSerializer(CPickleSerializer(), 100)
    ser.dump_stream(func(iterator), outfile)
    #dump_stream(iterator=out_iter, stream=outfile)


def local_connect(port):
    sock = None
    errors = []
    # Support for both IPv4 and IPv6.
    # On most of IPv6-ready systems, IPv6 will take precedence.
    for res in socket.getaddrinfo("127.0.0.1", port, socket.AF_UNSPEC, socket.SOCK_STREAM):
        af, socktype, proto, _, sa = res
        try:
            sock = socket.socket(af, socktype, proto)
            # sock.settimeout(int(os.environ.get("SPARK_AUTH_SOCKET_TIMEOUT", 15)))
            sock.settimeout(30)
            sock.connect(sa)
            # sockfile = sock.makefile("rwb", int(os.environ.get("SPARK_BUFFER_SIZE", 65536)))
            sockfile = sock.makefile("rwb", 65536)
            # _do_server_auth(sockfile, auth_secret)
            return (sockfile, sock)
        except socket.error as e:
            emsg = str(e)
            errors.append("tried to connect to %s, but an error occurred: %s" % (sa, emsg))
            sock.close()
            sock = None
    raise Exception("could not open socket: %s" % errors)


if __name__ == '__main__':
    print("Python version")
    print (sys.version)
    java_port = int(os.environ["PYTHON_WORKER_FACTORY_PORT"])
    sock_file, sock = local_connect(java_port)
    process(sock_file, sock_file)
    sock_file.flush()
    exit()