from __future__ import division

from functools import wraps
from itertools import count
import json
import struct
import sys
from twisted.internet import reactor

from twisted.internet.defer import Deferred, inlineCallbacks, maybeDeferred
from twisted.internet.protocol import Protocol, Factory
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol
from twisted.python import log
from txzookeeper import ZookeeperClient

zk = ZookeeperClient(servers='localhost:2181')


from parsley import makeProtocol



def encodeString(string):
    assert isinstance(string, bytes)
    return struct.pack('>H', len(string)) + string


def encodeArray(seq, elementEncoder=lambda elem: elem):
    prefix = struct.pack('>I', len(seq))
    return prefix + b''.join(elementEncoder(e) for e in seq)

def _encodeTopicFetchRequest(elem):

    def _encodeTopicMeta(meta):
        partition, offset, maxbytes = meta
        return struct.pack('>IQI', partition, offset, maxbytes)

    name, meta = elem
    print 'xD', meta
    return b''.join([
        encodeString(name),
        encodeArray(meta, elementEncoder=_encodeTopicMeta)
    ])

class KafkaSender(object):
    def __init__(self, transport):
        self._transport = transport
        self._nextCorrelationId = count()
        self._waiting = {}
        self._types = {}

    def metadataRequest(self, topicNames=None):
        if topicNames is None:
            topicNames = []
        apikey = 3
        apiversion = 0
        correlationid = next(self._nextCorrelationId)
        encoded = b''.join([
            struct.pack('>HHI', apikey, apiversion, correlationid),
            encodeString('asdf'),
            encodeArray(topicNames, elementEncoder=encodeString)
        ])
        d = Deferred()
        self._waiting[correlationid] = d
        self._types[correlationid] = 'metadataRequest'
        self.sendPacket(encoded)
        return d

    def fetchRequest(self, replicaId, maxWaitTime, minBytes, topics):
        apikey = 1
        apiversion = 0
        correlationid = next(self._nextCorrelationId)

        encoded = b''.join([
            struct.pack('>HHI', apikey, apiversion, correlationid),
            struct.pack('>III', replicaId, maxWaitTime, minBytes),
            encodeArray(topics, elementEncoder=_encodeTopicFetchRequest)
        ])

    def sendPacket(self, packet):
        prefixed = struct.pack('>I', len(packet)) + packet
        self._transport.write(prefixed)



def responder(f):
    @wraps(f)
    def _inner(self, *args, **kwargs):
        d = self._sender._waiting[self.correlationId]
        maybeDeferred(f, self, *args, **kwargs).chainDeferred(d)
    return _inner


class KafkaReceiver(object):
    currentRule = 'receiveResponse'
    def __init__(self, sender):
        self._sender = sender

    def prepareParsing(self, protocol):
        self.messageSize = None
        self.currentRule = 'receiveResponse'

    def finishParsing(self, reason):
        print 're', repr(reason)

    def foo(self, correlationId, msg):
        print 'foo', correlationId, repr(msg)

    def prepareReceiveMessage(self, size, correlationId):
        self.correlationId = correlationId
        req = self._sender._types.get(correlationId)
        self.currentRule = {'metadataRequest': 'metadataResponse'}.get(req, 'receiveUnknown')
        self.messageSize = size


    def receivedUnknown(self, data):
        print 'unk', repr(data)

    @responder
    def receivedMetadataResponse(self, brokers, topicmetadata):
        brokers = {nodeId: (host, port) for nodeId, host, port in brokers}
        topics = {}
        for topicErrorCode, topicName, partitionmetadata in topicmetadata:
            if topicErrorCode != 0:
                continue
            partitions = {}
            for partitionErrorCode, partitionId, leader, replicas, isr in partitionmetadata:
                if partitionErrorCode != 0:
                    continue
                partitions[partitionId] = (leader, replicas, isr)
            topics[topicName] = partitions
        return brokers, topics

    @responder
    def receivedProduceResponse(self, r):
        pass

    @responder
    def receivedFetchResponse(self, r):
        print 'r', r


grammar_source = open('txkafka.grammar').read()

def parseInt8(data):
    return struct.unpack('B', data)[0]

def parseInt16(data):
    return struct.unpack('>H', data)[0]

def parseInt32(data):
    return struct.unpack('>I', data)[0]

def parseInt64(data):
    return struct.unpack('>Q', data)[0]

bindings = {
    'parseInt8': parseInt8,
    'parseInt16': parseInt16,
    'parseInt32': parseInt32,
    'parseInt64': parseInt64
}

KafkaClientProtocol = makeProtocol(grammar_source, KafkaSender, KafkaReceiver,
    bindings=bindings)

@inlineCallbacks
def zkconnected(z):
    val, meta = yield z.get('/brokers/topics/test/partitions/0/state')

    broker = json.loads(val)['isr'][0]
    val, meta = yield z.get('/brokers/ids/%d' % (broker, ))
    val = json.loads(val)
    host, port = val['host'], val['port']
    ep = TCP4ClientEndpoint(reactor, host, port)
    proto = KafkaClientProtocol()
    yield connectProtocol(ep, proto)
    brokers, topics = yield proto.sender.metadataRequest(topicNames=['test'])

    test_zero_md = topics['test'][0]
    leader, replicas, isr = test_zero_md

    r = yield proto.sender.fetchRequest(replicaId=leader,
        maxWaitTime=0,
        minBytes=0,
        topics=[
            ('test', [(0, 0, 65535)])
        ])

log.startLogging(sys.stderr)
zk.connect().addCallback(zkconnected).addErrback(log.err)
reactor.run()