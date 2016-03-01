from __future__ import division

from itertools import count
import json
import struct
import sys
from twisted.internet import reactor

from twisted.internet.defer import Deferred, inlineCallbacks
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

        encoded = b''.join([
            struct.pack('>III')
        ])

    def sendPacket(self, packet):
        prefixed = struct.pack('>I', len(packet)) + packet
        print 'sending', repr(prefixed)
        self._transport.write(prefixed)


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
        req = self._sender._types.get(correlationId)
        self.currentRule = {'metadataRequest': 'metadataResponse'}.get(req, 'receiveUnknown')
        self.messageSize = size


    def receivedUnknown(self, data):
        print 'unk', repr(data)

    def receivedMetadataResponse(self, a, b):
        print 'mr', a, b

    def receivedProduceResponse(self, r):
        pass

    def receivedFetchResponse(self, r):
        pass


grammar_source = """
int8 = anything:d -> parseInt8(d)
int16 = <anything{2}>:d -> parseInt16(d)
int32 = <anything{4}>:d -> parseInt32(d)
int64 = <anything{8}>:d -> parseInt64(d)
string = int16:size <anything{size}>:data -> data
bytes = int32:size <anything{size}>:data -> data
int32list = int32:num int32{num}:nums -> nums

receiveResponse = (int32:s -> s-4):size int32:correlationId -> receiver.prepareReceiveMessage(size, correlationId)


broker = int32:nodeId string:host int32:port -> (nodeId, host, port)
brokerList = int32:num broker{num}:brokers -> brokers

partitionMetadata = int16:partitionErrorCode int32:partitionId int32:leader int32list:replicas int32list:isr -> (partitionErrorCode, partitionId, leader, replicas, isr)
partitionMetadataList = int32:num partitionMetadata{num}:partitions -> partitions

topicMetadata = int16:topicErrorCode string:topicName partitionMetadataList:partitionMetadata -> (topicErrorCode, topicName, partitionMetadata)
topicMetadataList = int32:num topicMetadata{num}:topics -> topics

metadataResponse = brokerList:brokers topicMetadataList:topicMetadata -> receiver.receivedMetadataResponse(brokers, topicMetadata)

partitionProduceResponse = int32:partition int16:errorCode int64:offset -> (partition, errorCode, offset)
partitionProduceResponseList = int32:num partitionProduceResponse{num}:l -> l
produceResponseItem = string:topicName partitionProduceResponseList:l -> (topicName, l)
produceResponse = int32:num produceResponseItem{num}:responses -> receiver.receivedProduceResponse(responses)  # not tested


message = int32:crc int8:magicByte int8:attributes int64:timestamp bytes:key bytes:value -> (crc, magicByte, attributes, timestamp, key, value)
messageSet = int64:offset int32:messageSize message:m -> (offset, m)
topicResponseItem = int32:partition int16:errorCode int64:highwaterMarkOffset int32:messageSetSize messageSet:messages -> (partition, errorCode, highwaterMarkOffset, messages)
fetchResponseItem = string:topicName int32:num topicResponseItem{num}:topicDetails -> (topicName, topicDetails)
fetchResponse = int32:num fetchResponseItem{num}:responses -> receiver.receivedFetchResponse(responses)  # not tested

receiveUnknown = ( -> receiver.messageSize):size  <anything{size}>:data -> receiver.receivedUnknown(data)
"""

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
    md = yield proto.sender.metadataRequest(topicNames=['test'])
    # leader = md.topics['test'].partitions[0].leader
    # broker = md.brokers[leader].host, md.brokers[leader].port
    # proto.fetchRequest('test', 0, 0)

log.startLogging(sys.stderr)
zk.connect().addCallback(zkconnected).addErrback(log.err)
reactor.run()