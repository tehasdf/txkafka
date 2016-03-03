from __future__ import division

from binascii import crc32
from functools import wraps
from itertools import count
import json
import struct
import sys

from twisted.logger import Logger, globalLogBeginner, textFileLogObserver
from twisted.internet.defer import Deferred, inlineCallbacks, maybeDeferred
from twisted.internet.task import react
from twisted.internet.protocol import Protocol, Factory
from twisted.internet.endpoints import TCP4ClientEndpoint, connectProtocol

from txzookeeper import ZookeeperClient

zk = ZookeeperClient(servers='localhost:2181')


from parsley import makeProtocol


log = Logger()

def encodeString(string):
    assert isinstance(string, bytes)
    return struct.pack('>h', len(string)) + string

def encodeBytes(string):
    assert isinstance(string, bytes)
    return struct.pack('>i', len(string)) + string

def encodeArray(seq, elementEncoder=lambda elem: elem):
    prefix = struct.pack('>i', len(seq))
    return prefix + b''.join(elementEncoder(e) for e in seq)

def _encodeTopicFetchRequest(elem):

    def _encodeTopicMeta(meta):
        partition, offset, maxbytes = meta
        return struct.pack('>iqi', partition, offset, maxbytes)

    name, meta = elem

    return b''.join([
        encodeString(name),
        encodeArray(meta, elementEncoder=_encodeTopicMeta)
    ])


def _encodeMessageSet(messages):
    all_encoded = []
    for message in messages:
        offset, magic, key, value = message
        attrs = 0
        message_bytes = b''.join([
            struct.pack('>bb', magic, attrs),
            '\xff\xff\xff\xff' if key is None else encodeBytes(key),
            encodeBytes(value)
        ])
        encoded = struct.pack('>qii',
            offset,
            len(message_bytes) + 4,  # add 4 for crc
            crc32(message_bytes)
        ) + message_bytes

        all_encoded.append(encoded)

    return b''.join(all_encoded)


def _encodeProducePartition(partitionData):
    partition, messages = partitionData
    mset = _encodeMessageSet(messages)
    msetSize = len(mset)

    return b''.join([
        struct.pack('>ii', partition, msetSize),
        mset
    ])

def _encodeProduceTopic(topic):
    name, partition_messages = topic
    return b''.join([
        encodeString(name),
        encodeArray(partition_messages, elementEncoder=_encodeProducePartition)
    ])

class KafkaSender(object):
    log = Logger()

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
            struct.pack('>hhi', apikey, apiversion, correlationid),
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
            struct.pack('>hhi', apikey, apiversion, correlationid),
            encodeString('asdf'),
            struct.pack('>iii', replicaId, maxWaitTime, minBytes),
            encodeArray(topics, elementEncoder=_encodeTopicFetchRequest)
        ])
        d = Deferred()
        self._waiting[correlationid] = d
        self._types[correlationid] = 'fetchRequest'
        self.sendPacket(encoded)
        return d

    def produceRequest(self, requiredAcks, timeout, topics):
        apikey = 0
        apiversion = 0
        correlationid = next(self._nextCorrelationId)

        encoded = b''.join([
            struct.pack('>hhi', apikey, apiversion, correlationid),
            encodeString('asdf'),
            struct.pack('>hi', requiredAcks, timeout),
            encodeArray(topics, elementEncoder=_encodeProduceTopic)
        ])


    def sendPacket(self, packet):
        self.log.debug('Sending packet: {length} bytes', length=len(packet))
        prefixed = struct.pack('>I', len(packet)) + packet
        self._transport.write(prefixed)



def responder(f):
    @wraps(f)
    def _inner(self, *args, **kwargs):
        d = self._sender._waiting[self.correlationId]
        (maybeDeferred(f, self, *args, **kwargs)
            .chainDeferred(d)
            .addBoth(self.cleanup)
        )
    return _inner

from ometa.grammar import OMeta
from ometa.tube import TrampolinedParser

class MessageSetReceiver(object):
    currentRule = 'receiveMessageSetPart'

    def __init__(self, consumer=None):
        self._messages = []
        self._consumer = consumer

    def receiveMessage(self, offset, m):
        crc, magicByte, attributes, key, value = m
        self._messages.append((key, value))
        if self._consumer:
            self.onMessageReceived(offset, key, value)

    def messageSetReceived(self):
        if self._consumer:
            self.onMessageSetReceived(self._messages)


class KafkaReceiver(object):
    currentRule = 'receiveResponse'
    log = Logger()

    def __init__(self, sender):
        self._sender = sender
        self._messageSetGrammar = OMeta(grammar_source).parseGrammar('messageSetGrammar')
        self._parsers = {}


    def _getMessageSetParser(self, topicName):
        if topicName not in self._parsers:
            receiver = MessageSetReceiver()
            self._parsers[topicName] = TrampolinedParser(grammar=self._messageSetGrammar,
            receiver=receiver, bindings=globalBindings)
        return self._parsers[topicName]

    def prepareParsing(self, protocol):
        self.messageSize = None
        self.currentRule = 'receiveResponse'

    def finishParsing(self, reason):
        pass

    def prepareReceiveMessage(self, size, correlationId):
        self.correlationId = correlationId
        req = self._sender._types.get(correlationId)
        self.currentRule = {
            'metadataRequest': 'metadataResponse',
            'fetchRequest': 'fetchResponse'

        }.get(req, 'receiveUnknown')
        log.debug('Now receiving {type}', type=self.currentRule)

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
    def receivedFetchResponse(self, responses):
        for topicName, topicDetails in responses:
            # XXX it needs to also use separate parsers for each partition
            # and maybe check that offsets are ok?
            messageSetParser = self._getMessageSetParser(topicName)
            for partition, errorCode, highwaterMarkOffset, part in topicDetails:
                if errorCode != 0:
                    raise ValueError(
                        'Topic fetch error: {topic}/{partition}: {error}'.format(
                        topic=topicName, partition=partition, error=errorCode))
                messageSetParser.receive(part)

    def cleanup(self, response):
        self.messageSize = None
        self.currentRule = 'receiveResponse'
        self.correlationId = None


grammar_source = open('txkafka.grammar').read()

def parseInt8(data):
    return struct.unpack('b', data)[0]

def parseInt16(data):
    return struct.unpack('>h', data)[0]

def parseInt32(data):
    return struct.unpack('>i', data)[0]

def parseInt64(data):
    return struct.unpack('>q', data)[0]

receivedLogger = Logger('Receiving')
def _doLog(a):
    receivedLogger.debug('{a!r}', a=a)
    return a

globalBindings = {
    'parseInt8': parseInt8,
    'parseInt16': parseInt16,
    'parseInt32': parseInt32,
    'parseInt64': parseInt64,
    'log': _doLog
}

KafkaClientProtocol = makeProtocol(grammar_source, KafkaSender, KafkaReceiver,
    bindings=globalBindings)


@inlineCallbacks
def zkconnected(z, reactor):
    val, meta = yield z.get('/brokers/topics/test/partitions/0/state')

    broker = json.loads(val)['isr'][0]
    val, meta = yield z.get('/brokers/ids/%d' % (broker, ))
    val = json.loads(val)
    host, port = val['host'], val['port']
    ep = TCP4ClientEndpoint(reactor, host, port)
    proto = KafkaClientProtocol()
    yield connectProtocol(ep, proto)
    brokers, topics = yield proto.sender.metadataRequest(topicNames=['test'])
    log.debug('Brokers: {brokers!r}', brokers=brokers)
    test_zero_md = topics['test'][0]
    leader, replicas, isr = test_zero_md

    r = yield proto.sender.fetchRequest(replicaId=-1,
        maxWaitTime=10,
        minBytes=0,
        topics=[
            ('test', [(0, 0, 65535)])
        ])


def main(reactor):
    globalLogBeginner.beginLoggingTo([textFileLogObserver(sys.stderr)])
    return zk.connect().addCallback(zkconnected, reactor)

if __name__ == '__main__':
    react(main)
