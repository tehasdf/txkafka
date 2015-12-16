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

    def metadataRequest(self, topics=None):
        if topics is None:
            topics = []
        apikey = 3
        apiversion = 0
        correlationid = next(self._nextCorrelationId)
        encoded = b''.join([
            struct.pack('>HHI', apikey, apiversion, correlationid),
            encodeString('asdf'),
            encodeArray(topics, elementEncoder=encodeString)
        ])
        d = Deferred()
        self._waiting[correlationid] = d
        self.sendPacket(encoded)
        return d

    def sendPacket(self, packet):
        prefixed = struct.pack('>I', len(packet)) + packet
        print 'sending', repr(prefixed)
        self._transport.write(prefixed)


class KafkaReceiver(object):
    currentRule = 'receiveResponse'
    def __init__(self, sender):
        self._sender = sender

    def prepareParsing(self, protocol):
        pass

    def finishParsing(self, reason):
        print 're', reason



grammar_source = """

int32 = <anything{4}>:d -> parseInt32(d)
responseMessage = <anything>*
receiveResponse = int32:size int32:correlationId responseMessage:msg
"""


def parseInt32(data):
    return struct.unpack('>I', data)[0]

bindings = {
    'parseInt32': parseInt32
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
    print 'asd', host, port
    ep = TCP4ClientEndpoint(reactor, host, port)
    proto = KafkaClientProtocol()
    yield connectProtocol(ep, proto)
    print 'asd', dir(proto)
    md = yield proto.sender.metadataRequest(topics=['test'])
    leader = md.topics['test'].partitions[0].leader
    broker = md.brokers[leader].host, md.brokers[leader].port
    print 'aa', broker
    proto.fetchRequest('test', 0, 0)
    print 'xd'

log.startLogging(sys.stderr)
zk.connect().addCallback(zkconnected).addErrback(log.err)
reactor.run()