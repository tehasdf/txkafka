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


class KafkaSender(object):
    def __init__(self, transport):
        self._transport = transport


class KafkaReceiver(object):
    currentRule = ''
    def __init__(self, sender):
        self._sender = sender

    def prepareParsing(self, protocol):
        pass

    def finishParsing(self, reason):
        pass
grammar_source = """
"""
KafkaClientProtocol = makeProtocol(grammar_source, KafkaSender, KafkaReceiver)

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