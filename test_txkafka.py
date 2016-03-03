import pytest



import parsley
from txkafka import globalBindings, _encodeMessageSet


@pytest.fixture(scope='session')
def grammar_source():
    with open('txkafka.grammar') as f:
        return f.read()

@pytest.fixture(scope='session')
def parser(grammar_source):
    return parsley.makeGrammar(grammar_source, globalBindings)


@pytest.mark.parametrize('receivedBytes,parsed', [
    ('\x00\x00\x00\x00', ''),
    ('\x00\x00\x00\x01a', 'a'),
    ('\x00\x00\x00\x02aa', 'aa'),
])
def test_bytes_parsing(parser, receivedBytes, parsed):
    assert parser(receivedBytes).bytes() == parsed

EXAMPLE_MESSAGESET = b''.join([
    "\x00\x00\x00\x00\x00\x00\x00\x00",  # int64:offset
    "\x00\x00\x00\x17",  # int32: messageSize  = 23 bytes
    # first message:
        "\xfd\xf9-\x06",  # int32:crc
        "\x00",  # int8:magicByte
        "\x00",  # int8:attributes
        "\xff\xff\xff\xff", # bytes:key
        "\x00\x00\x00\t", "message 1",  # bytes:value

    "\x00\x00\x00\x00\x00\x00\x00\x01",  # int64:offset
    "\x00\x00\x00\x17",  # int32:messageSize  = 23 bytes
    # second message:
        "d\xf0|\xbc",  # int32:crc
        "\x00",  # int8:magicByte
        "\x00",  # int8:attributes
        "\xff\xff\xff\xff",  # bytes:key
        "\x00\x00\x00\t", "message 2"
])

def test_parse_messageset(parser):

    messages = parser(EXAMPLE_MESSAGESET).messageSet()
    assert len(messages) == 2
    offset, m1 = messages[0]
    assert offset == 0
    crc, magic, attrs, key, val = m1
    assert key is None
    assert val == 'message 1'


def test_encode_messageset():
    messages = [(0, 0, None, 'message 1'), (1, 0, None, 'message 2')]
    encoded = _encodeMessageSet(messages)
    assert encoded == EXAMPLE_MESSAGESET
