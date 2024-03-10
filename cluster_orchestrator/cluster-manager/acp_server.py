def unpack(buf):
    l1 = buf[0]
    l2 = buf[1]
    l3 = buf[2]
    l4 = buf[3]
    return (l1<<24) + (l2<<16) + (l3<<8) + l4

def pack(seq):
    n1 = seq>>24
    n2 = (seq>>16) & 0xff
    n3 = (seq>>8) & 0xff
    n4 = seq & 0xff

    return chr(n1) + chr(n2) + chr(n3) + chr(n4)


def package(seq):
    data = bytearray(1024)  # Creates a bytearray of size 1024 initialized with zeros

    n1 = (seq >> 24)
    n2 = (seq >> 16) & 0xff
    n3 = (seq >> 8) & 0xff
    n4 = seq & 0xff

    data[0] = n1
    data[1] = n2
    data[2] = n3
    data[3] = n4

    return data

def process_resp(resp):
    seq = unpack(resp)
    return package(seq)
