import socket


def is_int(data):
    try:
        int(data)
        return True
    except TypeError:
        return False


def ping(host):
    '''
    :param host: url in "site.com:443" style
    :return: ping result
    '''
    host = host.replace('http://', '').replace('https://', '')

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(1)
    host_data = host.split(':')
    if len(host_data) == 1:
        port = 80
    elif len(host_data) == 2:
        if not is_int(host_data[1]):
            raise ValueError(f'Host port is not int: {host_data[1]}')
        host = host_data[0]
        port = int(host_data[1])
    else:
        raise ValueError(f'Wrong host format: {host}')

    try:
        s.connect((host, int(port)))
        s.shutdown(2)
        return True
    except ConnectionError as e:
        return e
    finally:
        try:
            s.shutdown(socket.SHUT_RDWR)
            s.close()
        except OSError:
            pass
