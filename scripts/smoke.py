#!/usr/bin/env python3
"""Проверка запущенного демо. Сбрасывает его данные; внешних отправок нет."""
import argparse
import json
import sys
import urllib.error
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://127.0.0.1:8080')
    args = parser.parse_args()
    base = args.url.rstrip('/')

    def call(method, path, body=None, expected=200):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(base + path, data=data, method=method,
                                     headers={'Content-Type': 'application/json'})
        try:
            response = urllib.request.urlopen(req, timeout=15)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            payload = response.read()
            assert response.code == expected, (method, path, response.code, payload)
            if response.headers.get_content_type() == 'application/json':
                return json.loads(payload)
            return payload.decode()

    assert call('GET', '/healthz')['status'] == 'ok'
    call('DELETE', '/api/demo')
    cases = {
        'push-success': ('DELIVERED', 'push', 1),
        'failover': ('DELIVERED', 'sms', 2),
        'late-ack': ('DELIVERED', 'sms', 2),
        'retry': ('DELIVERED', 'sms', 1),
        'expired': ('EXPIRED', None, 1),
        'all-failed': ('FAILED', None, 0),
        'email-only': ('SENT', 'email', 1),
    }
    for name, (state, via, count) in cases.items():
        result = call('POST', '/api/demo/scenarios/' + name)
        assert result['notification']['state'] == state, result
        assert result['notification'].get('delivered_via') == via, result
        assert len(result['provider_submissions']) == count, result
        assert not result['timers'] and not result['pending_retries'], result
        print('OK scenario:', name)

    body = {'idempotency_key': 'smoke-manual', 'user_id': 42,
            'simulation': {'push': 'silent', 'sms': 'silent'}}
    first = call('POST', '/api/notifications', body, expected=201)
    repeated = call('POST', '/api/notifications', body)
    notification_id = first['notification']['id']
    assert repeated['notification']['id'] == notification_id
    call('POST', '/api/notifications', dict(body, user_id=43), expected=409)
    call('POST', '/api/demo/advance', {'seconds': 10})
    path = '/api/notifications/' + notification_id
    result = call('POST', path + '/replay')
    assert result['notification']['state'] == 'SENDING'
    assert result['notification']['step_idx'] == 1
    assert len(result['provider_submissions']) == 2, result
    call('POST', path + '/receipts', {'step_idx': 1, 'channel': 'sms', 'status': 'DELIVERED'})
    ack = {'step_idx': 0, 'channel': 'push', 'status': 'DELIVERED'}
    result = call('POST', path + '/receipts', ack)
    repeated = call('POST', path + '/receipts', ack)
    assert len(result['events']) == len(repeated['events']), repeated
    assert len(repeated['provider_submissions']) == 2
    assert 'notification_demo_cross_channel_duplicates_total 2' in call('GET', '/metrics')
    assert call('GET', '/openapi.json')['openapi'] == '3.0.3'
    assert 'SwaggerUIBundle' in call('GET', '/swagger/')
    print('OK manual flow: idempotency, conflict, timeout, replay, DLR, late ACK, metrics')
    print('All HTTP smoke checks passed.')


if __name__ == '__main__':
    try:
        main()
    except (AssertionError, OSError, ValueError) as exc:
        print('FAIL:', exc, file=sys.stderr)
        sys.exit(1)
