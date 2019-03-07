from zmon_worker_extras.check_plugins.datalake import DatalakeWrapper

from mock import MagicMock


def resp_mock(failure=False):
    resp = MagicMock()
    resp.ok = True if not failure else False
    json_res = {
        "id": "abc",
        "limit": 10000,
        "offset": 0,
        "result": [],
        "status": "FINISHED",
        "truncated": False
    } if not failure else {'message': "failed!", 'id': "def"}
    resp.json.return_value = json_res

    return resp


def requests_mock(resp, failure=None):
    req = MagicMock()

    if failure is not None:
        req.side_effect = failure
    else:
        req.return_value = resp

    return req


def test_datalake_query(monkeypatch):
    resp = resp_mock()
    request_history = requests_mock(resp)
    monkeypatch.setattr('requests.Session.get', request_history)
    monkeypatch.setattr('requests.Session.post', request_history)

    url = 'http://datalake'
    dl = DatalakeWrapper(url)

    q = 'SELECT * FROM dummy_table LIMIT 1'
    result = dl.query(query=q)

    assert result == resp.json.return_value

    request_history.assert_called_with('http://datalake/jobs/abc/output',
                                       json=q)
