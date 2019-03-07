from zmon_worker_monitor.builtins.plugins.datalake import DatalakeWrapper
from zmon_worker_monitor.zmon_worker.common.http import get_user_agent

from mock import MagicMock


def resp_mock(failure=False):
    resp = MagicMock()
    resp.ok = True if not failure else False
    json_res = {
        "id": "abc",
        "limit": 10000,
        "offset": 0,
        "result": [],
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
    get = requests_mock(resp)
    monkeypatch.setattr('requests.get', get)

    url = 'http://datalake/'
    dl = DatalakeWrapper(url)

    q = 'SELECT * FROM dummy_table LIMIT 1'
    result = dl.query(query=q)

    assert result == resp.json.return_value

    get.assert_called_with('http://datalake/jobs',
                           headers={'User-Agent': get_user_agent()},
                           params={'query': q},
                           timeout=10)
