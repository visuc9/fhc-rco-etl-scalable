import atexit
import json
import os
import sys
import msal


def get_sharepoint_token(cache_file, client_id, authority_url, scopes) -> str:
    cache = msal.SerializableTokenCache()
    if os.path.exists(cache_file):
        cache.deserialize(open(cache_file, 'r').read())
    atexit.register(
        lambda: open(
            cache_file, 'w'
        ).write(
            cache.serialize()
        ) if cache.has_state_changed else None
    )
    app = msal.PublicClientApplication(
        client_id=client_id,
        authority=authority_url,
        token_cache=cache
    )
    az_auth_result = None
    accounts = app.get_accounts()
    if accounts:
        az_auth_result = app.acquire_token_silent(
            scopes=scopes,
            account=accounts[0]
        )
    else:
        print('No accounts stored, so you will have to auth/consent.')

    if not az_auth_result:
        flow = app.initiate_device_flow(scopes=scopes)
        if 'user_code' not in flow:
            raise ValueError(
                "Couldn't create device flow. %s" % json.dumps(flow, indent=2)
            )
        print(flow['message'])
        sys.stdout.flush()
        az_auth_result = app.acquire_token_by_device_flow(flow)

    if 'access_token' in az_auth_result:
        return az_auth_result['access_token']
    else:
        print(az_auth_result.get("error"))
        print(az_auth_result.get("error_description"))
        print(az_auth_result.get("correlation_id"))
        return None
