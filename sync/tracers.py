import textwrap


def request_tracer(response, *args, **kwargs) -> None:
    """
    A hook function to print-out request from the requests module.
    """
    headers = '\n'.join(
        f'{key}: {value}'
        for key, value in response.request.headers.items()
    )

    print(textwrap.dedent(
        '''
        ---------------- REQUEST -----------------
        {req.method} {req.url}
        {headers}

        {req.body}
        '''
    ).format(
        req=response.request,
        headers=headers,
    ))


def response_tracer(response, *args, **kwargs) -> None:
    """
    A hook function to print-out response from the requests module.
    """
    headers = '\n'.join(
        f'{key}: {value}'
        for key, value in response.headers.items()
    )

    print(textwrap.dedent(
        '''
        ---------------- RESPONSE ----------------
        {res.status_code} {res.reason} {res.url}
        {headers}

        {body}
        '''
    ).format(
        res=response,
        headers=headers,
        body=response.json()
    ))
