#!/usr/bin/env python

import requests
import pandas as pd



def lcls_archiver_restore(pvlist, isotime='2018-08-11T10:40:00.000-07:00', verbose=True):
    """
    Returns a dict of {'pvname':val} given a list of pvnames, at a time in ISO 8601 format, using the EPICS Archiver Appliance:
    
    https://slacmshankar.github.io/epicsarchiver_docs/userguide.html
    
    
    """
    
    url="http://lcls-archapp.slac.stanford.edu/retrieval/data/getDataAtTime?at="+isotime+"&includeProxies=true"
    headers = {'Content-Type':'application/json'}
    
    if verbose:
        print('Requesting:', url)
    
    data = pvlist
    r = requests.post(url, headers=headers, json=data)
    
    if not r.ok:
        raise RuntimeError(f"Archiver request failed. Response was: {r.status_code} - {r.reason}")
    
    
    res = r.json()
    d = {}
    for k in pvlist:
        if k not in res:
            if verbose:
                print('Warning: Missing PV:', k)
        else:
            d[k] = res[k]['val']
    return d




def lcls_archiver_history(pvname:str, raise_error: bool = True,
                        start: str ='2018-08-11T10:40:00.000-07:00', 
                        end: str ='2018-08-11T11:40:00.000-07:00',
                        verbose=True):
    """
    Get time series data from a PV name pvname,
        with start and end times in ISO 8601 format, using the EPICS Archiver Appliance:
    
    https://slacmshankar.github.io/epicsarchiver_docs/userguide.html
    
    Returns tuple: 
        secs, vals
    where secs is the UNIX timestamp, seconds since January 1, 1970, and vals are the values at those times.
    
    Seconds can be converted to a datetime object using:
    import datetime
    datetime.datetime.utcfromtimestamp(secs[0])
    
    """
    url="http://lcls-archapp.slac.stanford.edu/retrieval/data/getData.json?"
    url += "pv="+pvname
    url += "&from="+start
    url += "&to="+end
    #url += "&donotchunk"
    #url="http://lcls-archapp.slac.stanford.edu/retrieval/data/getData.json?pv=VPIO:IN20:111:VRAW&donotchunk"
    print(url)

    #TODO: do some exception handling here so the code doesn't break if you access multiple pvs
    r = requests.get(url)

    if r.ok:
        data =  r.json()
        secs = [x['secs'] for x in data[0]['data']]
        vals = [x['val'] for x in data[0]['data']]
        return secs, vals
    
    msg = f"Archiver request failed for {pvname}. Response was: {r.status_code} - {r.reason}"
    if raise_error:
        raise RuntimeError(msg)

    print(msg)
    print("Returning Empty Lists")
    return [], []

def lcls_archiver_history_dataframe(
    pvname: str | list[str],
    **kwargs,
) -> pd.DataFrame:
    """
    Same as lcls_archiver_history, but returns a DataFrame with the index as time.
    Accepts a single PV name or a list of PV names.
    """

    # Normalize input
    pvs = [pvname] if isinstance(pvname, str) else pvname

    dfs = []

    for pv in pvs:
        secs, vals = lcls_archiver_history(pv, **kwargs)

        ser = pd.to_datetime(secs, unit="s")
        df = pd.DataFrame({pv: vals}, index=ser)
        df.index.name = "time"

        dfs.append(df)

    # Outer join keeps all timestamps if PVs differ
    return pd.concat(dfs, axis=1)


    