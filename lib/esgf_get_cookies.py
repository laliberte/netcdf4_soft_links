import mechanize
import cookielib
import urllib
import ssl
import warnings
import requests

def cookieJar(dest_url,openid,password,username=None,authentication_url='ESGF'):
    '''
    Retrieve ESGF cookies using mechanize and by calling the right url.
    This function might be sensitive to a future evolution of the ESGF security.
    '''
    dest_node=get_node(dest_url)

    br = mechanize.Browser()
    cj = cookielib.LWPCookieJar()
    if openid==None or openid=='':
        warnings.warn('openid was not set. this was likely unintentional but will result is much fewer datasets.')
        return cj
    if password==None or password=='':
        warnings.warn('password was not set. this was likely unintentional but will result is much fewer datasets.')
        return cj

    br.set_cookiejar(cj)

    # Browser options
    br.set_handle_equiv(True)
    br.set_handle_redirect(True)
    br.set_handle_referer(True)
    br.set_handle_robots(False)

    # Follows refresh 0 but not hangs on refresh > 0
    br.set_handle_refresh(mechanize._http.HTTPRefreshProcessor(), max_time=1)

    # Want debugging messages?
    #br.set_debug_http(True)
    #br.set_debug_redirects(True)
    #br.set_debug_responses(True)

    br.addheaders = [('User-agent', 'Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.1) Gecko/2008071615 Fedora/3.0.1-1.fc9 Firefox/3.0.1')]

    if authentication_url == 'ESGF':
        base_url = dest_node+'/esg-orp/j_spring_openid_security_check.htm?openid_identifier='+urllib.quote_plus(openid)
    else:
        base_url = authentication_url 

    #Do not verify certificate (we do not worry about MITM)
    ssl._https_verify_certificates(False)
    r = br.open(base_url)
    html = r.read()

    br.select_form(nr=0)

    if authentication_url == 'ESGF':
        #ESGF base_url contains openid:
        if get_node(openid)=='https://ceda.ac.uk':
            if username==None:
                raise InputError('OpenIDs from CEDA (starting with https://ceda.ac.uk) require a username and none were provided.')
            br.form['username']=username
    else:
        #Assume other auth do not:
        br.form['username'] = openid

    try:
        br.form['password']=password
    except mechanize._form.ControlNotFoundError:
        br.close()
        raise InputError('Navigate to {0}. '
                         'If you are unable to login, you must either wait or use an OPENID from another node.')

    r=br.submit()

    if authentication_url == 'ESGF':
        if get_node(openid)=='https://ceda.ac.uk':
            #CEDA has an extra form to submit:
            br.select_form(nr=0)
            r=br.submit()
            html=r.read()
        br.close()

        resp = requests.get(dest_url,cookies=cj)
        if resp.status_code==403:
            #The user has not registered with a usage category:
            raise Exception('The kind of user must be selected. '
                            'To do so, navigate to {0}, log in using your openid '
                            'and select the group you belong to. '
                            'Agree to the terms and do NOT download data. '
                            'This has to be done only once per project.'.format(dest_url))
        resp.close()
    #else:
    #    #Test for registration...
    #    resp = requests.get(dest_url,cookies=cj)
    #    resp.close()

    #Restore certificate verification
    ssl._https_verify_certificates(True)
    return cj

def get_node(url):
        return '/'.join(url.split('/')[:3]).replace('http:','https:')
