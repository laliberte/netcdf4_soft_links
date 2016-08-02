import mechanize
import cookielib
import urllib
import ssl

def cookieJar(dest_url,openid,password):
    '''
    Retrieve ESGF cookies using mechanize and by calling the right url.
    This function might be sensitive to a future evolution of the ESGF security.
    '''
    dest_node='/'.join(dest_url.split('/')[:3]).replace('http:','https:')

    br = mechanize.Browser()
    cj = cookielib.LWPCookieJar()
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

    base_url=dest_node+'/esg-orp/j_spring_openid_security_check.htm?openid_identifier='+urllib.quote_plus(openid)

    #Do not verify certificate (we do not worry about MITM)
    ssl._https_verify_certificates(False)
    r = br.open(base_url)
    html = r.read()

    br.select_form(nr=0)

    br.form['password']=password
    br.submit()
    #Restore certificate verification
    ssl._https_verify_certificates(True)
    return cj
