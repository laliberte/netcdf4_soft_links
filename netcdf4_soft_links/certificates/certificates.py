#External:
import os, sys, string, subprocess
import getpass
import select
import pydap.lib

#External by related:
from netcdf4_pydap.cas.esgf import _get_node

#Internal:
from .onlineca_get_trustroots_wget import onlineca_get_trustroots_wget
from .onlineca_get_cert_wget import onlineca_get_cert_wget

def prompt_for_username_and_password(options):
    if (options.command=='certificates' and 
        'openid' in dir(options) and options.openid==None):
        options.openid=raw_input('Enter OpenID: ')

    if (('openid' in dir(options) and options.openid!=None) and
        get_node(options.openid)=='https://ceda.ac.uk'):
        if (options.command=='certificates' and 
            'username' in dir(options) and options.username==None):
            options.username=raw_input('Enter CEDA Username: ')
    else:
        if options.command=='certificates':
            raise InputError('Only OpenIDs from CEDA (starting with https://ceda.ac.uk) can \n\
                              be used to retrieve certificates.')
        elif ('use_certificates' in dir(options) and options.use_certificates):
            raise InputError('Only OpenIDs from CEDA (starting with https://ceda.ac.uk) can \n\
                              be used to retrieve certificates. Do not use --use_certificates.')
        
    if not 'password' in dir(options):
        options.password=None
    elif ( 'openid' in dir(options) and 
         options.openid!=None and
         options.password == None):

        if not options.password_from_pipe:
            options.password = getpass.getpass('Enter Credential phrase: ').strip()
        else:
            prompt_timeout=1
            i,o,e=select.select([sys.stdin],[],[],prompt_timeout)
            if i:
                options.password = sys.stdin.readline().strip()
            else:
                print('--password_from_pipe selected but no password was piped. Exiting.')
                return

    #Retrieve certificates or set dods_conf:
    if (('use_certificates' in dir(options) and options.use_certificates) 
         or  options.command=='certificates'):
        registering_service='ceda'
        if 'username' in dir(options) and options.username!=None:
            retrieve_certificates(options.username, registering_service, user_pass=options.password,
                                  trustroots=options.no_trustroots,
                                  timeout=options.timeout)
        else:
            retrieve_certificates(None, registering_service)
    return options

def retrieve_certificates(username,registering_service,user_pass=None,trustroots=False,timeout=120):
    home=os.getenv('HOME')

    dods=True
    dodsrc='%s/.dodsrc' % home
    esgfdir='%s/.esg4' % home
    if username==None and dods:
      #Set the environment:
      os.environ['DODS_CONF']=dodsrc
      #This modifies pydap. All three lines must be loaded in order:
      #pydap.lib.CACHE=esgfdir+'/dods_cache'
      #import pydap_esgf
      #pydap_esgf.install_esgf_client(os.environ['X509_USER_PROXY'],os.environ['X509_USER_PROXY'])
      return

    http_proxy=os.getenv('http_proxy')
    https_proxy=os.getenv('https_proxy')
    if http_proxy != None and https_proxy== None:
      print('You have http_proxy set but not https_proxy: download tests are likely to fail')

    ee = { 'smhi':'esg-dn1.nsc.liu.se', 'pcmdi':'pcmdi9.llnl.gov', 'ipsl':'esgf-node.ipsl.fr', 'ceda':'slcs.ceda.ac.uk',
    'dkrz':'esgf-data.dkrz.de', 'pik':'esg.pik-potsdam.de', 'jpl':'jpl-esg.jpl.nasa.gov' }

    registering_service = ee.get( registering_service, registering_service )

    for f in [esgfdir,esgfdir+'/certificates',esgfdir+'/dods_cache']:
      if not os.path.isdir( f ):
        os.mkdir( f )

    dodstext = """## generated by enesGetCert
HTTP.COOKIEJAR=%(esgfdir)s/dods_cache/.dods_cookies
HTTP.SSL.CERTIFICATE=%(esgfdir)s/credentials.pem
HTTP.SSL.KEY=%(esgfdir)s/credentials.pem
HTTP.SSL.CAPATH=%(esgfdir)s/certificates
HTTP.TIMEOUT=%(timeout)s"""
#CURL.VERBOSE=0
#CURL.COOKIEJAR=.dods_cookies
#CURL.SSL.CERTIFICATE=%(esgfdir)s/credentials.pem
#CURL.SSL.KEY=%(esgfdir)s/credentials.pem
#CURL.SSL.CAPATH=%(esgfdir)s/certificates

    if dods:
      oo = open( dodsrc, 'w' )
      oo.write( dodstext % locals() )
      oo.close()
      #Temporary fix: write a local .dodsrc
      #oo = open( '.dodsrc', 'w' )
      #oo.write( dodstext % locals() )
      #oo.close()


    oo = open(esgfdir+'/onlineca-get-trustroots-wget.sh','w')
    oo.write(onlineca_get_trustroots_wget())
    oo.close()

    if trustroots:
        #Do not have to renew frequently
        call_to_script=['bash',esgfdir+'/onlineca-get-trustroots-wget.sh','-b','-U','https://'+registering_service+'/onlineca/trustroots/','-c',esgfdir+'/certificates']
        #print ' '.join(call_to_script)
        subprocess.call(call_to_script)

    oo = open(esgfdir+'/onlineca-cert-wget.sh','w')
    oo.write(onlineca_get_cert_wget())
    oo.close()
    call_to_script=['bash',esgfdir+'/onlineca-cert-wget.sh','-l',username,'-U','https://'+registering_service+'/onlineca/certificate/','-c',esgfdir]
    #print ' '.join(call_to_script)
    if user_pass!=None:
        call_to_script.append('-S')
        p=subprocess.Popen(call_to_script,stdout=subprocess.PIPE,stdin=subprocess.PIPE)
        stdout=p.communicate(input=user_pass)[0]
        p.stdin.close()
    else:
        subprocess.call(call_to_script)

    #Remove cookies for a clean slate:
    try:
        os.remove(esgfdir+'/dods_cache/.dods_cookies')
    except:
        pass
    return
