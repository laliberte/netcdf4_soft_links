#External:
import os, sys, string, subprocess
import getpass

#Internal:
from onlineca_get_trustroots_wget import onlineca_get_trustroots_wget
from onlineca_get_cert_wget import onlineca_get_cert_wget

def prompt_for_username_and_password(options):
    if (options.command=='certificates' and 
        'username' in dir(options) and options.username==None):
        options.username=raw_input('Enter Username: ')
        
    if 'username' in dir(options) and options.username!=None:
        if not options.password_from_pipe:
            options.password=getpass.getpass('Enter Credential phrase: ')
        else:
            timeout=1
            i,o,e=select.select([sys.stdin],[],[],timeout)
            if i:
                user_pass=sys.stdin.readline()
            else:
                print '--password_from_pipe selected but no password was piped. Exiting.'
                return
        retrieve_certificates(options.username,options.service,user_pass=options.password,trustroots=options.no_trustroots)
    else:
        options.password=None
    return options

def retrieve_certificates(username,registering_service,user_pass=None,trustroots=False):
    home=os.getenv('HOME')
    http_proxy=os.getenv('http_proxy')
    https_proxy=os.getenv('https_proxy')
    if http_proxy != None and https_proxy== None:
      print 'You have http_proxy set but not https_proxy: download tests are likely to fail'

    esgfdir='%s/.esg4' % home

    dods=True

    ee = { 'smhi':'esg-dn1.nsc.liu.se', 'pcmdi':'pcmdi9.llnl.gov', 'ipsl':'esgf-node.ipsl.fr', 'badc':'slcs.ceda.ac.uk',
    'dkrz':'esgf-data.dkrz.de', 'pik':'esg.pik-potsdam.de', 'jpl':'jpl-esg.jpl.nasa.gov' }
    dodsrc='%s/.dodsrc' % home

    registering_service = ee.get( registering_service, registering_service )

    for f in [esgfdir,esgfdir+'/certificates']:
      if not os.path.isdir( f ):
        os.mkdir( f )

    dodstext = """## generated by enesGetCert
HTTP.VERBOSE=0
HTTP.COOKIEJAR=.dods_cookies
HTTP.SSL.CERTIFICATE=%(esgfdir)s/credentials.pem
HTTP.SSL.KEY=%(esgfdir)s/credentials.pem
HTTP.SSL.CAPATH=%(esgfdir)s/certificates"""
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
      oo = open( '.dodsrc', 'w' )
      oo.write( dodstext % locals() )
      oo.close()

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

    return
    #port=MyProxyClient.PROPERTY_DEFAULTS['port']
    #lifetime=MyProxyClient.PROPERTY_DEFAULTS['proxyCertLifetime']
    #
    #if MyProxyClient.X509_CERT_DIR_ENVVARNAME in os.environ:
    #    cadir = os.environ[MyProxyClient.X509_CERT_DIR_ENVVARNAME]
    #else:
    #    cadir = os.path.join(
    #                    os.path.expanduser(MyProxyClient.USER_TRUSTROOT_DIR))
    #
    #client_props = dict(caCertDir=cadir,
    #                    hostname=registering_service,
    #                    port=port,
    #                    proxyCertLifetime=lifetime,
    #                    )
    #
    #myproxy = MyProxyClient(**client_props)
    #
    #creds = myproxy.logon(username, password,
    #                      bootstrap=True,
    #                      updateTrustRoots=False)
    #return

def test_certificates():
    home=os.getenv('HOME')
    http_proxy=os.getenv('http_proxy')
    https_proxy=os.getenv('https_proxy')
    if http_proxy != None and https_proxy== None:
      print 'You have http_proxy set but not https_proxy: download tests are likely to fail'

    esgfdir='%s/.esg4' % home

    print 'Testing certificate by running wget request in spider mode'
    cmd='wget -c -nH --certificate=%(esgfdir)s/credentials.pem --private-key=%(esgfdir)s/credentials.pem --save-cookies=%(esgfdir)s/cookies --load-cookies=%(esgfdir)s/cookies --ca-directory=%(esgfdir)s/certificates --no-check-certificate --spider   http://vesg.ipsl.fr/thredds/fileServer/esg_dataroot/CMIP5/output1/IPSL/IPSL-CM5A-LR/rcp85/mon/atmos/cfMon/r1i1p1/v20111119/clhcalipso/clhcalipso_cfMon_IPSL-CM5A-LR_rcp85_r1i1p1_200601-230012.nc  1> .wgsp 2> .wgspe' % locals()
    cmd2=['wget', '-c', '-nH', '--certificate=%(esgfdir)s/credentials.pem' %locals(), '--private-key=%(esgfdir)s/credentials.pem' %locals(), '--save-cookies=%(esgfdir)s/cookies' %locals(), '--load-cookies=%(esgfdir)s/cookies' %locals(), '--ca-directory=%(esgfdir)s/certificates' %locals(), '--no-check-certificate', '--spider', 'http://vesg.ipsl.fr/thredds/fileServer/esg_dataroot/CMIP5/output1/IPSL/IPSL-CM5A-LR/rcp85/mon/atmos/cfMon/r1i1p1/v20111119/clhcalipso/clhcalipso_cfMon_IPSL-CM5A-LR_rcp85_r1i1p1_200601-230012.nc' ] 
    if http_proxy != None:
      cmd = ( 'export http_proxy=%s ; export https_proxy=%s ;' % (http_proxy, https_proxy) ) + cmd
    print cmd
    subprocess.Popen( cmd2, env=os.environ.copy(), stdout=open('.wgsp','w'), stderr=open('.wgspe','w')  ).communicate()
    ##subprocess.Popen( cmd ).readlines()
    ii = open( '.wgspe' ).readlines()
    assert string.strip( ii[-2] ) in ['Remote file exists.','200 OK'] , 'File not found -- check .wgspe for error messages'
    os.unlink( '.wgsp' )
    os.unlink( '.wgspe' )

    print 'Check 1 OK'
    print '--------------------------------------------------'

    os.popen( 'ncdump -v 1> .nctmp 2> .nc2' ).readlines()
    ii = open( '.nc2' ).readlines()
    os.unlink( '.nctmp' )
    os.unlink( '.nc2' )
    try:
      x = string.split( string.split( ii[-1] )[3], '.' )
      maj = int(x[0])
      min = int(x[1])
      ncd = maj == 4 and min >= 1 or maj > 4
      if not ncd:
        print 'Netcdf libraries do not support opendap -- check 2 will not be completed'
    except:
      print 'Failed to identify ncdump version'
      ncd = False

    if ncd:
      print 'Testing certificate by requesting header of pr_day_HadGEM2-ES_esmControl_r1i1p1_20891201-20991130.nc'
      os.popen( 'ncdump -h http://cmip-dn1.badc.rl.ac.uk/thredds/dodsC/esg_dataroot/cmip5/output1/MOHC/HadGEM2-ES/esmControl/day/atmos/day/r1i1p1/v20120423/pr/pr_day_HadGEM2-ES_esmControl_r1i1p1_20891201-20991130.nc > .tmp ; md5sum .tmp > .md5' ).readlines()
      ii = open( '.md5' ).readlines()
      assert string.split(ii[0])[0] == '8f1d9ede885a527bcbdd1ddf1a5ed699', 'Checksum of header does not match expected value -- check .tmp and .md5 for clues'

      print 'Check 2 OK'
      print '--------------------------------------------------'

      print 'try: ncview http://cmip-dn1.badc.rl.ac.uk/thredds/dodsC/esg_dataroot/cmip5/output1/MOHC/HadGEM2-ES/esmControl/day/atmos/day/r1i1p1/v20120423/pr/pr_day_HadGEM2-ES_esmControl_r1i1p1_20891201-20991130.nc'
