#External:
import sys
import getpass

#Internal:
import certificates
import manage_soft_links_class
import manage_soft_links_parsers

def main(string_call=None,queues=dict(),semaphores=dict()):
    import argparse 
    import textwrap

    #Option parser
    version_num='0.1'
    description=textwrap.dedent('''\
    This script aggregates soft links to OPENDAP or local files.\
    ''')

    epilog='Version {0}: Frederic Laliberte, Paul Kushner 01/2016\n\
\n\
If using this code to process data from the ESGF please cite:\n\n\
Efficient, robust and timely analysis of Earth System Models: a database-query approach (2015):\n\
F. Laliberté, Juckes, M., Denvil, S., Kushner, P. J., TBD, Submitted.'.format(version_num)
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
                            description=description,
                            version='%(prog)s '+version_num,
                            epilog=epilog)

    #Generate subparsers
    manage_soft_links_parsers.generate_subparsers(parser,epilog,None)

    if string_call != None:
        options=parser.parse_args(string_call)
    else:
        options=parser.parse_args()

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
        certificates.retrieve_certificates(options.username,options.service,user_pass=options.password,trustroots=options.no_trustroots)
    else:
        options.password=None

    if options.command!='certificates':
        getattr(manage_soft_links_class,options.command)(options,queues,semaphores)
        
if __name__ == "__main__":
    main()
