#External:
import sys
import getpass

#Internal:
import certificates
import manage_soft_links_class
import manage_soft_links_parsers

def main():
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

    options=parser.parse_args()

    if options.command=='certificates':
        #certificates.retrieve_certificates(options.username,options.password,options.registering_service)
        if not options.password_from_pipe:
            user_pass=getpass.getpass('Enter Credential phrase:')
        else:
            user_pass=sys.stdin.readline()
        certificates.retrieve_certificates(options.username,options.service,user_pass=user_pass)
        #certificates.test_certificates()
    else:
        getattr(manage_soft_links_class,options.command)(options)
        
if __name__ == "__main__":
    main()
