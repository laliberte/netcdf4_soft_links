#External:
import sys

#Internal:
import certificates
import manage_soft_links_class
import manage_soft_links_parsers

def main(string_call=None):
    import argparse 
    import textwrap

    #Option parser
    version_num='0.5'
    description=textwrap.dedent('''\
    This script aggregates soft links to OPENDAP or local files.\
    ''')
    epilog='Version {0}: Frederic Laliberte (03/2016),\n\
Previous versions: Frederic Laliberte, Paul Kushner (2011-2015).\n\
\n\
If using this code to retrieve and process data from the ESGF please cite:\n\n\
Efficient, robust and timely analysis of Earth System Models: a database-query approach (2016):\n\
F. Laliberte, Juckes, M., Denvil, S., Kushner, P. J., TBD, Submitted.'.format(version_num)
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

    options=certificates.prompt_for_username_and_password(options)

    if options.command!='certificates':
        getattr(manage_soft_links_class,options.command)(options)
        
if __name__ == "__main__":
    main()
