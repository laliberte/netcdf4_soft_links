#External:
import sys

#Internal:
from .certificates import certificates
from . import commands, parsers

def main(string_call=None):
    import argparse 
    import textwrap

    #Generate subparsers
    parsers.generate_subparsers(None)

    if string_call != None:
        options=parser.parse_args(string_call)
    else:
        options=parser.parse_args()

    options=certificates.prompt_for_username_and_password(options)

    if options.command!='certificates':
        getattr(commands,options.command)(options)
        
if __name__ == "__main__":
    main()
