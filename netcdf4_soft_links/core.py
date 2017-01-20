# External
import sys  # pragma: no cover

# Internal:
from . import commands, parsers, certificates  # pragma: no cover


def main():  # pragma: no cover
    nc4sl_from_list(sys.argv)


def nc4sl_from_list(args_list):  # pragma: no cover
    # Generate subparsers
    options = parsers.full_parser(args_list)

    options = certificates.prompt_for_username_and_password(options)

    getattr(commands, options.command)(options)


if __name__ == "__main__":  # pragma: no cover
    main()
