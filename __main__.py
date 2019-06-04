"""
Library to hold sheet processing for arbitrary grouping of salary and finances
"""
import logging
import sys
import argparse
import json
from pathlib import Path
from .openpyxl_adapter import OpenpyxlDataSource
from .processor import Processor, Operator


logger = logging.getLogger(__name__)  # pylint: disable=invalid-name


def parse_operator(name: str, operator: Operator) -> Operator:
    """
    1. Create an instance of the class called type
    2. Instantiate other operators if they exist
    3. Add other operators to this instance
    4. Return built
    """
    from importlib import import_module

    if "." in operator["type"]:
        module_path, class_name = operator["type"].rsplit(".", 1)
        module = import_module(module_path)
        klass = getattr(module, class_name)
    else:
        module = import_module(".processor", package=__package__)
        klass = getattr(module, operator["type"])
    logger.debug(
        "Operator '%s' instantiated as '%s' args '%s'",
        name,
        klass.__name__,
        operator["args"],
    )
    instance = klass(name, operator["args"])

    if "operators" in operator:
        for name, operator in operator["operators"].items():
            instance.add_operator(parse_operator(name, operator))

    return instance


def main():
    """ The main function to run the module when not used as a library """

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s",
        handlers=[logging.StreamHandler(sys.stderr)],
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "source_wb", help="filename of the source excel workbook", type=Path
    )
    parser.add_argument(
        "-c",
        "--config",
        help="optional config filename, otherwise source_wb.json is used.",
        type=Path,
    )
    parser.add_argument(
        "-d",
        "--debug",
        help="optional debug argument. Set debug message level.",
        type=str,
    )
    args = parser.parse_args()

    # Set debug level from argument. Default to WARNING
    if args.debug:
        if "debug" == args.debug.lower():
            logging.getLogger().setLevel(logging.DEBUG)
        elif "info" == args.debug.lower():
            logging.getLogger().setLevel(logging.INFO)
        else:
            raise Exception("-d debugging option not valid")

    # If a config file is not specified use the workbook stem + .toml
    if not args.config:
        args.config = Path(args.source_wb).with_suffix(".json")

    logger.debug("Arguments: %s", args)

    config = json.load(open(args.config))

    if config["config_version"] != 1:
        raise ValueError(
            "Config file '{}' should be version 1. Version is {}".format(
                args.config, config["config_version"]
            )
        )

    data_source = OpenpyxlDataSource(args.source_wb, header_row=config["header_row"])
    proc = Processor(data_source)

    for name, operator in config["operators"].items():
        proc.add_operator(parse_operator(name, operator))

    proc.start()
    print(proc.operators[1].output())


main()
pass
