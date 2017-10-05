#!/usr/bin/env

import argparse
import os
import colorama
import handlers
import common
from colorama import Fore

colorama.init()
lines = []
for idx, line in enumerate(common.RESOURCES[common.AMATERASU_LOGO]):
    if idx <= 7:
        lines.append("\033[38;5;202m" + line)
    elif 7 < idx < 14:
        lines.append("\033[38;5;214m" + line)
    else:
        lines.append("\033[38;5;220m" + line)
desc = ''.join(lines)
desc += Fore.RESET + '\n\n'
desc += common.RESOURCES[common.APACHE_LOGO]
desc += common.RESOURCES[common.AMATERASU_TXT]

parser = argparse.ArgumentParser(
    prog="ama",
    formatter_class=argparse.RawDescriptionHelpFormatter,
    description=desc)

subparsers = parser.add_subparsers(title="Available Actions", description="You can choose to either create a new Amaterasu repo, or run an existing Amaterasu pipeline")

init_parser = subparsers.add_parser(common.INIT, help="Scaffolding for a new Amaterasu job repository")
init_parser.add_argument('path', nargs='?', default=os.getcwd())
init_parser.set_defaults(which=common.INIT)

run_parser = subparsers.add_parser(common.RUN, help='Run an Amaterasu pipeline')
run_parser.set_defaults(which=common.RUN)

args = parser.parse_args()
if hasattr(args, 'which'):
    handlers.handle_args(args)