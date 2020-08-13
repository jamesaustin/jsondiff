#!/usr/bin/env python3
from os.path import join as path_join, splitext as path_splitext, basename as path_basename
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, FileType
from json import load as json_load, dump as json_dump, JSONEncoder
from sys import stdin as sys_stdin
from fnmatch import fnmatch
from numbers import Number
from math import isclose

from msgpack import load as msgpack_load

import logging

LOG = logging.getLogger(__name__)


class UniqueDict(dict):
    def __init__(self, generator):
        super().__init__()
        for k, v in generator:
            valid_key = k
            counter = 0
            while valid_key in self:
                valid_key = f"{k}.{counter}"
                counter += 1
            self[valid_key] = v


def jsondiff(_d1, _d2, ignore=frozenset(), set_sort=True):
    def _ignore(path):
        for f in ignore:
            if fnmatch(path, f):
                return True
        return False

    def diff(d1, d2, path):
        if _ignore(path):
            return
        if isinstance(d1, dict) and isinstance(d2, dict):
            k1, k2 = set(d1.keys()), set(d2.keys())
            for k in k1 - k2:
                key = path_join(path, str(k))
                if not _ignore(key):
                    print("-{}".format(key))
            for k in k1 & k2:
                key = path_join(path, str(k))
                diff(d1[k], d2[k], key)
            for k in k2 - k1:
                key = path_join(path, str(k))
                if not _ignore(key):
                    print("+{}".format(key))
        elif (
            set_sort
            and isinstance(d1, list)
            and isinstance(d2, list)
            and all(isinstance(d, (int, str, bytes)) for d in d1)
            and all(isinstance(d, (int, str, bytes)) for d in d2)
        ):
            d1 = set(d1)
            d2 = set(d2)
            for d in d1 - d2:
                key = path_join(path, str(d))
                if not _ignore(key):
                    print("-{}".format(key))
            for d in d2 - d1:
                key = path_join(path, str(d))
                if not _ignore(key):
                    print("+{}".format(key))
        elif isinstance(d1, list) and isinstance(d2, list):
            n1, n2 = len(d1), len(d2)
            if n1 < n2:
                print("+{}/[{}]/...".format(path, n2 - n1))
            elif n1 > n2:
                print("-{}/[{}]".format(path, n1 - n2))
            for n, (v1, v2) in enumerate(zip(d1, d2)):
                key = path_join(path, str(n))
                diff(v1, v2, path_join(path, key))
        elif not (isinstance(d1, Number) and isinstance(d2, Number)) and type(d1) != type(d2):
            print('!{}: {}"{}" -> {}"{}"'.format(path, type(d1), d1, type(d2), d2))
        elif isinstance(d1, float) and isinstance(d2, float):
            if not isclose(d1, d2, abs_tol=0.00001):
                print("~{}: {}f -> {}f".format(path, d1, d2))
        elif d1 != d2:
            print('~{}: {}"{}" -> {}"{}"'.format(path, type(d1), d1, type(d2), d2))

    diff(_d1, _d2, "/")


def fixup_keys(data):
    assert isinstance(data, list) and len(data) == 2 and isinstance(data[1], list)
    obj, keys = data[0], [k.decode("utf-8") for k in data[1]]

    def helper(d):
        if isinstance(d, dict):
            return {keys[k]: helper(v) for k, v in d.items()}
        elif isinstance(d, list):
            return [helper(v) for v in d]
        else:
            return d

    return helper(obj)


def parse_args():
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("json", nargs="*", type=FileType("rb"), default=[sys_stdin], help="JSON files to process")
    parser.add_argument("--ignore", nargs="*", default=[], type=str)
    parser.add_argument("--set-sort", action="store_true")
    parser.add_argument("--fixup-keys", action="store_true")
    parser.add_argument("--dump-fixedup", action="store_true")
    parser.add_argument("--dump-counts", action="store_true")
    group = parser.add_argument_group("debugging options")
    group.add_argument("--verbose", "-v", action="store_true")
    group.add_argument("--debug", action="store_true")
    args = parser.parse_args()

    if args.debug:
        level = logging.DEBUG
    elif args.verbose:
        level = logging.INFO
    else:
        level = logging.WARNING
    logging.basicConfig(format="%(message)s", level=level)
    LOG.debug("# %s", args)

    return args


def main():
    args = parse_args()

    def load(filename):
        name, ext = path_splitext(filename.name)
        if ext == ".msgpack":
            d = msgpack_load(filename)
        else:
            d = json_load(filename)

        if args.fixup_keys:
            d = fixup_keys(d)

        class BytesEncoder(JSONEncoder):
            def default(self, obj):
                if isinstance(obj, bytes):
                    return obj.decode("utf-8")
                return JSONEncoder.default(self, obj)

        if args.dump_fixedup:
            with open(f"_{path_basename(name)}.json", "w") as f:
                json_dump(d, f, cls=BytesEncoder, sort_keys=True, separators=(",", ": "), indent=2)

        if args.dump_counts:
            with open(f"_{path_basename(name)}.counts", "w") as f:

                def _dump_counts(obj, path="/"):
                    if isinstance(obj, dict):
                        print(f"{len(obj):6}: {path}", file=f)
                        for k, v in obj.items():
                            _dump_counts(v, path_join(path, k))
                    elif isinstance(obj, list):
                        print(f"{len(obj):6}: {path}", file=f)
                        for n, v in enumerate(obj):
                            _dump_counts(v, path_join(path, str(n)))

                _dump_counts(d)

        return d

    base = load(args.json[0])
    for f in args.json[1:]:
        j = load(f)
        jsondiff(base, j, args.ignore, args.set_sort)


if __name__ == "__main__":
    main()
