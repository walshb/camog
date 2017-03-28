#!/usr/bin/env python
#
# Copyright 2017 Ben Walsh
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
import logging

##logging.basicConfig(level=logging.DEBUG)

import argparse

_logger = logging.getLogger(__name__)


_indent = 4
_label_idx = 0

_label_prefix = ''


def _emit(args, s):
    global _indent

    assert not s.endswith(':')  # no labels

    if s.startswith('NEXTCHAR'):
        s = args.nextchar + s[8:]

    if s.startswith('}'):
        _indent -= 4

    print ' ' * _indent + s

    if s.endswith('{'):
        _indent += 4


def _new_label(s):
    global _label_idx

    _label_idx += 1

    return '%s%s%s' % (_label_prefix, s, _label_idx)


def _if(conds, otherwise='goto bad'):
    res = []
    nullcode = None
    elsetok = ''
    for cond, label, coco in conds:
        if cond is True:
            assert nullcode is None or not coco
            if nullcode is None:
                nullcode = coco
            continue

        res.append('%sif (%s) {' % (elsetok, cond))
        res.extend(coco)
        res.append('}')
        elsetok = 'else '

    res.append('else {')
    if nullcode is not None:
        res.extend(nullcode)
    else:
        res.append('%s;' % otherwise)
    res.append('}')

    return res


def _seq(*args):
    res = Seq(args[-2], args[-1])
    for elem in args[-3::-1]:
        res = Seq(elem, res)
    return res


def _multichar(cond, code):
    return Multiple(SingleChar(cond, code))


class Nothing(object):
    def __init__(self):
        self._label = None


    def getcode(self, nullok):
        return []


    def getconds(self, nullok):
        return [(True, self._label, [])]


    def acceptnull(self):
        return True


    def copy_and_label(self):
        res = Nothing()
        res._label = _new_label('n')
        return res


class SingleChar(object):
    def __init__(self, cond, code):
        self._cond = cond
        self._code = code
        self._label = None


    def getcode(self, nullok):
        return _if(self.getconds(nullok))


    def getconds(self, nullok):
        label = 'goodend' if nullok else 'badend'
        code = [self._code,
                'NEXTCHAR(%s);' % label]
        return [(self._cond, self._label, code)]


    def acceptnull(self):
        return False


    def copy_and_label(self):
        res = SingleChar(self._cond, self._code)
        res._label = _new_label('c')
        return res


class Multiple(object):
    def __init__(self, exp1):
        self._exp1 = exp1


    def getcode(self, nullok):
        return _if(self.getconds(nullok))


    def getconds(self, nullok):
        conds1 = self._exp1.getconds(nullok)
        res = [(expcond, explabel,
                ['do {'] + expcode + ['} while (%s);' % expcond])
               for expcond, explabel, expcode in conds1]
        if nullok:
            res.append((True, self._label2, []))  # or null
        return res


    def acceptnull(self):
        return True


    def copy_and_label(self):
        res = Multiple(self._exp1.copy_and_label())
        res._label = _new_label('m')
        res._label2 = _new_label('m')
        return res


class Or(object):
    def __init__(self, exp1, exp2):
        self._exp1 = exp1
        self._exp2 = exp2


    def getcode(self, nullok):
        return _if(self.getconds(nullok))


    def getconds(self, nullok):
        return self._exp1.getconds(nullok) + self._exp2.getconds(nullok)


    def acceptnull(self):
        return self._exp1.acceptnull() or self._exp2.acceptnull()


    def copy_and_label(self):
        res = Or(self._exp1.copy_and_label(), self._exp2.copy_and_label())
        res._label = _new_label('o')
        return res


class Seq(object):
    def __init__(self, exp1, exp2):
        self._exp1 = exp1
        self._exp2 = exp2
        self._label = None


    def getcode(self, nullok):
        code1 = self._exp1.getcode(nullok and self._exp2.acceptnull())
        code2 = self._exp2.getcode(nullok)
        return code1 + code2


    def getconds(self, nullok):
        conds2 = self._exp2.getconds(nullok)
        conds1 = self._exp1.getconds(nullok and self._exp2.acceptnull())
        res = []

        assert all(cond is not True for (cond, label, coco) in conds1[:-1])  # "any" must come at end

        has_any = (conds1[-1][0] is True)
        if has_any:  # last is "any"
            conds1 = [conds1[-1]] + conds1[:-1]  # rearrange

        idx = 0
        for (cond, label, coco) in conds1:
            if cond is True:
                res.extend([(cond2, label2, coco + coco2)
                            for (cond2, label2, coco2) in conds2])
            else:
                code2 = self._exp2.copy_and_label().getcode(nullok)
                res.append((cond, label, coco + code2))

            idx += 1

        return res


    def acceptnull(self):
        return self._exp1.acceptnull() and self._exp2.acceptnull()


    def copy_and_label(self):
        res = Seq(self._exp1.copy_and_label(), self._exp2.copy_and_label())
        res._label = _new_label('s')
        return res


def _doparse(args, exp):
    exp = exp.copy_and_label()  # want a tree, not a DAG.
    code = exp.getcode(True)
    for c in code:
        _emit(args, c)


def main():
    global _label_prefix

    parser = argparse.ArgumentParser()

    parser.add_argument('--nextchar', default='NEXTCHAR')
    parser.add_argument('--label-prefix', default='')

    args = parser.parse_args()

    _label_prefix = args.label_prefix

    spaces = Multiple(SingleChar("c == ' '", ""))

    plus = SingleChar("c == '+'", "")
    minus = SingleChar("c == '-'", "sign = -1;")
    plus_or_minus = Or(plus, Or(minus, Nothing()))

    change_to_double = "if (col_type == COL_TYPE_INT) { CHANGE_TYPE(&columns[col_idx], int64_t, double); columns[col_idx].type = col_type = COL_TYPE_DOUBLE; }"

    dot = SingleChar("c == '.'", change_to_double)
    digit = SingleChar("(digit = c ^ '0') <= 9", "value = value * 10 + digit;")
    frac_digit = SingleChar("(digit = c ^ '0') <= 9", "value = value * 10 + digit; ++fracexpo;")

    expo_plus = SingleChar("c == '+'", "")
    expo_minus = SingleChar("c == '-'", "exposign = -1;")
    expo_plus_or_minus = Or(expo_plus, Or(expo_minus, Nothing()))

    expo_digit = SingleChar("(digit = c ^ '0') <= 9", "expo = expo * 10 + digit;")

    expo = _seq(SingleChar("(c | 32) == 'e'", change_to_double),
                expo_plus_or_minus,
                expo_digit,
                Multiple(expo_digit))

    expr = _seq(spaces,
                Or(_seq(plus_or_minus,
                        Or(_seq(digit,
                                Multiple(digit),
                                Or(_seq(dot,
                                        Multiple(frac_digit)),
                                   Nothing())),
                           _seq(dot,
                                frac_digit,
                                Multiple(frac_digit))),
                        Or(expo,
                           Nothing()),
                        spaces),
                   Nothing()))

    _doparse(args, expr)

    return 0


if __name__ == '__main__':
    sys.exit(main())
