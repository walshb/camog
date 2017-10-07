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

_outfp = None

def _emit(args, s):
    global _indent, _outfp

    if _outfp is None:
        _outfp = open(args.outfilename, 'w')

    assert not s.endswith(':')  # no labels

    if s.startswith('NEXTCHAR'):
        s = args.nextchar + s[8:]

    if s.startswith('}'):
        _indent -= 4

    _outfp.write(' ' * _indent + s + '\n')

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

        for (cond, label, coco) in conds1:
            if cond is True:
                res.extend([(cond2, label2, coco + coco2)
                            for (cond2, label2, coco2) in conds2])
            else:
                code2 = self._exp2.copy_and_label().getcode(nullok)
                res.append((cond, label, coco + code2))

        res = ([c for c in res if c[0] is not True]
               + [c for c in res if c[0] is True])

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
    parser.add_argument('--type-stage', default=False, action='store_true')
    parser.add_argument('--outfilename')

    args = parser.parse_args()

    _label_prefix = args.label_prefix

    t = (lambda s: s) if args.type_stage else (lambda s: '')
    p = (lambda s: '') if args.type_stage else (lambda s: s)
    tp = (lambda s1, s2: s1) if args.type_stage else (lambda s1, s2: s2)

    spaces = Multiple(SingleChar("c == ' '", ""))

    plus = SingleChar("c == '+'", "")
    minus = SingleChar("c == '-'", "")

    change_to_double = "if (col_type == COL_TYPE_INT64) { columns[col_idx].type = col_type = COL_TYPE_DOUBLE; }"

    dot = SingleChar("c == '.'", t(change_to_double))

    pl_digit = SingleChar("(digit = c ^ '0') <= 9", p("value = value * 10 + digit;"))
    pl_frac_digit = SingleChar("(digit = c ^ '0') <= 9", p("value = value * 10 + digit; ++fracexpo;"))
    mi_digit = SingleChar("(digit = c ^ '0') <= 9", p("value = value * 10 - digit;"))
    mi_frac_digit = SingleChar("(digit = c ^ '0') <= 9", p("value = value * 10 - digit; ++fracexpo;"))
    if args.type_stage:
        pl_int_digits = Multiple(SingleChar("(digit = c ^ '0') <= 9", ""))
        pl_frac_digits = pl_int_digits
        mi_int_digits = pl_int_digits
        mi_frac_digits = pl_frac_digits
    else:
        pl_int_digits = _seq(Multiple(SingleChar("(digit = c ^ '0') <= 9 && (value - 922337203685477580) + ((digit + 8) >> 4) <= 0", "value = value * 10 + digit;")),
                             Multiple(SingleChar("(digit = c ^ '0') <= 9", "--fracexpo;")))
        pl_frac_digits = _seq(Multiple(SingleChar("(digit = c ^ '0') <= 9 && (value - 922337203685477580) + ((digit + 8) >> 4) <= 0", "value = value * 10 + digit; ++fracexpo;")),
                              Multiple(SingleChar("(digit = c ^ '0') <= 9", "")))
        mi_int_digits = _seq(Multiple(SingleChar("(digit = c ^ '0') <= 9 && (-value - 922337203685477580) + ((digit + 7) >> 4) <= 0", "value = value * 10 - digit;")),
                             Multiple(SingleChar("(digit = c ^ '0') <= 9", "--fracexpo;")))
        mi_frac_digits = _seq(Multiple(SingleChar("(digit = c ^ '0') <= 9 && (-value - 922337203685477580) + ((digit + 7) >> 4) <= 0", "value = value * 10 - digit; ++fracexpo;")),
                              Multiple(SingleChar("(digit = c ^ '0') <= 9", "")))

    pl_mantissa = _seq(Or(plus, Nothing()),
                       Or(_seq(pl_digit,
                               pl_int_digits,
                               Or(_seq(dot,
                                       pl_frac_digits),
                                  Nothing())),
                          _seq(dot,
                               pl_frac_digit,
                               pl_frac_digits)))

    mi_mantissa = _seq(minus,
                       Or(_seq(mi_digit,
                               mi_int_digits,
                               Or(_seq(dot,
                                       mi_frac_digits),
                                  Nothing())),
                          _seq(dot,
                               mi_frac_digit,
                               mi_frac_digits)))


    expo_plus = SingleChar("c == '+'", "")
    expo_minus = SingleChar("c == '-'", p("exposign = -1;"))
    expo_plus_or_minus = Or(expo_plus, Or(expo_minus, Nothing()))

    expo_digit = SingleChar("(digit = c ^ '0') <= 9", p("expo = (expo * 10 + digit) & 511;"))

    expo = _seq(SingleChar("(c | 32) == 'e'", t(change_to_double)),
                expo_plus_or_minus,
                expo_digit,
                Multiple(expo_digit))

    null_expr = Or(_seq(SingleChar("(c | 32) == 'n'", ""),
                        SingleChar("(c | 32) == 'a'", ""),
                        SingleChar("(c | 32) == 'n'",
                                   tp(change_to_double, "expo = INT_MIN;"))),
                   Nothing())

    expr = _seq(spaces,
                Or(_seq(Or(pl_mantissa,
                           mi_mantissa),
                        Or(expo,
                           Nothing()),
                        spaces),
                   null_expr))

    _doparse(args, expr)

    return 0


if __name__ == '__main__':
    sys.exit(main())
