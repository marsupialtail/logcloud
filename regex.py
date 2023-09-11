import json

class Node:
    def __init__(self, value, left=None, right=None):
        self.value = value
        self.left = left
        self.right = right

    def __repr__(self):
        return f'Node({repr(self.value)})'

    def pretty(self, level=0):
        ret = "\t"*level+repr(self.value)+"\n"
        if self.left is not None:
            ret += self.left.pretty(level+1)
        if self.right is not None:
            ret += self.right.pretty(level+1)
        return ret
    
    def to_json(self):
        json_dict = {"value": self.value}
        if self.left is not None:
            json_dict["left"] = self.left.to_json()
        if self.right is not None:
            json_dict["right"] = self.right.to_json()
        return json_dict
    


class Parser:
    def __init__(self, regex):
        self.regex = regex
        self.index = 0

    def parse(self):
        return self._regex()

    def print_tree(self):
        print(self.parse().pretty())
    
    def print_tree_json(self):
        print(json.dumps(self.parse().to_json(), indent=4))

    def _regex(self):
        term_node = self._term()

        if self._has_more() and self._peek() == '|':
            self._consume('|')
            regex_node = self._regex()
            return Node('union', term_node, regex_node)

        return term_node

    def _term(self):
        factor_node = self._factor()

        if self._has_more() and self._peek() not in ('|', ')'):
            term_node = self._term()
            return Node('concat', factor_node, term_node)

        return factor_node

    def _factor(self):
        base_node = self._base()

        while self._has_more() and self._peek() == '*':
            self._consume('*')
            base_node = Node('repeat', base_node)

        return base_node

    def _base(self):
        if self._peek() == '(':
            self._consume('(')
            regex_node = self._regex()
            self._consume(')')
            return regex_node

        char = self._consume()
        if char is not None:
            return Node('char: ' + char)
        return None

    def _peek(self):
        if self._has_more():
            return self.regex[self.index]
        return None

    def _consume(self, expected=None):
        if not self._has_more():
            return None

        current = self.regex[self.index]

        if expected is not None and current != expected:
            raise ValueError('Expected "{}" but got "{}"'.format(expected, current))

        self.index += 1
        return current

    def _has_more(self):
        return self.index < len(self.regex)


def parse_regex(regex):
    parser = Parser(regex)
    parser.print_tree_json()
    return parser.parse()


print(parse_regex('a*b*c'))