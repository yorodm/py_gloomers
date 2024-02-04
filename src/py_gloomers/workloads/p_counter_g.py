"""P Counter with Gossip"""
from typing import Optional
from py_gloomers.node import Node, StdIOTransport, Body, log, reply_to
from py_gloomers.types import BodyFields, MessageTypes


node = Node(transport=StdIOTransport())

def main():  # noqa
    node.run()


if __name__ == "__main__":
    main()
