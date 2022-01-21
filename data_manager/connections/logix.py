from pycomm3 import LogixDriver
from ..tag import Tag


class LogixConnection(Tag):
    def __init__(self, params: dict) -> None:
        super().__init__(params)