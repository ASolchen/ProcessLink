# -*- coding: utf-8 -*-
#
# Copyright (c) 2022 Adam Solchenberger <asolchenberger@gmail.com>
# Copyright (c) 2022 Jason Engman <jengman@testtech-solutions.com>
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
from typing import Any

class PropertyError(Exception):
    """
    raised when setting or getting a property that does not exist
    """

class APIClass(object):
    """
    uses a list of properties that are availible externally
    each class that gets extended will add it properties to its super()
    """
    def __init__(self) -> None:
        super().__init__()
        self.properties = []

    def get(self, prop: str) -> Any:
        if not prop in self.properties:
            raise PropertyError(f"'{prop}' is not a property of {self}")
        return getattr(self, prop)
    
    def set(self, prop: str, value: Any) -> bool:
        if not prop in self.properties:
            raise PropertyError(f"'{prop}' is not a property of {self}")
        try:
            setattr(self, prop, value)
        except AttributeError:
            raise PropertyError(f"Attempt to set a read-only property '{prop}' of {self}")
        return True