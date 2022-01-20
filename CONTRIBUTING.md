## Contributing to data_manager

This document aims to provide a brief guide on how to contribute to `data_manager`.  

### Who can contribute?

Anyone! Contributions from any user are welcome.  Contributions aren't limited to changing code. 
Filing bug reports, asking questions, adding examples or documentation are all ways to contribute.
New users may find it helpful to start with improving documentation, type hinting, or tests.  

## Asking a question

Questions can be submitted as either an issue or a discussion post.  A general question not directly related to the code
or one that may be beneficial to other users would be most appropriate in the discussions area.  One that is about a 
specific feature or could turn into a feature request or bug report would be more appropriate as an issue.  If submitting
a question as an issue, please use the _question_ template.  

## Submitting an Issue

No code is perfect, `data_manager` is no different and user submitted issues aid in improving the quality of this library.
Before submitting an issue, check to see if someone has already submitted one before so we can avoid duplicate issues. 

### Bug Reports

To submit a bug report, please create an issue using the _Bug Report_ template. Please include as much information as 
possible relating to the bug.  The more detailed the bug report, the easier and faster it will be to resolve.

### Feature Requests

For feature requests or enhancements, please create an issue using the _Feature Request_ template.


## Submitting Changes

Submitting code or documentation changes is another way to contribute.  All contributions should be made in the form of 
a pull request.  You should fork this repository and clone it to your machine.  All work is done in the `develop` branch 
first before merging to `master`.  All pull requests should target the `develop` branch.

Some requirements for code changes to be accepted include:

- code should be _pythonic_ and follow PEP8, PEP20, and other Python best-practices or common conventions
- public methods should have docstrings which will be included in the documentation
- comments and docstrings should explain _why_ and _how_ the code works, not merely _what_ it is doing
- type hinting should be used as much as possible, all public methods need to have hints
- new functionality should have tests
- run the _user_ tests and verify there are no issues
- avoid 3rd party dependencies, code should only require the Python standard library
- avoid breaking changes, unless adequately justified
- do not update the library version

Some suggested contributions include:
- type hinting
    - all public methods are type hinted, but many internal methods are missing them
- tests
    - new tests are always welcome, particularly offline tests or any methods missing tests
