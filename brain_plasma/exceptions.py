class BrainError(Exception):
    """
    base class of brain_plasma exceptions
    """

    pass


class BrainNamespaceNameError(BrainError):
    pass


class BrainNamespaceRemoveDefaultError(BrainError):
    pass


class BrainNamespaceNotExistError(BrainError):
    pass


class BrainNameNotExistError(BrainError):
    pass


class BrainNameLengthError(BrainError):
    pass


class BrainNameTypeError(BrainError):
    pass


class BrainClientDisconnectedError(BrainError):
    pass


class BrainLearnNameError(BrainError):
    pass


class BrainRemoveOldNameValueError(BrainError):
    pass


class BrainUpdateNameError(BrainError):
    pass
