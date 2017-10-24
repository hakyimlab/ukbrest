
def get_list(values):
    """
    If values is a list, return. If it is a single value, return a list with it.
    :param values:
    :return:
    """
    if isinstance(values, (list, tuple)):
        return values

    return [values]
