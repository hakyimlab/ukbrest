
def get_list(values):
    """
    If values is a list, return. If it is a single value, return a list with it.
    :param values:
    :return:
    """
    if isinstance(values, (list, tuple)):
        return values

    return [values]


def _update_parameters_from_args(params, args):
    for k, v in vars(args).items():
        if k in params and v is not None:
            params[k] = v

    return params


def _parameter_empty(parameters, parameter_name):
    return (parameter_name not in parameters or parameters[parameter_name] is None)
