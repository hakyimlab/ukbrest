from os.path import dirname, abspath, join


def get_repository_path(data_filename):
    directory = dirname(abspath(__file__))
    directory = join(directory, 'data/')
    return join(directory, data_filename)


def get_full_path(filename):
    root_dir = dirname(dirname(abspath(__file__)))
    return join(root_dir, filename)
