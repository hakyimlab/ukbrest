from os.path import dirname, abspath, join


def get_repository_path(data_filename):
    directory = dirname(abspath(__file__))
    directory = join(directory, 'data/')
    return join(directory, data_filename)
