from parameters_validation import validate_parameters, no_whitespaces, non_blank


class DataLakeEntity:
    @validate_parameters
    def __init__(self, path: non_blank(no_whitespaces(str))):
        self.path = path.lstrip("/").rstrip("/")
