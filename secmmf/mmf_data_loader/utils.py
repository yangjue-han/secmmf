class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'


class DictList(dict):
    def __setitem__(self, key, value):
        try:
            # Assumes there is a list on the key
            self[key].append(value)
        except KeyError: # If it fails, because there is no key
            super(DictList, self).__setitem__(key, value)
        except AttributeError: # If it fails because it is not a list
            super(DictList, self).__setitem__(key, [self[key], value])
