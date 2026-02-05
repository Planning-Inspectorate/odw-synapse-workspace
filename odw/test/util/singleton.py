class Singleton(type):
    """
    Singleton utility class
    """

    _INSTANCES = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._INSTANCES:
            if cls not in cls._INSTANCES:
                # Create and initialise the singleton instance
                cls._INSTANCES[cls] = super(Singleton, cls).__call__(
                    cls, *args, **kwargs
                )
        return cls._INSTANCES[cls]
