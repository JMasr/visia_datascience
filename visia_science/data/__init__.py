class QuestionaryError(Exception):
    """Base class for all questionary related errors."""

    def __init__(self, message, error_type=None):
        super().__init__(message)
        self.error_type = error_type

    def __str__(self):
        if self.error_type:
            return f"[{self.error_type}] - {super().__str__()}"
        return super().__str__()
