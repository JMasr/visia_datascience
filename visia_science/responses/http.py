from pydantic import BaseModel, Field

from visia_science import app_logger


class BasicResponse(BaseModel):
    """
    The BasicResponse class is a Pydantic model that represents a standard HTTP response structure with fields for
    success status, status code, and a message. It includes a method to log these response details using a logger.

    Example Usage
    -------------
    response = BasicResponse(success=True, status_code=200, message="Operation successful")
    response.log_response(module="DataModule", action="FetchData")

    Main functionalities
    --------------------
    * Representing a standard response structure with success status, status code, and message.
    * Logging the response details using a predefined logger.

    Parameters
    ----------
    success: bool
        The success status of the operation.
    status_code: int
        The status code of the operation.
    message: str
        The message describing the operation.

    Methods
    -------
    * log_response(self, module: str, action: str): Logs the response details.
    """

    success: bool
    status_code: int = Field(..., ge=100, le=599)
    message: str

    def log_response(self, module: str, action: str) -> None:
        """
        Logs the response details including the module name, action, success status, message, and status code.

        Parameters
        ----------
        module: str
            The name of the module where the operation was performed.
        action: str
            The name of the action performed in the module.

        """
        app_logger.info(
            f"{module} - {action} - Success: {self.success} - Message: {self.message} - Status Code: {self.status_code}"
        )


class ListResponse(BasicResponse):
    data: list


class DataResponse(BasicResponse):
    data: dict
