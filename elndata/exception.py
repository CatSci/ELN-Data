import sys

def error_message_detail(error, error_detail: sys):
    """_summary_

    Args:
        error (_type_): _description_
        error_detail (sys): _description_
    """

    _,_, exc_tb = error_detail.exc_info()

    file_name = exc_tb.tb_frame.f_code.co_filename

    return "Error Occurred in python script [{0}] line number [{1}] error message [{2}]".format(
        file_name, exc_tb.tb_lineno, str(error)
    )

class ELNException(Exception):

    def __init__(self,error_message, error_detail):
        super().__init__(error_message)

        self.error_message = error_message_detail(error_message, error_detail= error_detail)

    def __str__(self) -> str:
        return self.error_message