import sys
from typing import Optional
from ..core.display import _decorate_str


def ask_confirmation(
            self,
            s: str = "<magenta><b>Do you want to continue? [y/n]:</b>"
                     "</magenta> ",
            exit: bool = True
    ) -> Optional[bool]:
        """Request user input to confirm or reject an instruction.

        Args:
            s (str): Message to be printed to ask user confirmation.
            exit (bool): If ``True`` and user answer is ``n`` (no), then
                the program execution is terminated.

        Returns:
            (Optional[bool]): User response.
        """
        user_input = None

        while str(user_input) not in ["y", "n"]:
            if user_input is not None:
                self.print(f"<error>Invalid input '{user_input}'</error>")

            user_input = input(_decorate_str(s))

            if str(user_input) == "y":
                response = True

            elif str(user_input) == "n":
                if exit:
                    self.print(
                        "<warning>Program finished by the user</warning>"
                    )
                    sys.exit(0)

                else:
                    response = False
            else:
                pass

        return response
