from datetime import datetime, timezone
import logging
import socket

import aioping

from settings import DATE_FORMAT_SOCRATA, STATUS_CODES
from config import SCHEMA

logger = logging.getLogger("__main__")


class Device:
    """A container for a single http-enabled device."""
    def __repr__(self):
        return f"<{self.device_type} '{self.ip_address}'>"

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        # verify required fields present
        self._raise_if_invalid()
        # set additional atttributes
        self.id = self._get_id()
        self.status_code = 0

    async def ping(self):
        """Async (non-blocking) attempt to ping the device's IP address.

        All exceptions are supressed and translated to the device's status_code
        and status_desc attributes.

        Side-effects:
            - Set self.timestamp to the current time
            - Set self.dellay to the ping delay in ms (if successful)
            - Set self.status_code and self.status_desc accordingly
        Returns:
            int: the device's status code.
        """
        logger.debug(f"Ping {self.ip_address}")
        self.timestamp = datetime.now(timezone.utc).strftime(DATE_FORMAT_SOCRATA)
        try:
            delay = await aioping.ping(self.ip_address, timeout=self.timeout) * 1000
            self.delay = int(delay)
            self.status_code = 1
            logger.debug(f"Success: {self.ip_address} in {self.delay}ms")
        except TimeoutError:
            self.status_code = -1
            pass
        except socket.gaierror:
            # invalid hostname
            self.status_code = -2
            pass
        except Exception as e:
            # unknown error
            self.status_code = -3
            pass
        self.status_desc = STATUS_CODES[self.status_code]
        self.status_code < 0 and logger.warning(
            f"Ping {self.ip_address} failed with code {self.status_code}: {self.status_desc}"
        )
        return self.status_code

    @property
    def __dict__(self):
        """Return a dict of instance properties. Only return keys defined in schema, which
        allows us to exclude extra instance properties, .e.g timeout.

        Returns:
            dict: instance properties
        """
        return {s: getattr(self, s, None) for s in SCHEMA.keys()}

    def _raise_if_invalid(
        self, required_attrs=["ip_address", "device_id", "device_type"]
    ):
        """Test if the instance has minimum required fields.

        Raises:
            ValueError if required attributes are Falsey
        """
        for attr in required_attrs:
            try:
                assert getattr(self, attr)
            except AssertionError:
                raise ValueError(f"Missing value for field {attr}")
            except AttributeError:
                raise ValueError(f"Missing required field {attr}")

    def _get_id(self) -> str:
        """Format the ID of the comm status record

        Args:
            device_id (int): the device's unique ID
            dt (datetime.datetime): a datetime instance
        Returns:
            str: the formatted record ID
        """
        now = datetime.now(timezone.utc)
        return f"{self.device_id}_{self.device_type}_{int(now.timestamp() * 1000)}"
