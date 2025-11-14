from datetime import datetime
from zoneinfo import ZoneInfo

from .constants import Timezone


def get_kst_now() -> datetime:
    return datetime.now(ZoneInfo(Timezone.KST))
