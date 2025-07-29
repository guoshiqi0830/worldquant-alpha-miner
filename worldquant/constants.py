from enum import Enum

CHECK_METRIC_MAPPING = {
    'LOW_SHARPE': 'sharpe',
    'LOW_FITNESS': 'fitness',
    'LOW_TURNOVER': 'turnover',
    'LOW_SUB_UNIVERSE_SHARPE': 'sub_universe_sharpe',
    'CONCENTRATED_WEIGHT': 'concentrated_weight',
    'SELF_CORRELATION': 'self_correlation',
}

class Status(Enum):
    PENDING = 'PENDING'
    WAITING = 'WAITING'
    EXPIRED = 'EXPIRED'
    ERROR = 'ERROR'
    PASS = 'PASS'
    FAIL = 'FAIL'
    ACTIVE = 'ACTIVE'
    COMPLETE = 'COMPLETE'
    UNSUBMITTED = 'UNSUBMITTED'
