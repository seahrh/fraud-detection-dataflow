from decimal import Decimal
from typing import NamedTuple


class Transaction(NamedTuple):
    trans_id: int
    account_id: int
    date: int
    type: str
    operation: str
    amount: Decimal
    balance: Decimal
    k_symbol: str
    bank: str
    account: int
    district_id: int
    acct_freq: str
    a2: str
    a3: str
    a4: int
    a5: int
    a6: int
    a7: int
    a8: int
    a9: int
    a10: Decimal
    a11: int
    a12: Decimal
    a13: Decimal
    a14: int
    a15: int
    a16: int
    disp_type: str
    card_id: int
    card_type: str
    card_issued: str
    loan_date: int
    loan_amount: int
    loan_duration: int
    loan_payments: Decimal
    loan_status: str

    # noinspection PyArgumentList
    # suppress warning: parameter `typename` unfilled
    @classmethod
    def from_protobuf(cls, obj):
        """Converts the Transaction proto object to a NamedTuple."""
        return cls(
            trans_id=obj.trans_id,
            account_id=obj.account_id,
            date=obj.date,
            type=obj.type,
            operation=obj.operation,
            amount=Decimal(obj.amount),
            balance=Decimal(obj.balance),
            k_symbol=obj.k_symbol,
            bank=obj.bank,
            account=obj.account,
            district_id=obj.district_id,
            acct_freq=obj.acct_freq,
            a2=obj.a2,
            a3=obj.a3,
            a4=obj.a4,
            a5=obj.a5,
            a6=obj.a6,
            a7=obj.a7,
            a8=obj.a8,
            a9=obj.a9,
            a10=Decimal(obj.a10),
            a11=obj.a11,
            a12=Decimal(obj.a12),
            a13=Decimal(obj.a13),
            a14=obj.a14,
            a15=obj.a15,
            a16=obj.a16,
            disp_type=obj.disp_type,
            card_id=obj.card_id,
            card_type=obj.card_type,
            card_issued=obj.card_issued,
            loan_date=obj.loan_date,
            loan_amount=obj.loan_amount,
            loan_duration=obj.loan_duration,
            loan_payments=Decimal(obj.loan_payments),
            loan_status=obj.loan_status,
        )
