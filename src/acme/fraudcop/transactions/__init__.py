from decimal import Decimal
from typing import NamedTuple, Dict, Optional

from acme.fraudcop.transactions import transaction_pb2


class Transaction(NamedTuple):
    trans_id: int
    account_id: Optional[int] = None
    date: Optional[int] = None
    type: Optional[str] = None
    operation: Optional[str] = None
    amount: Optional[Decimal] = None
    balance: Optional[Decimal] = None
    k_symbol: Optional[str] = None
    bank: Optional[str] = None
    account: Optional[int] = None
    district_id: Optional[int] = None
    acct_freq: Optional[str] = None
    a2: Optional[str] = None
    a3: Optional[str] = None
    a4: Optional[int] = None
    a5: Optional[int] = None
    a6: Optional[int] = None
    a7: Optional[int] = None
    a8: Optional[int] = None
    a9: Optional[int] = None
    a10: Optional[Decimal] = None
    a11: Optional[int] = None
    a12: Optional[Decimal] = None
    a13: Optional[Decimal] = None
    a14: Optional[int] = None
    a15: Optional[int] = None
    a16: Optional[int] = None
    disp_type: Optional[str] = None
    card_id: Optional[int] = None
    card_type: Optional[str] = None
    card_issued: Optional[str] = None
    loan_date: Optional[int] = None
    loan_amount: Optional[int] = None
    loan_duration: Optional[int] = None
    loan_payments: Optional[Decimal] = None
    loan_status: Optional[str] = None

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
            amount=Decimal(obj.amount) if obj.HasField("amount") else None,
            balance=Decimal(obj.balance) if obj.HasField("balance") else None,
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
            a10=Decimal(obj.a10) if obj.HasField("a10") else None,
            a11=obj.a11,
            a12=Decimal(obj.a12) if obj.HasField("a12") else None,
            a13=Decimal(obj.a13) if obj.HasField("a13") else None,
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
            loan_payments=Decimal(obj.loan_payments)
            if obj.HasField("loan_payments")
            else None,
            loan_status=obj.loan_status,
        )

    # noinspection PyArgumentList
    # suppress warning: parameter `typename` unfilled
    @classmethod
    def from_dictionary_of_strings(cls, d: Dict[str, str], null_token=""):
        return cls(
            trans_id=int(d["trans_id"]),
            account_id=int(d["account_id"]) if d["account_id"] != null_token else None,
            date=int(d["date"]) if d["date"] != null_token else None,
            type=d["type"] if d["type"] != null_token else None,
            operation=d["operation"] if d["operation"] != null_token else None,
            amount=Decimal(d["amount"]) if d["amount"] != null_token else None,
            balance=Decimal(d["balance"]) if d["balance"] != null_token else None,
            k_symbol=d["k_symbol"] if d["k_symbol"] != null_token else None,
            bank=d["bank"] if d["bank"] != null_token else None,
            account=int(d["account"]) if d["account"] != null_token else None,
            district_id=int(d["district_id"])
            if d["district_id"] != null_token
            else None,
            acct_freq=d["acct_freq"] if d["acct_freq"] != null_token else None,
            a2=d["a2"] if d["a2"] != null_token else None,
            a3=d["a3"] if d["a3"] != null_token else None,
            a4=int(d["a4"]) if d["a4"] != null_token else None,
            a5=int(d["a5"]) if d["a5"] != null_token else None,
            a6=int(d["a6"]) if d["a6"] != null_token else None,
            a7=int(d["a7"]) if d["a7"] != null_token else None,
            a8=int(d["a8"]) if d["a8"] != null_token else None,
            a9=int(d["a9"]) if d["a9"] != null_token else None,
            a10=Decimal(d["a10"]) if d["a10"] != null_token else None,
            a11=int(d["a11"]) if d["a11"] != null_token else None,
            a12=Decimal(d["a12"]) if d["a12"] != null_token else None,
            a13=Decimal(d["a13"]) if d["a13"] != null_token else None,
            a14=int(d["a14"]) if d["a14"] != null_token else None,
            a15=int(d["a15"]) if d["a15"] != null_token else None,
            a16=int(d["a16"]) if d["a16"] != null_token else None,
            disp_type=d["disp_type"] if d["disp_type"] != null_token else None,
            card_id=int(d["card_id"]) if d["card_id"] != null_token else None,
            card_type=d["card_type"] if d["card_type"] != null_token else None,
            card_issued=d["card_issued"] if d["card_issued"] != null_token else None,
            loan_date=int(d["loan_date"]) if d["loan_date"] != null_token else None,
            loan_amount=int(d["loan_amount"])
            if d["loan_amount"] != null_token
            else None,
            loan_duration=int(d["loan_duration"])
            if d["loan_duration"] != null_token
            else None,
            loan_payments=Decimal(d["loan_payments"])
            if d["loan_payments"] != null_token
            else None,
            loan_status=d["loan_status"] if d["loan_status"] != null_token else None,
        )

    def to_protobuf(self):
        m = transaction_pb2.Transaction()
        m.trans_id = self.trans_id
        if self.account_id is not None:
            m.account_id = self.account_id
        if self.date is not None:
            m.date = self.date
        if self.type is not None:
            m.type = self.type
        if self.operation is not None:
            m.operation = self.operation
        if self.amount is not None:
            m.amount = str(self.amount)
        if self.balance is not None:
            m.balance = str(self.balance)
        if self.k_symbol is not None:
            m.k_symbol = self.k_symbol
        if self.bank is not None:
            m.bank = self.bank
        if self.account is not None:
            m.account = self.account
        if self.district_id is not None:
            m.district_id = self.district_id
        if self.acct_freq is not None:
            m.acct_freq = self.acct_freq
        if self.a2 is not None:
            m.a2 = self.a2
        if self.a3 is not None:
            m.a3 = self.a3
        if self.a4 is not None:
            m.a4 = self.a4
        if self.a5 is not None:
            m.a5 = self.a5
        if self.a6 is not None:
            m.a6 = self.a6
        if self.a7 is not None:
            m.a7 = self.a7
        if self.a8 is not None:
            m.a8 = self.a8
        if self.a9 is not None:
            m.a9 = self.a9
        if self.a10 is not None:
            m.a10 = str(self.a10)
        if self.a11 is not None:
            m.a11 = self.a11
        if self.a12 is not None:
            m.a12 = str(self.a12)
        if self.a13 is not None:
            m.a13 = str(self.a13)
        if self.a14 is not None:
            m.a14 = self.a14
        if self.a15 is not None:
            m.a15 = self.a15
        if self.a16 is not None:
            m.a16 = self.a16
        if self.disp_type is not None:
            m.disp_type = self.disp_type
        if self.card_id is not None:
            m.card_id = self.card_id
        if self.card_type is not None:
            m.card_type = self.card_type
        if self.card_issued is not None:
            m.card_issued = self.card_issued
        if self.loan_date is not None:
            m.loan_date = self.loan_date
        if self.loan_amount is not None:
            m.loan_amount = self.loan_amount
        if self.loan_duration is not None:
            m.loan_duration = self.loan_duration
        if self.loan_payments is not None:
            m.loan_payments = str(self.loan_payments)
        if self.loan_status is not None:
            m.loan_status = self.loan_status
        return m
