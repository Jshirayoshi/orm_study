from sqlalchemy import Column, DateTime, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()

class User(Base):
    """
    システムに登録されるユーザーの情報。
    """
    __tablename__ = 'users'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='ユーザーID')
    name = Column(String(length=100), nullable=False, comment='ユーザー名')
    email = Column(String(length=255), nullable=False, unique=True, comment='メールアドレス')
    phone_number = Column(String(length=20), comment='電話番号')
    created_at = Column(DateTime, server_default=func.now(), comment='作成日時')

class Product(Base):
    """
    販売する商品の情報。
    """
    __tablename__ = 'products'

    id = Column(Integer, primary_key=True, autoincrement=True, comment='商品ID')
    name = Column(String(length=255), nullable=False, comment='商品名')
    price = Column(Integer, nullable=False, default=0, comment='商品価格')
    category = Column(String(length=50), index=True, comment='商品のカテゴリ。例: Electronics, Books')
    stock_quantity = Column(Integer, nullable=False, default=0, comment='在庫数')
