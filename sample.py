import os
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from typing import List

# ---------------------------------------------------
# 1. models.py のインポート
# ---------------------------------------------------
# generate_models.py で生成された models.py をインポートします
try:
    # 現在のディレクトリにある models.py を動的にインポート
    # これにより、スクリプト実行時に生成されたモデルクラスが利用可能になります
    import importlib.util
    spec = importlib.util.spec_from_file_location("orm_models", "models.py")
    orm_models = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(orm_models)

    Base = orm_models.Base
    User = orm_models.User
    Product = orm_models.Product

    print("models.py からモデルを正常にロードしました。")

except ImportError:
    print("エラー: 'models.py' が見つからないか、インポートできませんでした。")
    print("先に 'generate_models.py' を実行して 'models.py' を生成してください。")
    exit(1)
except Exception as e:
    print(f"モデルのロード中に予期せぬエラーが発生しました: {e}")
    exit(1)


# ---------------------------------------------------
# 2. データベース接続設定
# ---------------------------------------------------
# SQLite データベースファイルのパス
DATABASE_FILE = 'app.db'
DATABASE_URL = f"sqlite:///{DATABASE_FILE}"

# SQLAlchemy エンジンを作成
engine = create_engine(DATABASE_URL)

# セッションファクトリを作成
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ---------------------------------------------------
# 3. データベースの初期化 (テーブル作成)
# ---------------------------------------------------
def init_db():
    """データベース内のすべてのテーブルを作成します。"""
    print(f"データベース '{DATABASE_FILE}' の初期化中...")
    Base.metadata.create_all(bind=engine)
    print("テーブルが作成されました。")

# ---------------------------------------------------
# 4. CRUD 操作の関数
# ---------------------------------------------------

# --- User モデルの CRUD ---

def create_user(session, name: str, email: str) -> User:
    """新しいユーザーを作成します。"""
    new_user = User(name=name, email=email)
    session.add(new_user)
    session.commit()
    session.refresh(new_user)
    print(f"作成: ユーザー ID={new_user.id}, 名前={new_user.name}, メール={new_user.email}, 作成日時={new_user.created_at}")
    return new_user

def get_user_by_id(session, user_id: int) -> User | None:
    """ID を指定してユーザーを取得します。"""
    user = session.query(User).filter(User.id == user_id).first()
    if user:
        print(f"取得: ユーザー ID={user.id}, 名前={user.name}")
    else:
        print(f"取得: ユーザー ID={user_id} が見つかりません。")
    return user

def get_all_users(session) -> List[User]:
    """すべてのユーザーを取得します。"""
    users = session.query(User).all()
    print("--- すべてのユーザー ---")
    if not users:
        print("ユーザーは存在しません。")
    for user in users:
        user_info = {k: v for k, v in user.__dict__.items() if not k.startswith('_sa_')}
        print(f"ユーザー情報: {user_info}")
    print("----------------------")
    return users

def update_user_email(session, user_id: int, new_email: str) -> User | None:
    """ユーザーのメールアドレスを更新します。"""
    user = session.query(User).filter(User.id == user_id).first()
    if user:
        old_email = user.email
        user.email = new_email
        session.commit()
        session.refresh(user)
        print(f"更新: ユーザー ID={user.id}, メール: {old_email} -> {user.email}")
    else:
        print(f"更新失敗: ユーザー ID={user_id} が見つかりません。")
    return user

def delete_user(session, user_id: int):
    """ユーザーを削除します。"""
    user = session.query(User).filter(User.id == user_id).first()
    if user:
        session.delete(user)
        session.commit()
        print(f"削除: ユーザー ID={user.id}, 名前={user.name}")
    else:
        print(f"削除失敗: ユーザー ID={user_id} が見つかりません。")

# --- Product モデルの CRUD ---

def create_product(session, name: str, price: int, category: str = None, stock: int = 0) -> Product:
    """新しい商品を作成します。"""
    new_product = Product(name=name, price=price, category=category, stock_quantity=stock)
    session.add(new_product)
    session.commit()
    session.refresh(new_product)
    print(f"作成: 商品 ID={new_product.id}, 名前={new_product.name}, 価格={new_product.price}")
    return new_product

def get_product_by_id(session, product_id: int) -> Product | None:
    """ID を指定して商品を取得します。"""
    product = session.query(Product).filter(Product.id == product_id).first()
    if product:
        print(f"取得: 商品 ID={product.id}, 名前={product.name}, 価格={product.price}")
    else:
        print(f"取得: 商品 ID={product_id} が見つかりません。")
    return product

def update_product_price(session, product_id: int, new_price: int) -> Product | None:
    """商品の価格を更新します。"""
    product = session.query(Product).filter(Product.id == product_id).first()
    if product:
        old_price = product.price
        product.price = new_price
        session.commit()
        session.refresh(product)
        print(f"更新: 商品 ID={product.id}, 価格: {old_price} -> {product.price}")
    else:
        print(f"更新失敗: 商品 ID={product_id} が見つかりません。")
    return product

def delete_product(session, product_id: int):
    """商品を削除します。"""
    product = session.query(Product).filter(Product.id == product_id).first()
    if product:
        session.delete(product)
        session.commit()
        print(f"削除: 商品 ID={product.id}, 名前={product.name}")
    else:
        print(f"削除失敗: 商品 ID={product_id} が見つかりません。")

# ---------------------------------------------------
# 5. メインの実行ブロック
# ---------------------------------------------------
if __name__ == "__main__":
    # 既存の DB ファイルを削除してクリーンな状態から始める (テスト用)
    if os.path.exists(DATABASE_FILE):
        os.remove(DATABASE_FILE)
        print(f"既存のデータベースファイル '{DATABASE_FILE}' を削除しました。")

    init_db()

    # DB セッションの取得
    session = SessionLocal()

    try:
        # --- ユーザーの CRUD 操作 ---
        print("\n--- ユーザー操作 ---")
        user1 = create_user(session, "Alice", "alice@example.com")
        user2 = create_user(session, "Bob", "bob@example.com")
        create_user(session, "Charlie", "charlie@example.com")

        get_all_users(session)

        # ユーザー1のメールアドレスを更新
        update_user_email(session, user1.id, "alice.updated@example.com")
        get_user_by_id(session, user1.id)

        # 存在しないユーザーの更新を試みる
        update_user_email(session, 999, "nonexistent@example.com")

        # ユーザー2を削除
        delete_user(session, user2.id)
        get_all_users(session)

        # 存在しないユーザーの削除を試みる
        delete_user(session, 998)

        # --- 商品の CRUD 操作 ---
        print("\n--- 商品操作 ---")
        product1 = create_product(session, "Laptop", 120000, "Electronics", 50)
        product2 = create_product(session, "Python Book", 3500, "Books", 200)
        create_product(session, "Desk Chair", 15000, "Furniture", 100)

        get_product_by_id(session, product1.id)

        # 商品2の価格を更新
        update_product_price(session, product2.id, 3000)
        get_product_by_id(session, product2.id)

        # 存在しない商品の更新を試みる
        update_product_price(session, 997, 1000)

        # 商品1を削除
        delete_product(session, product1.id)
        get_product_by_id(session, product1.id) # 削除されたことを確認

    except Exception as e:
        session.rollback() # エラーが発生した場合はロールバック
        print(f"トランザクション中にエラーが発生しました: {e}")
    finally:
        session.close() # セッションを閉じる
        print("\nセッションを閉じました。")
