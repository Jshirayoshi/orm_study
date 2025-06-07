import re
import os
import sys
import yaml
import subprocess
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from typing import Dict, Any, List, Set, Optional

# ---------------------------------------------------
# 1. 内部設定とヘルパー関数
# ---------------------------------------------------

# データベースファイル名
DATABASE_FILE = 'app.db'
DATABASE_URL = f"sqlite:///{DATABASE_FILE}"

# Alembic のディレクトリ名
ALEMBIC_DIR = 'alembic'
ALEMBIC_INI = 'alembic.ini'
ALEMBIC_ENV_PY = os.path.join(ALEMBIC_DIR, 'env.py')

# 生成されるモデルファイルのパス
MODELS_PY_PATH = 'models.py'

# YAMLの型名とSQLAlchemyの型オブジェクト名のマッピング
SQLA_TYPE_MAP: Dict[str, str] = {
    "Integer": "Integer",
    "String": "String",
    "DateTime": "DateTime",
    "Boolean": "Boolean",
    "Float": "Float",
    "Text": "Text",
}

# YAMLのデフォルト値文字列とSQLAlchemyの関数名のマッピング
SQLA_FUNC_MAP: Dict[str, str] = {
    "func.now()": "func.now",
}

def _snake_to_pascal(snake_str: str) -> str:
    """snake_case 文字列を PascalCase (クラス名) に変換します。"""
    return "".join(word.capitalize() for word in snake_str.split('_'))

def _normalize_yaml_value(value: Any) -> str:
    """YAMLの値をPythonコードで安全に表現できる文字列に変換します。"""
    if isinstance(value, str):
        return repr(value)
    return str(value)

# ---------------------------------------------------
# 2. モデルコード生成関数 (generate_models_code)
# ---------------------------------------------------
def generate_models_code(yaml_content: str) -> str:
    """
    YAMLコンテンツからSQLAlchemy ORMモデルのPythonコードを生成します。
    """
    try:
        schema: Dict[str, Any] = yaml.safe_load(yaml_content)
    except yaml.YAMLError as e:
        raise ValueError(f"エラー: YAMLスキーマコンテンツの解析に失敗しました: {e}")

    if not isinstance(schema, dict) or 'tables' not in schema or not isinstance(schema['tables'], dict):
        raise ValueError("無効なYAMLスキーマ: 'tables' セクションが見つからないか、不正な形式です。")

    imports: Set[str] = set()
    used_sqlalchemy_types: Set[str] = set()
    needs_sqlalchemy_func_import: bool = False

    model_code_lines: List[str] = []

    for table_key_in_yaml, table_def in schema['tables'].items():
        if not isinstance(table_def, dict):
            raise ValueError(f"テーブル '{table_key_in_yaml}' の定義が不正な形式です。")
        
        class_name = table_def.get('class_name', _snake_to_pascal(table_key_in_yaml))
        db_table_name = table_def.get('table_name', table_key_in_yaml.lower())

        table_description = table_def.get('description', '')

        model_code_lines.append(f"class {class_name}(Base):")
        if table_description:
            model_code_lines.append(f"    \"\"\"")
            for line in table_description.strip().split('\n'):
                model_code_lines.append(f"    {line.strip()}")
            model_code_lines.append(f"    \"\"\"")
        
        model_code_lines.append(f"    __tablename__ = '{db_table_name}'")
        model_code_lines.append("")

        columns_def = table_def.get('columns')
        if not isinstance(columns_def, dict):
            raise ValueError(f"テーブル '{table_key_in_yaml}' には 'columns' セクションがないか、不正な形式です。")

        for col_name, col_def in columns_def.items():
            if not isinstance(col_def, dict) or 'type' not in col_def:
                raise ValueError(f"テーブル '{table_key_in_yaml}' のカラム '{col_name}' は不正な形式か、'type' がありません。")

            yaml_type = col_def['type']
            if yaml_type not in SQLA_TYPE_MAP:
                raise ValueError(f"サポートされていないSQLAlchemyの型 '{yaml_type}' がカラム '{col_name}' に指定されています。")
            
            used_sqlalchemy_types.add(SQLA_TYPE_MAP[yaml_type])
            column_type_str = SQLA_TYPE_MAP[yaml_type]

            if yaml_type == "String" and 'length' in col_def:
                column_type_str = f"String(length={col_def['length']})"
            
            column_args: List[str] = []

            if col_def.get('primary_key'):
                column_args.append("primary_key=True")
            if col_def.get('autoincrement'):
                column_args.append("autoincrement=True")
            if col_def.get('nullable') is False:
                column_args.append("nullable=False")
            if col_def.get('unique'):
                column_args.append("unique=True")
            
            if 'default' in col_def:
                default_val = col_def['default']
                if isinstance(default_val, str) and default_val.startswith('func.'):
                    needs_sqlalchemy_func_import = True
                    column_args.append(f"server_default={default_val}") 
                else:
                    column_args.append(f"default={_normalize_yaml_value(default_val)}")
            
            if col_def.get('index'):
                column_args.append("index=True")
            
            if 'comment' in col_def:
                column_args.append(f"comment={_normalize_yaml_value(col_def['comment'])}")

            args_str = ", ".join(column_args)
            if args_str:
                 model_code_lines.append(f"    {col_name} = Column({column_type_str}, {args_str})")
            else:
                 model_code_lines.append(f"    {col_name} = Column({column_type_str})")
        model_code_lines.append("")

    general_sqlalchemy_imports = ["Column"]
    if used_sqlalchemy_types:
        for t in used_sqlalchemy_types:
            if t not in general_sqlalchemy_imports:
                general_sqlalchemy_imports.append(t)
    
    final_imports = [f"from sqlalchemy import {', '.join(sorted(general_sqlalchemy_imports))}"]

    if needs_sqlalchemy_func_import:
        final_imports.append("from sqlalchemy.sql import func")
    
    final_imports.append("from sqlalchemy.ext.declarative import declarative_base")
    final_imports = sorted(list(set(final_imports)))

    full_code = "\n".join(final_imports)
    full_code += "\n\n"
    full_code += "Base = declarative_base()\n\n"
    full_code += "\n".join(model_code_lines)

    return full_code

# ---------------------------------------------------
# 3. Alembic セットアップ関数
# ---------------------------------------------------
def setup_alembic():
    """Alembic環境を初期化し、設定ファイルを準備します。"""
    if not os.path.exists(ALEMBIC_DIR):
        print(f"Alembic環境を初期化中: '{ALEMBIC_DIR}'...")
        try:
            subprocess.run(["alembic", "init", ALEMBIC_DIR], check=True, capture_output=True)
            print("Alembic環境が正常に初期化されました。")
        except subprocess.CalledProcessError as e:
            print(f"Alembic初期化エラー: {e.stderr.decode()}")
            sys.exit(1)
    else:
        print(f"Alembic環境は既に存在します: '{ALEMBIC_DIR}'。")

    # alembic.ini を設定
    with open(ALEMBIC_INI, 'r') as f:
        ini_content = f.read()
    ini_content = re.sub(r'sqlalchemy.url = .*', f'sqlalchemy.url = {DATABASE_URL}', ini_content)
    with open(ALEMBIC_INI, 'w') as f:
        f.write(ini_content)
    print(f"'{ALEMBIC_INI}' にデータベースURLを設定しました。")

    # alembic/env.py を設定
    with open(ALEMBIC_ENV_PY, 'r') as f:
        env_content = f.read()

    # Base をインポートする行と sys.path の追加
    # idempotency を考慮し、既に存在する場合は追加しない
    if "import models" not in env_content:
        # target_metadata の設定行を探し、その直前に挿入する
        env_content = re.sub(
            r'target_metadata = None',
            f'sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))\n'
            f'try:\n'
            f'    import models\n'
            f'    target_metadata = models.Base.metadata\n'
            f'except ImportError:\n'
            f'    print("Error: Could not import models.py for Alembic. Ensure it is generated.", file=sys.stderr)\n'
            f'    sys.exit(1)\n',
            env_content,
            count=1 # 最初のマッチだけ置換
        )
        # 既存の `target_metadata = None` が残ってたら削除
        env_content = env_content.replace('target_metadata = None', '')


    with open(ALEMBIC_ENV_PY, 'w') as f:
        f.write("import os,sys\n"+env_content)
    print(f"'{ALEMBIC_ENV_PY}' を更新しました。")


# ---------------------------------------------------
# 4. Alembic マイグレーション実行関数
# ---------------------------------------------------
def run_alembic_migrations(message: str = "auto-generated migration"):
    """Alembic のリビジョンを生成し、データベースに適用します。"""
    print("\n--- Alembic マイグレーション実行 ---")
    
    # リビジョンの自動生成
    try:
        print(f"Alembicリビジョンを自動生成中 (メッセージ: '{message}')...")
        result = subprocess.run(
            ["alembic", "-c", ALEMBIC_INI, "revision", "--autogenerate", "-m", message],
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print(f"Alembic revision warnings/errors:\n{result.stderr}")
    except subprocess.CalledProcessError as e:
        print(f"Alembicリビジョン生成エラー: {e.stderr}")
        sys.exit(1)
    except FileNotFoundError:
        print("エラー: 'alembic' コマンドが見つかりません。Alembicがインストールされ、PATHが通っていることを確認してください。")
        sys.exit(1)

    # 生成されたリビジョンを適用
    try:
        print("Alembicマイグレーションを適用中 ('alembic upgrade head')...")
        result = subprocess.run(
            ["alembic", "-c", ALEMBIC_INI, "upgrade", "head"],
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print(f"Alembic upgrade warnings/errors:\n{result.stderr}")
        print("データベーススキーマが正常に更新されました。")
    except subprocess.CalledProcessError as e:
        print(f"Alembicマイグレーション適用エラー: {e.stderr}")
        sys.exit(1)

# ---------------------------------------------------
# 5. モデルの動的インポートとCRUD関数 (crud_sample.pyの内容を統合)
# ---------------------------------------------------

# Alembic 実行後に models.py からモデルを動的にロードするためにグローバル変数を用意
User: Any = None
Product: Any = None
Base: Any = None

def load_models_dynamically():
    """
    生成された models.py からモデルを動的にインポートします。
    Alembicの実行後に呼び出されることを想定。
    """
    global User, Product, Base
    try:
        spec = importlib.util.spec_from_file_location("orm_models", MODELS_PY_PATH)
        if spec is None:
            raise ImportError(f"'{MODELS_PY_PATH}' の仕様をロードできませんでした。")
        orm_models = importlib.util.module_from_spec(spec)
        if spec.loader:
            spec.loader.exec_module(orm_models)
        else:
            raise ImportError(f"'{MODELS_PY_PATH}' のローダーがありません。")

        Base = orm_models.Base
        User = orm_models.User
        Product = orm_models.Product

        print("モデルを正常にロードしました。")

    except ImportError as e:
        print(f"エラー: モデルのロード中に問題が発生しました - {e}", file=sys.stderr)
        print("models.pyが正しく生成されているか、およびその内容を確認してください。", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"予期せぬエラー: モデルの動的ロード中にエラーが発生しました - {e}", file=sys.stderr)
        sys.exit(1)

# CRUD 関数 (models.py がロードされた後にのみ実行可能)
def create_user(session, name: str, email: str, phone_number: Optional[str] = None) -> User:
    new_user = User(name=name, email=email, phone_number=phone_number)
    session.add(new_user)
    session.commit()
    session.refresh(new_user)
    print(f"作成: ユーザー ID={new_user.id}, 名前={new_user.name}, メール={new_user.email}, 電話={new_user.phone_number}, 作成日時={new_user.created_at}")
    return new_user

def get_user_by_id(session, user_id: int) -> Optional[User]:
    user = session.query(User).filter(User.id == user_id).first()
    if user:
        print(f"取得: ユーザー ID={user.id}, 名前={user.name}, メール={user.email}, 電話={user.phone_number}")
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

def update_user_info(session, user_id: int, new_email: Optional[str] = None, new_phone: Optional[str] = None) -> Optional[User]:
    user = session.query(User).filter(User.id == user_id).first()
    if user:
        if new_email:
            user.email = new_email
        if new_phone is not None:
            user.phone_number = new_phone
        session.commit()
        session.refresh(user)
        print(f"更新: ユーザー ID={user.id}, メール={user.email}, 電話={user.phone_number}")
    else:
        print(f"更新失敗: ユーザー ID={user_id} が見つかりません。")
    return user

def delete_user(session, user_id: int):
    user = session.query(User).filter(User.id == user_id).first()
    if user:
        session.delete(user)
        session.commit()
        print(f"削除: ユーザー ID={user.id}, 名前={user.name}")
    else:
        print(f"削除失敗: ユーザー ID={user_id} が見つかりません。")

def create_product(session, name: str, price: int, category: Optional[str] = None, stock: int = 0) -> Product:
    new_product = Product(name=name, price=price, category=category, stock_quantity=stock)
    session.add(new_product)
    session.commit()
    session.refresh(new_product)
    print(f"作成: 商品 ID={new_product.id}, 名前={new_product.name}, 価格={new_product.price}")
    return new_product

def get_product_by_id(session, product_id: int) -> Optional[Product]:
    product = session.query(Product).filter(Product.id == product_id).first()
    if product:
        print(f"取得: 商品 ID={product.id}, 名前={product.name}, 価格={product.price}")
    else:
        print(f"取得: 商品 ID={product_id} が見つかりません。")
    return product

def update_product_price(session, product_id: int, new_price: int) -> Optional[Product]:
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
    product = session.query(Product).filter(Product.id == product_id).first()
    if product:
        session.delete(product)
        session.commit()
        print(f"削除: 商品 ID={product.id}, 名前={product.name}")
    else:
        print(f"削除失敗: 商品 ID={product_id} が見つかりません。")


# ---------------------------------------------------
# 6. メインの実行ロジック
# ---------------------------------------------------
if __name__ == "__main__":
    print("--- スキーマ更新、DBマイグレーション、CRUD実行スクリプト ---")

    # --- ステップ 1: YAML スキーマを定義 ---
    # ここにスキーマ定義を記述してください。
    # この文字列を変更することでスキーマを更新できます。
    # 例: Userテーブルに phone_number を追加する
    CURRENT_SCHEMA_YAML = """
tables:
  User:
    table_name: users
    description: "システムに登録されるユーザーの情報。"
    columns:
      id: {type: Integer, primary_key: true, autoincrement: true, comment: "ユーザーID"}
      name: {type: String, length: 100, nullable: false, comment: "ユーザー名"}
      email: {type: String, length: 255, unique: true, nullable: false, comment: "メールアドレス"}
      phone_number: {type: String, length: 20, nullable: true, comment: "電話番号"} # ★追加したカラム
      created_at: {type: DateTime, default: 'func.now()', comment: "作成日時"}
  Product:
    table_name: products
    description: "販売する商品の情報。"
    columns:
      id: {type: Integer, primary_key: true, autoincrement: true, comment: "商品ID"}
      name: {type: String, length: 255, nullable: false, comment: "商品名"}
      price: {type: Integer, nullable: false, default: 0, comment: "商品価格"}
      category: {type: String, length: 50, nullable: true, index: true, comment: "商品のカテゴリ。例: Electronics, Books"}
      stock_quantity: {type: Integer, nullable: false, default: 0, comment: "在庫数"}
"""
    # ★スキーマの変更例: Productテーブルに `last_updated_at` を追加する場合
    # CURRENT_SCHEMA_YAML = """
    # tables:
    #   User:
    #     table_name: users
    #     description: "システムに登録されるユーザーの情報。"
    #     columns:
    #       id: {type: Integer, primary_key: true, autoincrement: true, comment: "ユーザーID"}
    #       name: {type: String, length: 100, nullable: false, comment: "ユーザー名"}
    #       email: {type: String, length: 255, unique: true, nullable: false, comment: "メールアドレス"}
    #       phone_number: {type: String, length: 20, nullable: true, comment: "電話番号"}
    #       created_at: {type: DateTime, default: 'func.now()', comment: "作成日時"}
    #   Product:
    #     table_name: products
    #     description: "販売する商品の情報。"
    #     columns:
    #       id: {type: Integer, primary_key: true, autoincrement: true, comment: "商品ID"}
    #       name: {type: String, length: 255, nullable: false, comment: "商品名"}
    #       price: {type: Integer, nullable: false, default: 0, comment: "商品価格"}
    #       category: {type: String, length: 50, nullable: true, index: true, comment: "商品のカテゴリ。例: Electronics, Books"}
    #       stock_quantity: {type: Integer, nullable: false, default: 0, comment: "在庫数"}
    #       last_updated_at: {type: DateTime, default: 'func.now()', onupdate: 'func.now()', nullable: false, comment: "最終更新日時"} # ★新しいカラム
    # """


    # --- ステップ 2: models.py の生成 ---
    print("\n--- models.py を更新/生成中 ---")
    try:
        generated_code = generate_models_code(CURRENT_SCHEMA_YAML)
        with open(MODELS_PY_PATH, 'w', encoding='utf-8') as f:
            f.write(generated_code)
        print(f"'{MODELS_PY_PATH}' が正常に生成/更新されました。")
    except ValueError as e:
        print(f"モデル生成エラー: {e}", file=sys.stderr)
        sys.exit(1)

    # --- ステップ 3: Alembic 環境のセットアップ ---
    setup_alembic()

    # --- ステップ 4: Alembic マイグレーションの実行 ---
    # 既存の DB ファイルを削除してクリーンな状態から始めるか尋ねる (初回テスト用)
    if not os.path.exists(DATABASE_FILE) and not os.path.exists(os.path.join(ALEMBIC_DIR, 'versions')):
        print(f"\n'{DATABASE_FILE}' と Alembic リビジョンが見つかりません。")
        print("初回セットアップとして、新しいデータベースを作成し、初期マイグレーションを実行します。")
        run_alembic_migrations(message="initial migration")
    else:
        print(f"\n'{DATABASE_FILE}' または Alembic リビジョンが存在します。")
        print("スキーマの変更を検出し、新しいマイグレーションを適用します。")
        run_alembic_migrations(message=f"update schema {datetime.now().strftime('%Y%m%d%H%M%S')}")

    # --- ステップ 5: 生成されたモデルの動的ロード ---
    print("\n--- 生成されたモデルを動的ロード中 ---")
    import importlib.util
    load_models_dynamically() # モデルがロードされる

    # --- ステップ 6: CRUD 操作の実行 ---
    print("\n--- データベース CRUD 操作 ---")
    engine = create_engine(DATABASE_URL)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = SessionLocal()

    try:
        # ユーザーの CRUD 操作
        print("\n--- ユーザー操作 ---")
        if not session.query(User).first(): # ユーザーが一人もいない場合のみ初期データ追加
            print("初期ユーザーデータを追加します。")
            create_user(session, "Alice", "alice@example.com", "090-1111-2222")
            create_user(session, "Bob", "bob@example.com", None)
            create_user(session, "Charlie", "charlie@example.com", "080-3333-4444")
        else:
            print("既存ユーザーが存在します。初期データの追加はスキップします。")

        get_all_users(session)

        # ユーザー1の情報を更新
        if user1_id := session.query(User.id).filter(User.name == "Alice").scalar():
            update_user_info(session, user1_id, new_email="alice.new@example.com", new_phone="070-9876-5432")
            get_user_by_id(session, user1_id)
        
        # ユーザー2を削除
        if user2_id := session.query(User.id).filter(User.name == "Bob").scalar():
            delete_user(session, user2_id)
        get_all_users(session)

        # 商品の CRUD 操作
        print("\n--- 商品操作 ---")
        if not session.query(Product).first(): # 商品が一つもいない場合のみ初期データ追加
            print("初期商品データを追加します。")
            create_product(session, "Laptop", 120000, "Electronics", 50)
            create_product(session, "Python Book", 3500, "Books", 200)
        else:
            print("既存商品が存在します。初期データの追加はスキップします。")

        if product_id := session.query(Product.id).filter(Product.name == "Laptop").scalar():
            update_product_price(session, product_id, 110000)
            get_product_by_id(session, product_id)
        
        if product_id := session.query(Product.id).filter(Product.name == "Python Book").scalar():
            delete_product(session, product_id)
        get_product_by_id(session, product_id) # 削除確認

    except Exception as e:
        session.rollback()
        print(f"エラー: CRUD 操作中に問題が発生しました - {e}", file=sys.stderr)
    finally:
        session.close()
        print("\n--- 全ての処理が完了しました ---")
