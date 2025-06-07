import yaml
import os
import re
from typing import Dict, Any, List, Set

# --- 内部定数とヘルパー関数 ---

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
# このマップはfunc.*を識別するためにのみ使用し、インポート文字列には直接使いません
SQLA_FUNC_MAP: Dict[str, str] = {
    "func.now()": "func.now", # `func.now()`のような文字列を`func.now`にマッピング
}

def _snake_to_pascal(snake_str: str) -> str:
    """
    snake_case 文字列を PascalCase (クラス名) に変換します。
    例: 'user_profile' -> 'UserProfile'
    """
    return "".join(word.capitalize() for word in snake_str.split('_'))

def _normalize_yaml_value(value: Any) -> str:
    """
    YAMLの値をPythonコードで安全に表現できる文字列に変換します。
    特に文字列のためにrepr()を使用します。
    """
    if isinstance(value, str):
        return repr(value) # 文字列はクォーテーションで囲む
    return str(value) # その他の型はそのまま文字列に変換

# --- メインのコード生成ロジック ---

def generate_models_code(yaml_path: str) -> str:
    """
    YAMLスキーマファイルからSQLAlchemy ORMモデルのPythonコードを生成します。

    Args:
        yaml_path: YAMLスキーマファイルのパス。

    Returns:
        生成されたSQLAlchemyモデルのPythonコードを含む文字列。

    Raises:
        ValueError: YAMLスキーマが無効な場合や、不正なフォーマットの場合。
    """
    try:
        with open(yaml_path, 'r', encoding='utf-8') as f:
            schema: Dict[str, Any] = yaml.safe_load(f)
    except FileNotFoundError:
        raise ValueError(f"エラー: YAMLスキーマファイル '{yaml_path}' が見つかりません。")
    except yaml.YAMLError as e:
        raise ValueError(f"エラー: YAMLスキーマファイル '{yaml_path}' の解析に失敗しました: {e}")

    # スキーマのルート構造の基本的な検証
    if not isinstance(schema, dict) or 'tables' not in schema or not isinstance(schema['tables'], dict):
        raise ValueError("無効なYAMLスキーマ: 'tables' セクションが見つからないか、不正な形式です。")

    # 動的に必要なインポートを収集
    imports: Set[str] = set() 

    used_sqlalchemy_types: Set[str] = set()
    # `func` モジュール全体をインポートする必要があるため、フラグで管理
    needs_sqlalchemy_func_import: bool = False 

    model_code_lines: List[str] = []

    # 各テーブル定義を処理し、モデルクラスのコードを生成
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
                    # func.* が使われている場合は func モジュールのインポートが必要
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

    # 最終的なインポート文を生成
    general_sqlalchemy_imports = ["Column"]
    if used_sqlalchemy_types:
        for t in used_sqlalchemy_types:
            if t not in general_sqlalchemy_imports:
                general_sqlalchemy_imports.append(t)
    
    final_imports = [f"from sqlalchemy import {', '.join(sorted(general_sqlalchemy_imports))}"]

    # func モジュールが必要な場合のみインポートを追加
    if needs_sqlalchemy_func_import:
        final_imports.append("from sqlalchemy.sql import func")
    
    final_imports.append("from sqlalchemy.ext.declarative import declarative_base")
    final_imports = sorted(list(set(final_imports)))

    full_code = "\n".join(final_imports)
    full_code += "\n\n"
    full_code += "Base = declarative_base()\n\n"
    full_code += "\n".join(model_code_lines)

    return full_code

# --- メインの実行スクリプト ---

if __name__ == "__main__":
    current_dir = os.path.dirname(os.path.abspath(__file__))
    schema_path = os.path.join(current_dir, 'schema.yml')
    output_path = os.path.join(current_dir, 'models.py')

    if not os.path.exists(schema_path):
        print(f"'{schema_path}' が見つかりません。サンプルを作成します。")
        sample_schema_content = """
tables:
  User:
    table_name: users
    description: "システムに登録されるユーザーの情報。"
    columns:
      id: {type: Integer, primary_key: true, autoincrement: true, comment: "ユーザーID"}
      name: {type: String, length: 100, nullable: false, comment: "ユーザー名"}
      email: {type: String, length: 255, unique: true, nullable: false, comment: "メールアドレス"}
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
                
        with open(schema_path, "w", encoding="utf-8") as f:
            f.write(sample_schema_content.strip())
        print(f"サンプル '{schema_path}' を作成しました。")

    try:
        generated_code = generate_models_code(schema_path)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(generated_code)
        print(f"SQLAlchemy ORM モデルが '{output_path}' に正常に生成されました。")

        if os.path.exists(output_path):
            print("\n--- 生成された models.py の内容 (一部) ---")
            with open(output_path, "r", encoding="utf-8") as f:
                for _ in range(30):
                    line = f.readline()
                    if not line:
                        break
                    print(line.rstrip())
            print("------------------------------------------")

    except ValueError as e:
        print(f"エラー: モデル生成に失敗しました - {e}")
    except Exception as e:
        print(f"予期せぬエラーが発生しました: {e}")
