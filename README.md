# db-auto-pilot

db-auto-pilot は、複数の Excel / CSV を LLM で整理して SQLite に取り込み、自然言語で検索できるローカル向けアプリです。原本テーブルと統合後テーブルの両方を保持し、DB 作成前に列統合の提案を確認できます。

## 何ができるか
- 複数の Excel / CSV を 1 つのデータセットとしてアップロード
- LLM が列名や意味の近さを分析し、統合候補とクレンジング方針を提案
- 提案に対して自然言語で補足し、承認後に SQLite へ DB 化
- `部署別の売上合計を見たい` のような自然言語から SQL を生成
- 結果テーブル、生成 SQL、説明文、検索履歴を確認

## 使い方
1. 左側の Upload から `.xlsx` `.xls` `.csv` を複数選択して `Create Dataset`。
2. `Generate Proposal` で統合候補を作成。
3. 必要なら自然言語で補足します。
例: `X.xlsx のA列と Y.xlsx のB列は似ていますが、業務上は別概念なので統合しないでください`
4. `Approve and Build DB` で原本テーブルと統合後テーブルを作成。
5. `Query Studio` で `raw` か `merged` を選び、自然言語で検索します。

## 起動方法
通常利用はルートの `run-app.command` を使います。

```bash
./run-app.command
```

この 1 コマンドで以下をまとめて実行します。

- backend 依存の同期
- frontend 依存のインストール
- frontend の build
- FastAPI の起動

起動後は `http://127.0.0.1:8000` を開いて使います。  
macOS では Finder から `run-app.command` をダブルクリックしても起動できます。

必要ならルートの `.env.example` を `.env` にコピーし、`OPENAI_API_KEY` を設定してください。未設定でも動きますが、その場合はローカル推定ロジックで提案と簡易 SQL を生成します。

## デスクトップアプリ化
Tauri でラップして `.app` や `.dmg` を作るための土台も追加しています。  
フロントエンドを Tauri WebView で表示し、バックエンドは PyInstaller で固めた sidecar を同梱します。

開発モード:

```bash
cd frontend
npm run tauri:dev
```

配布用ビルド:

```bash
cd frontend
npm run tauri:build
```

このとき以下が自動で走ります。

- backend の `uv sync --extra dev --extra desktop`
- PyInstaller による sidecar backend 生成
- frontend build
- Tauri bundle build

macOS では生成物は `frontend/src-tauri/target/release/bundle/` 配下に出ます。  
署名や notarization はまだ未設定なので、社外配布前には追加対応が必要です。

## 開発用起動
フロントエンドをホットリロード付きで触るときだけ、backend / frontend を別々に起動します。

`nix` が使えるなら、まず開発シェルに入ります。

```bash
nix develop
```

backend:

```bash
cd backend
uv sync --extra dev
uvicorn app.main:app --reload
```

frontend:

```bash
cd frontend
npm install
npm run dev
```

## 制約
- 更新系 SQL は許可しません
- 想定はローカル単一ユーザー利用です
- 可視化や高度な分析はまだ未実装です

詳細な要件は [Spec.md](./Spec.md) を参照してください。
