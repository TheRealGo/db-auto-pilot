# db-auto-pilot Spec

## 使い方
- このドキュメントは、各ステップごとに `理想的な状態` と `現在の実装` を書き分ける。
- `現在の実装` が `理想的な状態` と一致したら、そのステップは完了。
- 一致していない差分が、そのまま次にやるべきタスクになる。
- 全ステップが一致したら、プロジェクトは完成。

## ゴール
LLM とローカルのデータ処理を活用して、ユーザーが手元の Excel / CSV を構造化 DB に変換し、自然言語で検索・集計できるようにする。

---

## Step 1. ファイル取り込み

### 理想的な状態
- ユーザーは複数の Excel / CSV をまとめて 1 データセットとして取り込める。
- 複数 sheet を含む Excel も扱える。
- 取り込み時に列名正規化、型推定、サンプル値取得、欠損率や distinct 率などの基本プロファイルを作れる。
- 元ファイル名、sheet 名、元行番号を後続処理で追跡できる。

### 現在の実装
- 複数の Excel / CSV を 1 データセットとして取り込める。
- Excel の複数 sheet も取り込める。
- 取り込み時に列名正規化、logical type、sample values、null ratio、distinct ratio を作っている。
- `object` 列に対しても numeric / datetime / boolean ratio を見て logical type を補強している。
- observation 用に `non_null_count` `unique_count` `normalized_samples` `category_samples` `value_lengths` などの profile を作っている。
- raw DataFrame に `_source_file` `_source_sheet` `_row_index` を付与している。

### 差分から見える次タスク
- 和暦や業務固有コードなど、文字列ベース型推定の precision をさらに上げる。
- 観察メタデータを relation 推定や query prompt にそのまま再利用できるよう整理する。

---

## Step 2. Agentic な統合提案生成

### 理想的な状態
- 単なる固定 JSON 生成ではなく、Code Interpreter / Agent 的な流れで提案を作る。
- backend はローカルで観察ツールを提供し、LLM は必要に応じて追加観察を要求できる。
- 典型的には `観察 -> 追加観察 -> 提案確定` の multi-step loop で提案を固める。
- 列名だけでなく、値傾向、サンプル、型、表記ゆれも踏まえて統合候補を判断できる。
- proposal には observations、比較候補、agent step history、統合判断、保留事項が残る。

### 現在の実装
- proposal 生成は multi-step loop になっている。
- backend は `list_tables` `search_columns` `table_pair_overlap` `describe_columns` `sample_rows` `distinct_values` `column_group_compare` `value_overlap` `column_pair_compare` を提供している。
- proposal に `observations` `comparison_candidates` `agent_steps` `raw_llm_responses` `merge_decisions` `questions_for_user` を保存している。
- 候補生成では列名、token、型、distinct ratio に加えて normalized alias と sample value overlap を使っている。
- revise feedback からは保守的な structured override を抽出し、`keep_separate` `force_merge` `canonical_name_override` として proposal 後処理に反映している。
- backend 側で `schema_draft` `review_items` `merged_tables` を再構成し、`canonical_proposal` と承認チェックリストも作っている。
- `review_items` には `blocking` / `advisory` の severity を付け、canonical candidate には evidence summary と override 適用有無を付けている。
- decision action や canonical name の response validation を backend 側で厳格化している。

### 差分から見える次タスク
- relation 観察の precision をさらに上げる。
- 低信頼候補の review 基準を実データで再調整する。
- 実データ評価セットをさらに拡充し、proposal quality の回帰を継続計測できるようにする。

---

## Step 3. DB 作成前の確認フロー

### 理想的な状態
- ユーザーは DB 作成前に統合方針を確認できる。
- proposal には「統合する」「統合しない」「確認したい」が明確に出る。
- ユーザーは自然言語で補足指示を返し、その内容を踏まえて proposal が再生成される。
- このフローは DB 作成前の合意形成として扱う。

### 現在の実装
- Generate Proposal と Revise Proposal のフローがある。
- ユーザーの自然言語 feedback を入れて proposal を再生成できる。
- `review_items` `questions_for_user` `user_decisions` が返る。
- revise 時は直前 proposal の summary / review_items / user_decisions を agent loop に渡している。
- feedback から抽出できた override は backend 側で candidate 単位に強制適用している。
- UI で canonical summary、approval checklist、merge candidates、review items、feedback 入力を表示している。
- dataset detail から proposal version history を追える。

### 差分から見える次タスク
- legacy view をさらに縮退し、canonical proposal を唯一の承認 UI に寄せる。
- structured override の解釈対象を、業務別名や複数候補指定まで広げる。
- 保留中の確認項目の説明文と承認フローをさらに詰める。

---

## Step 4. データ保持方針

### 理想的な状態
- 元データに近い raw テーブルを保持する。
- ユーザー合意を反映した merged テーブルも保持する。
- merged 側から元ファイル・元行・元列への対応関係を追える。
- proposal、承認判断、materialization 実行履歴を後から追える。

### 現在の実装
- raw テーブルと merged テーブルの両方を保持している。
- merged テーブルに `_source_file` `_source_sheet` `_source_row_index` `_source_table` を入れている。
- approval decisions と merged column lineage を保存している。
- materialization runs に generated code と結果要約を保存している。

### 差分から見える次タスク
- 元列対応の精度をさらに上げる。
- proposal version と materialization run の関係をさらに見やすくする。
- 履歴の再現性を高めるため、必要なら observation snapshot の保存粒度を上げる。

---

## Step 5. 承認後の統合作成

### 理想的な状態
- 承認後は agent / Code Interpreter 相当が統合コードを生成してローカルで実行する。
- 列名統一、基本クレンジング、keep separate / merge を反映して merged データを作れる。
- 実行されたコード、実行結果、失敗理由が残る。
- 危険な操作は reject しつつ、アップロード済みデータ処理には十分な自由度を持たせる。

### 現在の実装
- proposal 承認と materialization 承認を分離している。
- proposal 承認後に materialization plan を作り、LLM には review 用の materialization proposal と normalization 方針を生成させている。
- merged DataFrame の組み立てコードは backend 側で deterministic に生成し、LLM の自由記述コードに依存しすぎない形に寄せている。
- ユーザーは normalization decisions、risk notes、quality expectations、generated code を確認してから materialization を承認できる。
- generated code は subprocess sandbox でローカル実行される。
- materialization proposal は履歴として保存され、失敗後は retry proposal を再生成できる。
- `materialization_runs` に generated code、generation summary、guard summary、quality summary、warnings、結果要約または error を保存している。
- AST ベースの guard で危険 import、危険 call、危険 attribute、introspection 系呼び出しを拒否している。
- runtime でも merged result shape、provenance 列、lineage shape を検証している。
- guard failure と execution failure を区別して保存・表示している。
- timeout、transport、result validation 失敗も区別して保存できる。
- 軽微な欠損は repair してから validation する。
- backend 側で数値・日付・文字列の deterministic normalization を適用している。
- materialization proposal の response validation を強化し、component / column 整合や supported normalization actions を検証している。
- plan には sample values や profile 情報に加えて suggested value mapping 候補も含め、`map_values` の config に流し込めるようにしている。
- retry 時は前回 run の error stage、guard violation、quality warning、column issues、previous run id、column patches を structured retry context として再提案に渡している。
- quality warning には warning detail と suggested actions を付け、retry guidance に同じ taxonomy を流している。
- UI では materialization proposal と source run のつながり、retry context、targeted fixes、proposal/run の履歴を表示している。

### 差分から見える次タスク
- 表記ゆれ吸収や値マッピングの精度をさらに上げる。
- quality summary を使った自動補正と retry 改善をさらに強化する。
- generated code をさらに declarative にして、plan 差分から完全再現できる形に寄せる。
- proposal version と materialization proposal / execution run のつながりを UI 上でさらに見やすくする。

---

## Step 6. 自然言語検索

### 理想的な状態
- ユーザーは自然言語で参照系・集計系の質問を投げられる。
- LLM は意図に沿った SQL を生成する。
- 更新系 SQL は拒否する。
- fallback ではなく、LLM が使えない時は明示エラーにする。

### 現在の実装
- raw / merged テーブルに対して自然言語 query を投げられる。
- LLM が SQL を生成し、SELECT 系のみ許可している。
- `INSERT` `UPDATE` `DELETE` `DROP` などは禁止している。
- LLM が使えない時は fallback せず、エラーを返す。
- query 用 schema prompt に table mode、logical type、lineage、metric/date/dimension の hint を含めている。

### 差分から見える次タスク
- SQL 生成精度を上げる。
- 実データに合わせて schema prompt の sample / business hint をさらに厚くする。
- 実データで query 品質を評価して改善する。

---

## Step 7. 検索結果の表示

### 理想的な状態
- ユーザーは結果テーブル、生成 SQL、自然言語説明を確認できる。
- proposal と query のどちらも、内部処理の要点が見える。
- 結果が意図通りかを UI 上で素早く判断できる。

### 現在の実装
- query result として result table、generated SQL、explanation を表示している。
- proposal 画面では canonical summary、approval checklist、notes、observations、questions、merge candidates、agent steps を表示している。
- review items には severity、canonical candidates には override 適用有無と evidence 要約を表示している。
- materialization proposal / run の source run、retry context、targeted fixes、status、guard summary、generated code を表示している。
- proposal / materialization / query をまとめた dataset timeline を表示している。

### 差分から見える次タスク
- UI を canonical proposal 中心にさらに整理する。
- 提案・承認・materialization・query のつながりをより操作しやすくする。
- エラー表示をより運用向けに改善する。

---

## Step 8. セキュリティと安全性

### 理想的な状態
- アップロード済みデータの処理に必要な自由度は残しつつ、危険なコード実行は防げる。
- ディレクトリ越え、ファイル書き込み、ネットワークアクセス、subprocess 実行は reject する。
- 外部送信は明示した LLM endpoint のみで、不要な第三者送信はない。

### 現在の実装
- materialization code には AST ベースの静的 guard がある。
- 危険 import、危険 call、危険 attribute、introspection 系アクセスを reject している。
- runtime では最小 builtins のみを渡し、source frames は copy 済みのものを使っている。
- subprocess 実行に timeout と resource limit を掛けている。
- guard summary と violation 内容を `materialization_runs` と UI に表示している。
- runtime での外部送信先は LLM endpoint のみ。

### 差分から見える次タスク
- 間接参照や難読化された危険呼び出しへの対策をさらに強化する。
- resource limit の精度と実効性をさらに強化する。
- セキュリティ上の想定と保証を README / Spec に追加する。

---

## Step 9. 配布・起動体験

### 理想的な状態
- 非エンジニアでもすぐ起動できる。
- Web 開発用の複数プロセス起動を意識させない。
- 最終的にはデスクトップアプリとして配布できる。

### 現在の実装
- frontend build を backend が配信する構成になっている。
- デスクトップアプリ化の土台として Tauri packaging を入れている。
- アプリ内で API Key / Endpoint / Model を設定できる。

### 差分から見える次タスク
- release packaging を安定化する。
- macOS 配布の署名 / notarization を整理する。
- 初回利用ガイドやエラーハンドリングを整える。

---

## Step 10. 完成条件

### 理想的な状態
- すべての step で `現在の実装` が `理想的な状態` に一致している。
- 実データを使った評価で、proposal・materialization・query が実用に耐える。
- PM が客先でファイルを受け取り、その場で立ち上げて分析できる。

### 現在の実装
- 基本フローは一通りある。
- materialization は deterministic assembly と structured retry まで入った。
- proposal agent には relation 観察、structured override、severity 付き canonical review が入った。
- ただし、proposal 精度の実データ評価拡充、値マッピング精度改善、query 精度改善、UI 整理は未完了。

### 差分から見える次タスク
- 次に `Step 6. 自然言語検索` の精度改善を進める。
- 並行して `Step 5. 承認後の統合作成` の値マッピング精度改善を進める。
- 最後に UI と配布体験を仕上げる。
