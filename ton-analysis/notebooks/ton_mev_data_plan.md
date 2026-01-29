# TON内MEVデータ取得・初期分析方針（24hスナップショット）

## 目的
- STON.fiのUSDT<>TONペアを対象に、直近24時間でサンドイッチ等の有害MEVの有無と規模を掴む。
- スリッページ分布や遅延を簡易に測り、問題の有無を可視化する。

## 取得対象（最小セット）
1. **スワップログ（USDT<>TON on STON.fi）**
   - tx hash, block高さ, timestamp, sender, amount_in/out, 取得できればプール状態（before/after）。
   - 用途: サンドイッチ検知、スリッページ分布、トレードサイズ分布。
2. **ブロックメタデータ**
   - block高さ, timestamp, proposer/validator/builder（判明分）, tx数。
   - 用途: 送信→含まれる遅延、集中度の粗い把握。
3. **送信時刻（可能なら）**
   - 用途: 送信→含まれるまでの秒数分布、並び替え兆候。
4. **リファレンス価格**
   - TON/USDTの外部価格（CEXや他DEX）。
   - 用途: 公正価格推定、サンドイッチ時の価格乖離確認。

## 指標・検知
- サンドイッチ疑い: スワップ前後に逆→同方向の近接txがあり、被害者の実効価格が悪化。
- スリッページ分布: 期待レートとの差分を集計。
- 遅延: 送信時刻が取れれば送信→含まれる秒数の分布。
- 集中度（簡易）: 24hの提案者/ビルダー分布。

### TONにおけるMEV手段の前提
- **ガス入札競争（GPA）**: 事実上なし。ガスを積んで順番を奪う手段は使えない。
- **到達競争（低レイテンシ/特定ピア経由）**: シャード割り当てが送信者に選べないため、同一シャードに載るかは運要素が大きい。偶然同じシャードに載った場合のみ「先に受信したTXを優先」するポリシーなら効果がありうるが、狙撃性は低い。
- **提案者による並べ替え**: 可能。提案者はブロック内の順序を決める裁量があり、自身のTXや任意の順を先頭に置ける（有効性・ブロック制約を満たす範囲で）。同一シャードのブロックビルダーであることがFR/BR実行の現実的前提。

#### チェーン別ざっくり比較
- **Ethereum**: GPAあり（tip競争）。到達競争あり。提案者/ビルダー並べ替え余地あり（MEV-Boost/PBS系）。
- **Solana**: 優先手数料で実質GPA的競争あり。到達競争あり。リーダー裁量で並べ替え余地あり。
- **TON**: GPAなし。到達競争は「たまたま同じシャードに載った場合」に限定的に効くが、送信者はシャードを選べず狙撃性は低い。並べ替え余地はシャード提案者（ビルダー）の裁量が大きい。
  - シャーディング構造: シャードごとにブロックを生成し、マスターチェーンが各シャードの最新ブロック参照をBFTで合意する。Tx順序は各シャード内で提案者が決める（lt昇順で適用）ため、同一シャードかつ提案者権限がFR/BR成立の前提。マスターチェーンで参照順をいじってもシャード内の順序や価格インパクトはほぼ変わらない。
  - MEV有効範囲の目安（同一シャード前提）: gap=0（同一ブロック）が最大。gap=1（次ブロック）までは価格インパクトが残りうるが、gap≥2は希薄化しやすい。別シャードに載るTxは価格インパクトをほぼ踏めない。

## 範囲
- 期間: 直近24時間に限定。
- ペア: STON.fi USDT<>TON のみ（初回スコープ縮小）。
- 対象ルーターアドレス: [`EQCS4UEa5UaJLzOyyKieqQOQ2P9M-7kXpkO5HnP3Bv250cN3`](https://tonviewer.com/EQCS4UEa5UaJLzOyyKieqQOQ2P9M-7kXpkO5HnP3Bv250cN3)（STON.fi DEX, stonfi_router_v2）。
  - 参考: 他候補 `EQBSNX_5mSikBVttWhIaIb0f8jJU7fL6kvyyFVppd7dWRO6M`（stonfi_router_v2, *USDe専用）、`EQB3ncyBUTjZUA5EnFKR5_EnOMI9V1tTEAAPaiU71gc4TiUt`（stonfi_router）。

### Swap方向の決定（観察ベースのルール）
- 1つの swap は query_id で 2 呼び出しがペアになる:
  - 前半: in `Jetton Notify (0x7362d09c)`, out `Stonfi Swap V2 (0x6664de2a)`
  - 後半: in `Stonfi Pay To V2 (0x657b54f5)`, out `Jetton Transfer (0x0f8a7ea5)`
- 方向判定の優先順（実装済み）:
  1) Jetton Transfer の destination が USDTウォレットなら **TON→USDT**、pTONウォレットなら **USDT→TON**
  2) 次に Jetton Notify の sender が USDTウォレットなら **USDT→TON**、pTONウォレットなら **TON→USDT**
  3) 次に Swap V2 の `dex_payload.token_wallet1` が USDTウォレットなら **TON→USDT**、pTONウォレットなら **USDT→TON**
  - 該当が無ければ "unknown"（片側欠落時など）。

### 取得とパースのポイント
- エンドポイント例: `https://tonapi.io/v2/blockchain/accounts/{router}/transactions?limit=N&before_lt=...` をページング取得。
- `query_id` で In (Jetton Notify) と Out (Jetton Transfer) をペアリングし、1スワップを復元。
- 抜き出す主なフィールド（debug_extract_opcodes.py の出力例）:
  - `query_id`, `direction`
  - `notify` (tx_hash, in_msg→decoded_body.amount/sender/query_id)
  - `swap` (out_msg→decoded_body.left_amount/right_amount/dex_payload.token_wallet1, receiver/min_out など)
  - `pay` (in_msg→additional_info.amount0_out/amount1_out 等)
  - `transfer` (out_msg→decoded_body.amount/destination)
- `direction` と `in/out amount` から実効レート・スリッページを計算可能（Jetton decimals は別途考慮）。
- 24h分を `before_lt` でページングしてNDJSONに保存し、後段のノートで集計・検知に用いる。

#### スクリプト整理
- フェッチ系
  - `scripts/stonfi_fetch_swaps.py`: STONFi v2向け。query_id で Jetton Notify / SwapV2 / PayToV2 / Jetton Transfer を束ね、direction/in/out/rate/lt/utime を付けて NDJSON 出力。デフォルト出力先 `ton-analysis/data/stonfi_swaps_latest.ndjson`。direction が `unknown` は除外。
  - `scripts/dudust_fetch_swaps.py`: DeDust Classic向け。同様に query_id で束ね、direction/in/out/rate/lt/utime を付けて NDJSON 出力。デフォルト出力先 `ton-analysis/data/dudust_swaps_latest.ndjson`。direction が `unknown` は除外。
- 解析系
  - `scripts/swap_mev_detector.py`: レート統一（USDT/TON decimal-adjusted, scaled by 1000）、min_out 対比、FR/BR検知（同一ブロック・隣接・クロスブロック）を行う集計スクリプト。
    - レート統一: TON->USDT は 1/rate、USDT->TON は rate、その後1000倍スケール。
    - 主なオプション: `--data`, `--out`, `--enable-cross-block-br`, `--block-gap <n>`（デフォルト1）
    - Stonfi例: `MEV_FETCH_BLOCKS=true python ton-analysis/scripts/swap_mev_detector.py --data ton-analysis/data/stonfi_swaps_latest.ndjson --out ton-analysis/data/stonfi_mev_summary_gap1.txt --enable-cross-block-br --block-gap=1`
    - DuDust例: `MEV_FETCH_BLOCKS=true python ton-analysis/scripts/swap_mev_detector.py --data ton-analysis/data/dudust_swaps_latest.ndjson --out ton-analysis/data/dudust_mev_summary_gap1.txt --enable-cross-block-br --block-gap=1`
    - fetch_swapsは `NEXT_PUBLIC_TON_API_BASE_URL` / `TON_ROUTER` / `NEXT_PUBLIC_TON_API_KEY` 等を環境変数で上書き可能。mev_rate_checkは `MEV_FETCH_BLOCKS` でブロック取得のオン/オフを制御。
  - min_out 抽出: `swap.out_msg.decoded_body.dex_payload.swap_body.min_out` または `notify.in_msg.decoded_body.forward_payload.value.value.cross_swap_body.min_out`。
  - hit_pct = (min_out / actual_out) * 100（100%なら実受取がmin_outちょうど）。hit_pctが高いほど許容下限ギリギリ。
  - 現行サンプル: min_out 欠損なし（with_min_out=31, missing=0）、例: hit_pct max ≈99.97%, median=99.00%, mean≈95.48%。
  - 留意: hit_pctはユーザー設定の許容幅に依存。見積価格が無いため被害額は前後Txや外部価格を用いて評価する必要あり。

## 実行環境メモ
- Python: Homebrew の python@3.12（/opt/homebrew/bin/python3.12）
- 仮想環境: `.venv` を使用（`source .venv/bin/activate`）

## 次のステップ案
- スワップログ取得スクリプト雛形を作成し、24hデータをpull。
- サンドイッチ簡易検知とスリッページ分布を算出。
- ブロックメタと突合し、遅延・集中度をざっくり算出し可視化。