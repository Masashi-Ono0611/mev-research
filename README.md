# MEV Research (TON / TON<>EVM)

## Purpose
This repository hosts research for MEV on TON and cross-chain scenarios between TON and EVM. It collects reference papers, analysis artifacts, and writing assets for drafting a research paper.

## Directory Layout
- `docs/`
  - `outline/`: Paper outlines and chapter structures (Markdown).
  - `memo/`: Personal research memos (motivation, questions, hypotheses).
- `papers/`
  - `raw/`: Newly downloaded PDFs.
  - `notes/`: Paper reading notes (Markdown, same basename as PDF). Keep this minimal for quick recall.
- `ton-analysis/`
  - `data/`: Raw/processed TON data snapshots (date- and condition-stamped folders).
  - `notebooks/`: Analysis notebooks (code/experiments; run in an isolated environment).
  - `scripts/`: Data collection, preprocessing, visualization scripts.
- `evm-bridge-analysis/`
  - `data/`: Raw/processed bridge MEV data snapshots.
  - `notebooks/`: Cross-chain analyses (code/experiments).
  - `scripts/`: Bridge-specific data and modeling scripts.
- `models/`: Mathematical models or simulations (code and configs).
- `results/`: Figures and intermediate results intended for the paper.
- `reviews/`: Self-review or peer-review comments and follow-up tasks.
- `summaries/`: Topic-based concise summaries (for quick recall or presentations).

## Workflow Suggestions
1. **Paper ingestion (lightweight)**: Drop PDFs into `papers/raw/`, optionally normalize filenames (`YYYY_Author_Title.pdf`), and jot quick personal notes in `papers/notes/` with the same basename.
2. **Outlining and motivation**: Keep personal research memos (motivation/questions/hypotheses) in `docs/memo/`; evolve the paper structure in `docs/outline/`.
3. **Experiments**: Separate TON-only and cross-chain analyses. Organize data by date/network/conditions (e.g., `2026-01-19_mainnet_bridge-arb/`). Keep scripts in `scripts/`, analysis code/experiments in `notebooks/`.
4. **Results and figures**: Store publishable figures in `results/`, referencing them from the outline.
5. **Reviews**: Log review comments and action items in `reviews/`.

## Naming Conventions
- PDFs: `YYYY_Author_Title.pdf` (use consistent Title tokens).
- Notes: Same basename as the corresponding PDF (e.g., `2026_Smith_TON-MEV.md`).
- Data folders: `YYYY-MM-DD_network_description/` (e.g., `2026-01-19_mainnet_ton-blocks/`).

## Environment and Safety
- Use isolated environments for running notebooks or scripts; avoid leaking real credentials.
- Prefer simple, documented scripts; avoid one-off, untracked commands.
- Keep code, comments, and docs in English; conversational language may differ.
