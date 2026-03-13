# Case Study 4 — RAG & Search

Course: DAMG 7245 – Big Data Systems & Intelligent Analytics  
Program: MS in Information Systems, Northeastern University  

## Overview
This project implements Retrieval-Augmented Generation (RAG) and hybrid search to support evidence-backed score justification for the PE Org-AI-R platform.

## What this enables
- Semantic + hybrid retrieval over collected evidence
- Metadata filtering (company, dimension, source type, confidence)
- Score justification with cited evidence for investment committee workflows
- LLM routing with fallbacks for reliability

## Architecture (high-level)
CS1 → Company metadata  
CS2 → Evidence collection  
CS3 → Scoring  
CS4 → Search + Justification  

## Status
🚧 Repository initialized and scaffolding added

## Airflow Integration
- `pe-org-air-platform/dags/full_platform_pipeline.py` adds DAG `org_air_full_platform_pipeline`.
- This DAG orchestrates the core repo pipelines end-to-end.
- `scripts/backfill_companies.py` (CS1 seed/upsert)
- `scripts/collect_evidence.py` and `scripts/collect_signals.py` (CS2 collection)
- `scripts/compute_signal_scores.py` and `scripts/compute_company_signal_summaries.py` (signal scoring + summaries)
- `scripts/compute_dimension_scores.py` and `scripts/run_scoring_engine.py` (CS3 scoring)
- `scripts/index_evidence.py` (CS4 retrieval indexing)
